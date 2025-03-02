package integrations

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/parquet-go/parquet-go"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/expr"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/polarsignals/iceberg-go"
	"github.com/polarsignals/iceberg-go/catalog"
	"github.com/polarsignals/iceberg-go/table"
	"github.com/thanos-io/objstore"
)

// Constants and default configurations
const (
	DefaultOrphanedFileAge = 24 * time.Hour
)

var defaultWriterOptions = []table.WriterOption{
	table.WithManifestSizeBytes(8 * 1024 * 1024), // 8MiB manifest size
	table.WithMergeSchema(),
	table.WithExpireSnapshotsOlderThan(6 * time.Hour), // 6 hours of snapshots
	table.WithMetadataDeleteAfterCommit(),
	table.WithMetadataPreviousVersionsMax(3), // Keep 3 previous versions of the metadata
}

// Iceberg represents an Apache Iceberg-backed DataSink/DataSource.
type Iceberg struct {
	catalog             catalog.Catalog
	bucketURI           string
	bucket              objstore.Bucket
	logger              log.Logger
	partitionSpec       iceberg.PartitionSpec
	maxDataFileAge      time.Duration
	orphanedFileAge     time.Duration
	maintenanceSchedule time.Duration
	maintenanceCtx      context.Context
	maintenanceDone     context.CancelFunc
	maintenanceWg       sync.WaitGroup
}

// IcebergOption configures an Iceberg DataSink/DataSource.
type IcebergOption func(*Iceberg)

// NewIceberg initializes a new Iceberg DataSink/DataSource.
func NewIceberg(uri string, ctlg catalog.Catalog, bucket objstore.Bucket, options ...IcebergOption) (*Iceberg, error) {
	berg := &Iceberg{
		catalog:         ctlg,
		bucketURI:       uri,
		bucket:          catalog.NewIcebucket(uri, bucket),
		orphanedFileAge: DefaultOrphanedFileAge,
		logger:          log.NewNopLogger(),
	}

	for _, opt := range options {
		opt(berg)
	}

	// Start a maintenance goroutine if a schedule is set
	if berg.maintenanceSchedule > 0 {
		berg.startMaintenance()
	}

	return berg, nil
}

// startMaintenance initializes the maintenance goroutine based on the schedule.
func (i *Iceberg) startMaintenance() {
	i.maintenanceCtx, i.maintenanceDone = context.WithCancel(context.Background())
	i.maintenanceWg.Add(1)
	go func(ctx context.Context) {
		defer i.maintenanceWg.Done()
		ticker := time.NewTicker(i.maintenanceSchedule)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := i.Maintenance(ctx); err != nil {
					level.Error(i.logger).Log("msg", "iceberg maintenance failure", "err", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}(i.maintenanceCtx)
}

// Close stops maintenance and cleans up resources.
func (i *Iceberg) Close() error {
	if i.maintenanceDone != nil {
		i.maintenanceDone()
		i.maintenanceWg.Wait()
	}
	return nil
}

// Maintenance performs maintenance tasks on Iceberg tables.
func (i *Iceberg) Maintenance(ctx context.Context) error {
	dbs, err := i.catalog.ListNamespaces(ctx, []string{i.bucketURI})
	if err != nil {
		return err
	}

	for _, db := range dbs {
		tables, err := i.catalog.ListTables(ctx, []string{filepath.Join(append([]string{i.bucketURI}, db...)...)})
		if err != nil {
			return err
		}

		for _, tbl := range tables {
			if err := i.maintainTable(ctx, db, tbl); err != nil {
				return err
			}
		}
	}

	return nil
}

// maintainTable performs maintenance tasks for a specific Iceberg table.
func (i *Iceberg) maintainTable(ctx context.Context, db, tbl []string) error {
	tablePath := filepath.Join(i.bucketURI, db[0], tbl[0])
	t, err := i.catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
	if err != nil {
		return err
	}

	if i.maxDataFileAge > 0 {
		if err := i.deleteOldDataFiles(ctx, t); err != nil {
			return err
		}

		// Reload the modified table
		t, err = i.catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
		if err != nil {
			return err
		}
	}

	// Delete orphaned files
	return table.DeleteOrphanFiles(ctx, t, i.orphanedFileAge)
}

// deleteOldDataFiles deletes data files older than the configured maximum age.
func (i *Iceberg) deleteOldDataFiles(ctx context.Context, t table.Table) error {
	w, err := t.SnapshotWriter(defaultWriterOptions...)
	if err != nil {
		return err
	}
	defer w.Close(ctx)

	return w.DeleteDataFile(ctx, func(d iceberg.DataFile) bool {
		id, err := ulid.Parse(strings.TrimSuffix(filepath.Base(d.FilePath()), ".parquet"))
		if err != nil {
			level.Error(i.logger).Log("msg", "failed to parse ulid", "err", err)
			return false
		}
		return time.Since(ulid.Time(id.Time())) > i.maxDataFileAge
	})
}

// Iceberg options
func WithIcebergPartitionSpec(spec iceberg.PartitionSpec) IcebergOption {
	return func(i *Iceberg) {
		i.partitionSpec = spec
	}
}

func WithDataFileExpiry(maxAge time.Duration) IcebergOption {
	return func(i *Iceberg) {
		i.maxDataFileAge = maxAge
	}
}

func WithMaintenanceSchedule(schedule time.Duration) IcebergOption {
	return func(i *Iceberg) {
		i.maintenanceSchedule = schedule
	}
}

func WithLogger(l log.Logger) IcebergOption {
	return func(i *Iceberg) {
		i.logger = l
	}
}

func (i *Iceberg) String() string {
	return "Iceberg"
}

// Scan reads data from the Iceberg table and applies filters.
func (i *Iceberg) Scan(ctx context.Context, prefix string, _ *dynparquet.Schema, filter logicalplan.Expr, _ uint64, callback func(context.Context, any) error) error {
	t, err := i.catalog.LoadTable(ctx, []string{i.bucketURI, prefix}, iceberg.Properties{})
	if err != nil {
		if errors.Is(err, catalog.ErrorTableNotFound) {
			return nil
		}
		return fmt.Errorf("failed to load table: %w", err)
	}

	// Get the latest snapshot
	snapshot := t.CurrentSnapshot()
	list, err := snapshot.Manifests(i.bucket)
	if err != nil {
		return fmt.Errorf("error reading manifest list: %w", err)
	}

	fltr, err := expr.BooleanExpr(filter)
	if err != nil {
		return err
	}

	for _, manifest := range list {
		ok, err := manifestMayContainUsefulData(t.Metadata().PartitionSpec(), t.Schema(), manifest, fltr)
		if err != nil {
			return fmt.Errorf("failed to filter manifest: %w", err)
		}
		if !ok {
			continue
		}

		entries, schema, err := manifest.FetchEntries(i.bucket, false)
		if err != nil {
			return fmt.Errorf("fetch entries %s: %w", manifest.FilePath(), err)
		}

		for _, e := range entries {
			ok, err := manifestEntryMayContainUsefulData(icebergSchemaToParquetSchema(schema), e, fltr)
			if err != nil {
				return fmt.Errorf("failed to filter entry: %w", err)
			}
			if !ok {
				continue
			}

			// Process data files
			if err := i.processDataFile(ctx, e, fltr, callback); err != nil {
				return err
			}
		}
	}

	return nil
}

// processDataFile reads and processes a data file.
func (i *Iceberg) processDataFile(ctx context.Context, e iceberg.ManifestEntry, fltr expr.TrueNegativeFilter, callback func(context.Context, any) error) error {
	bkt := NewBucketReaderAt(i.bucket)
	r, err := bkt.GetReaderAt(ctx, e.DataFile().FilePath())
	if err != nil {
		return err
	}

	file, err := parquet.OpenFile(
		r,
		e.DataFile().FileSizeBytes(),
		parquet.FileReadMode(parquet.ReadModeAsync),
	)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", e.DataFile().FilePath(), err)
	}

	buf, err := dynparquet.NewSerializedBuffer(file)
	if err != nil {
		return err
	}

	for i := 0; i < buf.NumRowGroups(); i++ {
		rg := buf.DynamicRowGroup(i)
		mayContainUsefulData, err := fltr.Eval(rg, false)
		if err != nil {
			return err
		}
		if mayContainUsefulData {
			if err := callback(ctx, rg); err != nil {
				return err
			}
		}
	}
	return nil
}

// Upload uploads a Parquet file into the Iceberg table.
func (i *Iceberg) Upload(ctx context.Context, name string, r io.Reader) error {
	tablePath := filepath.Join(i.bucketURI, filepath.Dir(filepath.Dir(name)))
	t, err := i.catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
	if err != nil {
		if !errors.Is(err, catalog.ErrorTableNotFound) {
			return err
		}

		// Table doesn't exist, create it
		t, err = i.catalog.CreateTable(ctx, tablePath, iceberg.NewSchema(0), iceberg.Properties{},
			catalog.WithPartitionSpec(i.partitionSpec),
		)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	w, err := t.SnapshotWriter(defaultWriterOptions...)
	if err != nil {
		return err
	}

	if err := w.Append(ctx, r); err != nil {
		return err
	}

	return w.Close(ctx)
}

// Delete is a no-op for Iceberg.
func (i *Iceberg) Delete(_ context.Context, _ string) error {
	return nil
}

// icebergTypeToParquetNode maps Iceberg types to Parquet nodes.
func icebergTypeToParquetNode(t iceberg.Type) parquet.Node {
	switch t.Type() {
	case "long":
		return parquet.Int(64)
	case "binary":
		return parquet.String()
	case "boolean":
		return parquet.Leaf(parquet.BooleanType)
	case "int":
		return parquet.Int(32)
	case "float":
		return parquet.Leaf(parquet.FloatType)
	case "double":
		return parquet.Leaf(parquet.DoubleType)
	case "string":
		return parquet.String()
	default:
		panic(fmt.Sprintf("unsupported type: %s", t.Type()))
	}
}

// icebergSchemaToParquetSchema converts an Iceberg schema to a Parquet schema.
func icebergSchemaToParquetSchema(schema *iceberg.Schema) *parquet.Schema {
	g := parquet.Group{}
	for _, f := range schema.Fields() {
		g[f.Name] = icebergTypeToParquetNode(f.Type)
	}
	return parquet.NewSchema("iceberg", g)
}

func dataFileToParticulate(schema *parquet.Schema, d iceberg.DataFile) expr.Particulate {
	return &dataFileParticulate{
		schema: schema,
		data:   d,
	}
}

type dataFileParticulate struct {
	schema *parquet.Schema
	data   iceberg.DataFile
}

func (d *dataFileParticulate) Schema() *parquet.Schema {
	return d.schema
}

type virtualColumnChunk struct {
	pType       parquet.Type
	column      int
	numValues   int64
	nulls       int64
	lowerBounds []byte
	upperBounds []byte
}

func (d *dataFileParticulate) ColumnChunks() []parquet.ColumnChunk {
	virtualColumnChunks := make([]parquet.ColumnChunk, 0, len(d.schema.Fields()))
	for i := range d.schema.Fields() {
		virtualColumnChunks = append(virtualColumnChunks, &virtualColumnChunk{
			pType:       d.schema.Fields()[i].Type(),
			nulls:       d.data.NullValueCounts()[i],
			column:      i,
			lowerBounds: d.data.LowerBoundValues()[i],
			upperBounds: d.data.UpperBoundValues()[i],
			numValues:   d.data.Count(),
		})
	}
	return virtualColumnChunks
}

func (v *virtualColumnChunk) Type() parquet.Type   { return nil }
func (v *virtualColumnChunk) Column() int          { return v.column }
func (v *virtualColumnChunk) Pages() parquet.Pages { return nil }
func (v *virtualColumnChunk) ColumnIndex() (parquet.ColumnIndex, error) {
	return &virtualColumnIndex{
		pType:       v.pType,
		nulls:       v.nulls,
		lowerBounds: v.lowerBounds,
		upperBounds: v.upperBounds,
	}, nil
}
func (v *virtualColumnChunk) OffsetIndex() (parquet.OffsetIndex, error) { return nil, nil }
func (v *virtualColumnChunk) BloomFilter() parquet.BloomFilter          { return nil }
func (v *virtualColumnChunk) NumValues() int64                          { return v.numValues }

type virtualColumnIndex struct {
	lowerBounds []byte
	upperBounds []byte
	nulls       int64
	pType       parquet.Type
}

func (v *virtualColumnIndex) NumPages() int       { return 1 }
func (v *virtualColumnIndex) NullCount(int) int64 { return v.nulls }
func (v *virtualColumnIndex) NullPage(int) bool   { return false }
func (v *virtualColumnIndex) MinValue(int) parquet.Value {
	switch v.pType.Kind() {
	case parquet.Int64:
		i := binary.LittleEndian.Uint64(v.lowerBounds)
		return parquet.Int64Value(int64(i))
	case parquet.ByteArray:
		return parquet.ByteArrayValue(v.lowerBounds)
	default:
		return parquet.ByteArrayValue(v.lowerBounds)
	}
}

func (v *virtualColumnIndex) MaxValue(int) parquet.Value {
	switch v.pType.Kind() {
	case parquet.Int64:
		i := binary.LittleEndian.Uint64(v.upperBounds)
		return parquet.Int64Value(int64(i))
	case parquet.ByteArray:
		return parquet.ByteArrayValue(v.upperBounds)
	default:
		return parquet.ByteArrayValue(v.upperBounds)
	}
}

func (v *virtualColumnIndex) IsAscending() bool  { return true }
func (v *virtualColumnIndex) IsDescending() bool { return false }

func manifestEntryMayContainUsefulData(schema *parquet.Schema, entry iceberg.ManifestEntry, filter expr.TrueNegativeFilter) (bool, error) {
	return filter.Eval(dataFileToParticulate(schema, entry.DataFile()), false)
}

func manifestMayContainUsefulData(partition iceberg.PartitionSpec, schema *iceberg.Schema, manifest iceberg.ManifestFile, filter expr.TrueNegativeFilter) (bool, error) {
	if partition.IsUnpartitioned() {
		return true, nil
	}
	// Ignore missing columns as the partition spec only contains the columns that are partitioned
	return filter.Eval(manifestToParticulate(partition, schema, manifest), true)
}

func manifestToParticulate(partition iceberg.PartitionSpec, schema *iceberg.Schema, m iceberg.ManifestFile) expr.Particulate {
	// Convert the partition spec to a parquet schema
	g := parquet.Group{}
	virtualColumnChunks := make([]parquet.ColumnChunk, 0, partition.NumFields())
	for i := 0; i < partition.NumFields(); i++ {
		field := partition.Field(i)
		summary := m.Partitions()[i]
		node := icebergTypeToParquetNode(schema.Field(field.SourceID).Type)
		g[field.Name] = node
		virtualColumnChunks = append(virtualColumnChunks, &virtualColumnChunk{
			pType:       node.Type(),
			nulls:       0, // TODO future optimization?
			column:      i,
			lowerBounds: *summary.LowerBound,
			upperBounds: *summary.UpperBound,
			numValues:   1, // m.ExistingRows() + m.AddedRows() // TODO: future optimization?
		})
	}

	return &manifestParticulate{
		schema:       parquet.NewSchema("iceberg-partition", g),
		columnChunks: virtualColumnChunks,
	}
}

type manifestParticulate struct {
	columnChunks []parquet.ColumnChunk
	schema       *parquet.Schema
}

func (m *manifestParticulate) Schema() *parquet.Schema { return m.schema }

func (m *manifestParticulate) ColumnChunks() []parquet.ColumnChunk { return m.columnChunks }
