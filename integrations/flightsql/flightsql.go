package flightsql

import (
	"database/sql"

	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-adbc/go/adbc/sqldriver"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func init() {
	sql.Register("flightsql", sqldriver.Driver{
		Driver: flightsql.NewDriver(memory.DefaultAllocator),
	})
}
