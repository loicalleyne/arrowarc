package config

import (
	xtypes "github.com/ArrowArc/ArrowArc/internal/dbarrow/types"
	"github.com/apache/arrow/go/v17/arrow"
)

var OpenMeteoSchema = arrow.NewSchema([]arrow.Field{
	{Name: "city", Type: arrow.BinaryTypes.String},
	{Name: "temperature", Type: xtypes.NewJSONType()},
}, nil)

type City struct {
	Name      string
	Latitude  float64
	Longitude float64
}
