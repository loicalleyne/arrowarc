package schemas

import (
	"github.com/apache/arrow/go/v17/arrow"
)

func GetOpenMeteoArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "city", Type: arrow.BinaryTypes.String},
		{Name: "temperature", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
}
