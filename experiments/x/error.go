package experiments

import "fmt"

type conversionError struct {
	Location string
	Details  error
}

func (ce *conversionError) Error() string {
	if ce.Location == "" {
		return ce.Details.Error()
	}
	return fmt.Sprintf("conversion error in location %q: %v", ce.Location, ce.Details)
}

func (ce *conversionError) Unwrap() error {
	return ce.Details
}

func newConversionError(loc string, err error) *conversionError {
	return &conversionError{
		Location: loc,
		Details:  err,
	}
}
