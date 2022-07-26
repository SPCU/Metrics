package parser

import "github.com/SPCU/Api/metrics/models"

// Parser reads the input and convert it to models.TimeSeries
type Parser interface {
	ParseText(metricText string) ([]models.TimeSeries, error)
}
