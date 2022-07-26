package parser

import (
	"errors"
	"fmt"
	spcuLog "github.com/SPCU/Libraries/log"
	"github.com/SPCU/Metrics/models"
	"strconv"
	"strings"
	"time"
)

var log, _ = spcuLog.NewLogger(spcuLog.SpcuLoggerConfig{})

type PrometheusMetricParser struct {
}

func (p PrometheusMetricParser) ParseText(metricText string) ([]models.TimeSeries, error) {
	// Ignore the metric comment
	var metricLines []string
	textLines := strings.Split(metricText, "\n")
	for _, line := range textLines {
		if string(line[0]) != "#" {
			metricLines = append(metricLines, line)
		}
	}

	// Create TimeSeries
	var timeSeries []models.TimeSeries
	for _, metricLine := range metricLines {

		// To check whether this metric line is valid or not
		if err := validateMetricLine(metricLine); err != nil {
			return nil, err
		}

		// Parse the metric data point's value
		splitLine := strings.Split(metricLine, " ")
		if len(splitLine) < 2 {
			err := errors.New("bad metric format: can not split the line with space")
			log.Warn(err.Error())
			return nil, err
		}
		stringMetricValue := splitLine[len(splitLine)-1]
		// Cast the value to float64
		value, err := strconv.ParseFloat(stringMetricValue, 64)
		if err != nil {
			castErr := errors.New(
				fmt.Sprintf(
					"can not cast the value of metric in %s, err: %s", metricLine, err.Error(),
				),
			)
			log.Warn(castErr.Error())
			return nil, castErr
		}

		// Parse the metric name and tags
		var metricName string
		var tags []models.Tag
		if strings.Contains(metricLine, "{") {
			// Parse the metric name
			splitMetricID := strings.Split(metricLine, "{")
			if len(splitMetricID) != 2 {
				err := errors.New("bad metric format: can not find split the line with {")
				log.Warn(err.Error())
				return nil, err
			}
			metricName = splitMetricID[0]

			// Parse the metric tags
			stringMetricTagsWithCommas := splitMetricID[1]
			if len(stringMetricTagsWithCommas) == 0 {
				err := errors.New("bad metric format: there is no string after {")
				log.Warn(err.Error())
				return nil, err
			}
			// Take the tags part from the string
			stringMetricTagsWithCommas = strings.Split(stringMetricTagsWithCommas, "}")[0]
			if len(stringMetricTagsWithCommas) == 0 {
				err := errors.New("bad metric format: the length of tags is zero")
				log.Warn(err.Error())
				return nil, err
			}
			// Create the tags
			for _, stringTag := range strings.Split(stringMetricTagsWithCommas, ", ") {
				splitTag := strings.Split(stringTag, "=")
				tags = append(tags, models.Tag{
					Key:   splitTag[0],
					Value: splitTag[1][1:len(splitTag[1])],
				})
			}
		}

		// Create and append the TimeSeries
		timeSeries = append(timeSeries, models.TimeSeries{
			Name: metricName,
			Tags: tags,
			DataPoint: models.DataPoint{
				Timestamp: time.Now().UnixNano(),
				Value:     value,
			},
		})
	}

	return timeSeries, nil
}

func NewPrometheusMetricParser() (Parser, error) {
	return &PrometheusMetricParser{}, nil
}

// validateMetricLine validates the metrics line
func validateMetricLine(metricLine string) error {
	if strings.Count(metricLine, "{") > 1 {
		return errors.New("more that 1 { is in the metric line")
	}

	if strings.Count(metricLine, "}") > 1 {
		return errors.New("more that 1 { is in the metric line")
	}

	return nil
}
