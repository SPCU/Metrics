package models

type DataPoint struct {
	Timestamp int64
	Value     float64
}

type Tag struct {
	Key   string
	Value string
}
type TimeSeries struct {
	Name      string
	Tags      []Tag
	DataPoint DataPoint
}
