package writer

import pb "github.com/SPCU/Api/metrics/models"

type Writer interface {
	WriteTagged(ts *pb.TimeSeries)
}
