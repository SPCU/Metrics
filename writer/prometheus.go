package writer

import (
	"bytes"
	"context"
	"fmt"
	pb "github.com/SPCU/Api/metrics/models"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

const (
	writeTimeout    = 30 * time.Second
	userAgent       = "spcudata"
	epChannelLength = 1000 // Number of batches to stack up for each endpoint channel
	writeBatchSize  = 800  // Number of time series to batch up and send to prometheus endpoint
)

type writeEndpoint struct {
	url         string
	writeChan   chan []byte
	channelFull bool
	errorState  bool
}

// PromWriter is a metrics handler that sinks to Prometheus ingest endpoints
type PromWriter struct {
	writeEndpoints []*writeEndpoint
	incomingChan   chan *pb.TimeSeries
	futuresChan    chan map[string]int
}

// NewPrometheusM3Writer creates a metrics handler for Prometheus
func NewPrometheusM3Writer(endpoints []string) Writer {
	p := new(PromWriter)
	p.incomingChan = make(chan *pb.TimeSeries)
	p.futuresChan = make(chan map[string]int)

	go p.processWriteRequests()
	go p.processFutures()

	for _, ep := range endpoints {
		we := new(writeEndpoint)
		we.writeChan = make(chan []byte, epChannelLength)
		u, err := url.Parse(ep)
		if err != nil {
			log.Printf("ERROR: Invalid URL endpoint, %s: %s", ep, err)
			continue
		}
		if u.Path == "" {
			we.url = fmt.Sprintf("%s/api/v1/prom/remote/write", ep)
		} else {
			we.url = ep
		}

		p.writeEndpoints = append(p.writeEndpoints, we)
		go we.process()
	}

	return p
}

// tsToWriteRequest converts a batch of time series data to a prometheus write request
func tsToWriteRequest(tsBatch []*pb.TimeSeries) (*prompb.WriteRequest, map[string]int) {
	// Track future timestamps
	futureReports := make(map[string]int)
	// Specify the cutoff in ms since Unix epoch, since that's how time is specified in the samples
	futureCutoff := time.Now().Add(1*time.Minute).UnixNano() / int64(time.Millisecond)
	// Only one time series in this write request
	promTS := make([]prompb.TimeSeries, len(tsBatch))

	for i, ts := range tsBatch {
		// Add the special tag name
		ts.Tags = append(ts.Tags, &pb.Tag{Key: "__name__", Value: ts.Name})

		spcuSerial := "UNKNOWN"
		labels := make([]prompb.Label, len(ts.Tags))
		for i, tag := range ts.Tags {
			labels[i] = prompb.Label{Name: tag.Key, Value: tag.Value}
			if tag.Key == "spcu_serial" {
				spcuSerial = tag.Value
			}
		}

		// Only one sample in this time series
		sample := []prompb.Sample{{
			// Timestamp is int milliseconds
			Timestamp: ts.DataPoint.Timestamp,
			Value:     ts.DataPoint.Value,
		}}

		if ts.DataPoint.Timestamp > futureCutoff {
			futureReports[spcuSerial]++
		}

		promTS[i] = prompb.TimeSeries{Labels: labels, Samples: sample}
	}

	return &prompb.WriteRequest{
		Timeseries: promTS,
	}, futureReports
}

// WriteTagged writes time series to the incoming channel
func (p *PromWriter) WriteTagged(ts *pb.TimeSeries) {
	p.incomingChan <- ts
}

// processWriteRequests takes time series data off the incoming channel,
// puts them into batches, converts these batches into prometheus requests and
// then sends all these requests to the various endpoint channels
func (p *PromWriter) processWriteRequests() {
	for {

		tsBatch := make([]*pb.TimeSeries, writeBatchSize)

		for i := 0; i < writeBatchSize; i++ {
			tsBatch[i] = <-p.incomingChan
		}

		// Encode the data for sending
		promWR, futureReports := tsToWriteRequest(tsBatch)
		data, err := proto.Marshal(promWR)
		if err != nil {
			log.Print(err)
			return
		}
		encoded := snappy.Encode(nil, data)

		p.sendToEndpoints(encoded)
		p.futuresChan <- futureReports
	}
}

// sendToEndpoints sends the given data to all endpoint channels
func (p *PromWriter) sendToEndpoints(b []byte) {
	// Send data to all endpoints
	for _, ep := range p.writeEndpoints {
		select {
		case ep.writeChan <- b:
			ep.channelFull = false
		default:
			// Limit verbose logging. Only print once for every time it gets full
			if !ep.channelFull {
				log.Printf("ERROR: Channel full for %s", ep.url)
				ep.channelFull = true
			}
		}
	}
}

func (p *PromWriter) processFutures() {
	futureReports := make(map[string]int)
	t := time.NewTicker(time.Minute * 10)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			for k, v := range futureReports {
				log.Printf("Got %d future data points from SPU %q", v, k)
			}
			futureReports = make(map[string]int)
		case m := <-p.futuresChan:
			for k, v := range m {
				futureReports[k] += v
			}
		}
	}
}

// process takes data off the endpoint's channel and sends the data to the endpoint
func (ep *writeEndpoint) process() {

	httpClient := &http.Client{Timeout: writeTimeout}
	ctx := context.Background()

	for b := range ep.writeChan {

		body := bytes.NewReader(b)
		req, err := http.NewRequest("POST", ep.url, body)
		if err != nil {
			ep.error(err)
			continue
		}

		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", "snappy")
		req.Header.Set("User-Agent", userAgent)

		resp, err := httpClient.Do(req.WithContext(ctx))
		if err != nil {
			ep.error(err)
			continue
		}
		io.Copy(ioutil.Discard, resp.Body)
		bbb, _ := io.ReadAll(resp.Body)
		fmt.Println(string(bbb))
		resp.Body.Close()

		ep.errorState = false
	}
}

// error reduces logging by only logging once per error state
func (ep *writeEndpoint) error(err error) {
	if !ep.errorState {
		log.Printf("EROROR for endpoint %s: %s", ep.url, err)
		ep.errorState = true
	}
}
