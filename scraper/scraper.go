package scraper

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	spcuLog "github.com/SPCU/Libraries/log"
	"gopkg.in/yaml.v3"
)

var log, _ = spcuLog.NewLogger(spcuLog.SpcuLoggerConfig{})

type GlobalConfig struct {
	ScrapeInterval     string            `yaml:"scrape_interval"`
	ScrapeTimout       string            `yaml:"scrape_timeout"`
	EvaluationInterval string            `yaml:"evaluation_interval"`
	ExternalLabels     map[string]string `yaml:"external_labels"`
}

type StaticConfig struct {
	Targets []string `yaml:"targets"`
}

type JobConfig struct {
	Name          string         `yaml:"job_name"`
	MetricsPath   string         `yaml:"metrics_path"`
	Scheme        string         `yaml:"scheme"`
	StaticConfigs []StaticConfig `yaml:"static_configs"`
}

type Config struct {
	Global GlobalConfig `yaml:"global"`
	Jobs   []JobConfig  `yaml:"scrape_configs"`
}

// MetricScraper contains other applications' info so
// that it can gather the prometheus metrics and push them to the cloud.
type MetricScraper struct {
	Config Config
}

// Addr returns the url of the metrics
// TODO: Just considers the first target
func (jc *JobConfig) Addr() string {
	return fmt.Sprintf("%s://%s%s", jc.Scheme, jc.StaticConfigs[0].Targets[0], jc.MetricsPath)
}

// Gather makes an HTTP request and return the string of the metrics that
// are usually served on /metrics
func (ms *MetricScraper) Gather(jc JobConfig) (string, error) {
	// Http call to get the metrics
	resp, err := http.Get(jc.Addr())
	if err != nil {
		log.Warn(err)
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Warn(err)
		return "", err
	}

	return string(body), nil
}

func (ms *MetricScraper) PushMetrics(host string, orgUUID string, deviceUUID string) {
	for _, jobConfig := range ms.Config.Jobs {
		// Gather the metrics
		metrics, err := ms.Gather(jobConfig)
		if err != nil {
			log.Warn(err)
			continue
		}

		// Push
		requestBody := bytes.NewBufferString(metrics)
		url := fmt.Sprintf("http://%s/orgs/%s/devices/%s/apps/%s/metrics", host, orgUUID, deviceUUID, jobConfig.Name)
		_, err = http.Post(url, "text/plain", requestBody)
		if err != nil {
			log.Warn(err)
			continue
		}
	}
}

func NewScraper(cfg Config) (*MetricScraper, error) {
	return &MetricScraper{
		Config: cfg,
	}, nil
}

func ReadScraperConfigFile(path string) (Config, error) {
	// Read metrics config file (YAML file)
	cfgFile, err := ioutil.ReadFile(path)
	if err != nil {
		return Config{}, err
	}

	// Parse YAML file
	cfg := Config{}
	if err = yaml.Unmarshal(cfgFile, &cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}
