package scraper

import (
	"fmt"
	"github.com/SPCU/Metrics/parser"
	"io/ioutil"
	"net/http"

	"gopkg.in/yaml.v3"

	spculog "github.com/SPCU/Libraries/log"
)

var log, _ = spculog.NewLogger(spculog.SpcuLoggerConfig{})

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
	Parser parser.Parser
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

// NewScraper pulls metrics from multiple sources
func NewScraper(cfg Config, scrappingParser parser.Parser) (*MetricScraper, error) {
	return &MetricScraper{
		Config: cfg,
		Parser: scrappingParser,
	}, nil
}

// ReadScraperYAMLConfigFile read the scraper config from a YAML file
func ReadScraperYAMLConfigFile(path string) (Config, error) {
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
