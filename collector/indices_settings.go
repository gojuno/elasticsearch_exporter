package collector

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

// IndicesSettings information struct
type IndicesSettings struct {
	logger log.Logger
	client *http.Client
	url    *url.URL

	up                              *prometheus.GaugeVec
	readOnlyIndices                 *prometheus.GaugeVec
	totalScrapes, jsonParseFailures *prometheus.CounterVec
}

// NewIndicesSettings defines Indices Settings Prometheus metrics
func NewIndicesSettings(logger log.Logger, client *http.Client, url *url.URL) *IndicesSettings {
	return &IndicesSettings{
		logger: logger,
		client: client,
		url:    url,

		up: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prometheus.BuildFQName(namespace, "indices_settings_stats", "up"),
				Help: "Was the last scrape of the ElasticSearch Indices Settings endpoint successful.",
			},
			[]string{"url"}),
		totalScrapes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: prometheus.BuildFQName(namespace, "indices_settings_stats", "total_scrapes"),
				Help: "Current total ElasticSearch Indices Settings scrapes.",
			},
			[]string{"url"}),
		readOnlyIndices: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prometheus.BuildFQName(namespace, "indices_settings_stats", "read_only_indices"),
				Help: "Current number of read only indices within cluster",
			},
			[]string{"url"}),
		jsonParseFailures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: prometheus.BuildFQName(namespace, "indices_settings_stats", "json_parse_failures"),
				Help: "Number of errors while parsing JSON.",
			},
			[]string{"url"}),
	}
}

// Describe add Snapshots metrics descriptions
func (cs *IndicesSettings) Describe(ch chan<- *prometheus.Desc) {
	ch <- cs.up.WithLabelValues(cs.url.String()).Desc()
	ch <- cs.totalScrapes.WithLabelValues(cs.url.String()).Desc()
	ch <- cs.readOnlyIndices.WithLabelValues(cs.url.String()).Desc()
	ch <- cs.jsonParseFailures.WithLabelValues(cs.url.String()).Desc()
}

func (cs *IndicesSettings) getAndParseURL(u *url.URL, data interface{}) error {
	res, err := cs.client.Get(u.String())
	if err != nil {
		return fmt.Errorf("failed to get from %s://%s:%s%s: %s",
			u.Scheme, u.Hostname(), u.Port(), u.Path, err)
	}

	defer func() {
		err = res.Body.Close()
		if err != nil {
			_ = level.Warn(cs.logger).Log(
				"msg", "failed to close http.Client",
				"err", err,
			)
		}
	}()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP Request failed with code %d", res.StatusCode)
	}

	if err := json.NewDecoder(res.Body).Decode(data); err != nil {
		cs.jsonParseFailures.WithLabelValues(cs.url.String()).Inc()
		return err
	}
	return nil
}

func (cs *IndicesSettings) fetchAndDecodeIndicesSettings() (IndicesSettingsResponse, error) {

	u := *cs.url
	u.Path = path.Join(u.Path, "/_all/_settings")
	var asr IndicesSettingsResponse
	err := cs.getAndParseURL(&u, &asr)
	if err != nil {
		return asr, err
	}

	return asr, err
}

// Collect gets all indices settings metric values
func (cs *IndicesSettings) Collect(ch chan<- prometheus.Metric) {
	cs.totalScrapes.WithLabelValues(cs.url.String()).Inc()

	asr, err := cs.fetchAndDecodeIndicesSettings()
	if err != nil {
		cs.readOnlyIndices.WithLabelValues(cs.url.String()).Set(0)
		cs.up.WithLabelValues(cs.url.String()).Set(0)
		_ = level.Warn(cs.logger).Log(
			"msg", "failed to fetch and decode cluster settings stats",
			"err", err,
		)
		return
	}
	cs.up.WithLabelValues(cs.url.String()).Set(1)

	var c int
	for _, value := range asr {
		if value.Settings.IndexInfo.Blocks.ReadOnly == "true" {
			c++
		}
	}
	cs.readOnlyIndices.WithLabelValues(cs.url.String()).Set(float64(c))

	cs.up.Collect(ch)
	cs.totalScrapes.Collect(ch)
	cs.jsonParseFailures.Collect(ch)
	cs.readOnlyIndices.Collect(ch)
}
