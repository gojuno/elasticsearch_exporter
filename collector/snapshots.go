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

type snapshotMetric struct {
	Type   prometheus.ValueType
	Desc   *prometheus.Desc
	Value  func(snapshotStats SnapshotStatDataResponse) float64
	Labels func(repositoryName string, snapshotStats SnapshotStatDataResponse) []string
}

type repositoryMetric struct {
	Type   prometheus.ValueType
	Desc   *prometheus.Desc
	Value  func(snapshotsStats SnapshotStatsResponse) float64
	Labels func(repositoryName string) []string
}

var (
	defaultSnapshotLabels      = []string{"repository", "state", "version"}
	defaultSnapshotLabelValues = func(repositoryName string, snapshotStats SnapshotStatDataResponse) []string {
		return []string{repositoryName, snapshotStats.State, snapshotStats.Version}
	}
	defaultSnapshotRepositoryLabels      = []string{"repository"}
	defaultSnapshotRepositoryLabelValues = func(repositoryName string) []string {
		return []string{repositoryName}
	}
)

// Snapshots information struct
type Snapshots struct {
	logger log.Logger
	client *http.Client
	url    *url.URL

	up                              prometheus.Gauge
	totalScrapes, jsonParseFailures prometheus.Counter

	snapshotMetrics   []*snapshotMetric
	repositoryMetrics []*repositoryMetric
}

// NewSnapshots defines Snapshots Prometheus metrics
func NewSnapshots(logger log.Logger, client *http.Client, url *url.URL) *Snapshots {
	constLabels := constLabelsFromURL(url)
	return &Snapshots{
		logger: logger,
		client: client,
		url:    url,

		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName(namespace, "snapshot_stats", "up"),
			Help:        "Was the last scrape of the ElasticSearch snapshots endpoint successful.",
			ConstLabels: constLabels,
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        prometheus.BuildFQName(namespace, "snapshot_stats", "total_scrapes"),
			Help:        "Current total ElasticSearch snapshots scrapes.",
			ConstLabels: constLabels,
		}),
		jsonParseFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        prometheus.BuildFQName(namespace, "snapshot_stats", "json_parse_failures"),
			Help:        "Number of errors while parsing JSON.",
			ConstLabels: constLabels,
		}),
		snapshotMetrics: []*snapshotMetric{
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, "snapshot_stats", "snapshot_number_of_indices"),
					"Number of indices in the last snapshot",
					defaultSnapshotLabels, constLabels,
				),
				Value: func(snapshotStats SnapshotStatDataResponse) float64 {
					return float64(len(snapshotStats.Indices))
				},
				Labels: defaultSnapshotLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, "snapshot_stats", "snapshot_start_time_timestamp"),
					"Last snapshot start timestamp",
					defaultSnapshotLabels, constLabels,
				),
				Value: func(snapshotStats SnapshotStatDataResponse) float64 {
					return float64(snapshotStats.StartTimeInMillis / 1000)
				},
				Labels: defaultSnapshotLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, "snapshot_stats", "snapshot_end_time_timestamp"),
					"Last snapshot end timestamp",
					defaultSnapshotLabels, constLabels,
				),
				Value: func(snapshotStats SnapshotStatDataResponse) float64 {
					return float64(snapshotStats.EndTimeInMillis / 1000)
				},
				Labels: defaultSnapshotLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, "snapshot_stats", "snapshot_number_of_failures"),
					"Last snapshot number of failures",
					defaultSnapshotLabels, constLabels,
				),
				Value: func(snapshotStats SnapshotStatDataResponse) float64 {
					return float64(len(snapshotStats.Failures))
				},
				Labels: defaultSnapshotLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, "snapshot_stats", "snapshot_total_shards"),
					"Last snapshot total shards",
					defaultSnapshotLabels, constLabels,
				),
				Value: func(snapshotStats SnapshotStatDataResponse) float64 {
					return float64(snapshotStats.Shards.Total)
				},
				Labels: defaultSnapshotLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, "snapshot_stats", "snapshot_failed_shards"),
					"Last snapshot failed shards",
					defaultSnapshotLabels, constLabels,
				),
				Value: func(snapshotStats SnapshotStatDataResponse) float64 {
					return float64(snapshotStats.Shards.Failed)
				},
				Labels: defaultSnapshotLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, "snapshot_stats", "snapshot_successful_shards"),
					"Last snapshot successful shards",
					defaultSnapshotLabels, constLabels,
				),
				Value: func(snapshotStats SnapshotStatDataResponse) float64 {
					return float64(snapshotStats.Shards.Successful)
				},
				Labels: defaultSnapshotLabelValues,
			},
		},
		repositoryMetrics: []*repositoryMetric{
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, "snapshot_stats", "number_of_snapshots"),
					"Number of snapshots in a repository",
					defaultSnapshotRepositoryLabels, constLabels,
				),
				Value: func(snapshotsStats SnapshotStatsResponse) float64 {
					return float64(len(snapshotsStats.Snapshots))
				},
				Labels: defaultSnapshotRepositoryLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, "snapshot_stats", "oldest_snapshot_timestamp"),
					"Timestamp of the oldest snapshot",
					defaultSnapshotRepositoryLabels, constLabels,
				),
				Value: func(snapshotsStats SnapshotStatsResponse) float64 {
					if len(snapshotsStats.Snapshots) == 0 {
						return 0
					}
					return float64(snapshotsStats.Snapshots[0].StartTimeInMillis / 1000)
				},
				Labels: defaultSnapshotRepositoryLabelValues,
			},
		},
	}
}

// Describe add Snapshots metrics descriptions
func (s *Snapshots) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range s.snapshotMetrics {
		ch <- metric.Desc
	}
	ch <- s.up.Desc()
	ch <- s.totalScrapes.Desc()
	ch <- s.jsonParseFailures.Desc()
}

func (s *Snapshots) getAndParseURL(u *url.URL, data interface{}) error {
	res, err := s.client.Get(u.String())
	if err != nil {
		return fmt.Errorf("failed to get from %s://%s:%s%s: %s",
			u.Scheme, u.Hostname(), u.Port(), u.Path, err)
	}

	defer func() {
		err = res.Body.Close()
		if err != nil {
			_ = level.Warn(s.logger).Log(
				"msg", "failed to close http.Client",
				"err", err,
			)
		}
	}()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP Request failed with code %d", res.StatusCode)
	}

	if err := json.NewDecoder(res.Body).Decode(data); err != nil {
		s.jsonParseFailures.Inc()
		return err
	}
	return nil
}

func (s *Snapshots) fetchAndDecodeSnapshotsStats() (map[string]SnapshotStatsResponse, error) {
	mssr := make(map[string]SnapshotStatsResponse)

	u := *s.url
	u.Path = path.Join(u.Path, "/_snapshot")
	var srr SnapshotRepositoriesResponse
	err := s.getAndParseURL(&u, &srr)
	if err != nil {
		return nil, err
	}
	for repository := range srr {
		u := *s.url
		u.Path = path.Join(u.Path, "/_snapshot", repository, "/_all")
		var ssr SnapshotStatsResponse
		err := s.getAndParseURL(&u, &ssr)
		if err != nil {
			continue
		}
		mssr[repository] = ssr
	}

	return mssr, nil
}

// Collect gets Snapshots metric values
func (s *Snapshots) Collect(ch chan<- prometheus.Metric) {
	s.totalScrapes.Inc()
	defer func() {
		ch <- s.up
		ch <- s.totalScrapes
		ch <- s.jsonParseFailures
	}()

	// indices
	snapshotsStatsResp, err := s.fetchAndDecodeSnapshotsStats()
	if err != nil {
		s.up.Set(0)
		_ = level.Warn(s.logger).Log(
			"msg", "failed to fetch and decode snapshot stats",
			"err", err,
		)
		return
	}
	s.up.Set(1)

	// Snapshots stats
	for repositoryName, snapshotStats := range snapshotsStatsResp {
		for _, metric := range s.repositoryMetrics {
			ch <- prometheus.MustNewConstMetric(
				metric.Desc,
				metric.Type,
				metric.Value(snapshotStats),
				metric.Labels(repositoryName)...,
			)
		}
		if len(snapshotStats.Snapshots) == 0 {
			continue
		}

		lastSnapshot := snapshotStats.Snapshots[len(snapshotStats.Snapshots)-1]
		for _, metric := range s.snapshotMetrics {
			ch <- prometheus.MustNewConstMetric(
				metric.Desc,
				metric.Type,
				metric.Value(lastSnapshot),
				metric.Labels(repositoryName, lastSnapshot)...,
			)
		}
	}
}
