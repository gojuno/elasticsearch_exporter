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

const (
	namespace = "elasticsearch"
)

var (
	colors                     = []string{"green", "yellow", "red"}
	defaultClusterHealthLabels = []string{"cluster"}
)

type clusterHealthMetric struct {
	Type  prometheus.ValueType
	Desc  *prometheus.Desc
	Value func(clusterHealth clusterHealthResponse) float64
}

type clusterHealthStatusMetric struct {
	Type   prometheus.ValueType
	Desc   *prometheus.Desc
	Value  func(clusterHealth clusterHealthResponse, color string) float64
	Labels func(clusterName, color string) []string
}

// ClusterHealth type defines the collector struct
type ClusterHealth struct {
	logger log.Logger
	client *http.Client
	url    *url.URL

	up                              prometheus.Gauge
	totalScrapes, jsonParseFailures prometheus.Counter

	metrics      []*clusterHealthMetric
	statusMetric *clusterHealthStatusMetric
}

// NewClusterHealth returns a new Collector exposing ClusterHealth stats.
func NewClusterHealth(logger log.Logger, client *http.Client, url *url.URL) *ClusterHealth {
	subsystem := "cluster_health"
	constLabels := constLabelsFromURL(url)

	return &ClusterHealth{
		logger: logger,
		client: client,
		url:    url,

		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName(namespace, subsystem, "up"),
			Help:        "Was the last scrape of the ElasticSearch cluster health endpoint successful.",
			ConstLabels: constLabels,
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        prometheus.BuildFQName(namespace, subsystem, "total_scrapes"),
			Help:        "Current total ElasticSearch cluster health scrapes.",
			ConstLabels: constLabels,
		}),
		jsonParseFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        prometheus.BuildFQName(namespace, subsystem, "json_parse_failures"),
			Help:        "Number of errors while parsing JSON.",
			ConstLabels: constLabels,
		}),

		metrics: []*clusterHealthMetric{
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "active_primary_shards"),
					"The number of primary shards in your cluster. This is an aggregate total across all indices.",
					defaultClusterHealthLabels, constLabels,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.ActivePrimaryShards)
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "active_shards"),
					"Aggregate total of all shards across all indices, which includes replica shards.",
					defaultClusterHealthLabels, constLabels,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.ActiveShards)
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "delayed_unassigned_shards"),
					"Shards delayed to reduce reallocation overhead",
					defaultClusterHealthLabels, constLabels,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.DelayedUnassignedShards)
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "initializing_shards"),
					"Count of shards that are being freshly created.",
					defaultClusterHealthLabels, constLabels,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.InitializingShards)
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "number_of_data_nodes"),
					"Number of data nodes in the cluster.",
					defaultClusterHealthLabels, constLabels,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.NumberOfDataNodes)
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "number_of_in_flight_fetch"),
					"The number of ongoing shard info requests.",
					defaultClusterHealthLabels, constLabels,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.NumberOfInFlightFetch)
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "task_max_waiting_in_queue_millis"),
					"Tasks max time waiting in queue.",
					defaultClusterHealthLabels, constLabels,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.TaskMaxWaitingInQueueMillis)
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "number_of_nodes"),
					"Number of nodes in the cluster.",
					defaultClusterHealthLabels, constLabels,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.NumberOfNodes)
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "number_of_pending_tasks"),
					"Cluster level changes which have not yet been executed",
					defaultClusterHealthLabels, constLabels,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.NumberOfPendingTasks)
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "relocating_shards"),
					"The number of shards that are currently moving from one node to another node.",
					defaultClusterHealthLabels, constLabels,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.RelocatingShards)
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "unassigned_shards"),
					"The number of shards that exist in the cluster state, but cannot be found in the cluster itself.",
					defaultClusterHealthLabels, constLabels,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.UnassignedShards)
				},
			},
		},
		statusMetric: &clusterHealthStatusMetric{
			Type: prometheus.GaugeValue,
			Desc: prometheus.NewDesc(
				prometheus.BuildFQName(namespace, subsystem, "status"),
				"Whether all primary and replica shards are allocated.",
				[]string{"cluster", "color"}, constLabels,
			),
			Value: func(clusterHealth clusterHealthResponse, color string) float64 {
				if clusterHealth.Status == color {
					return 1
				}
				return 0
			},
		},
	}
}

// Describe set Prometheus metrics descriptions.
func (c *ClusterHealth) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.metrics {
		ch <- metric.Desc
	}
	ch <- c.statusMetric.Desc

	ch <- c.up.Desc()
	ch <- c.totalScrapes.Desc()
	ch <- c.jsonParseFailures.Desc()
}

func (c *ClusterHealth) fetchAndDecodeClusterHealth() (clusterHealthResponse, error) {
	var chr clusterHealthResponse

	u := *c.url
	u.Path = path.Join(u.Path, "/_cluster/health")
	res, err := c.client.Get(u.String())
	if err != nil {
		return chr, fmt.Errorf("failed to get cluster health from %s://%s:%s%s: %s",
			u.Scheme, u.Hostname(), u.Port(), u.Path, err)
	}

	defer func() {
		err = res.Body.Close()
		if err != nil {
			_ = level.Warn(c.logger).Log(
				"msg", "failed to close http.Client",
				"err", err,
			)
		}
	}()

	if res.StatusCode != http.StatusOK {
		return chr, fmt.Errorf("HTTP Request failed with code %d", res.StatusCode)
	}

	if err := json.NewDecoder(res.Body).Decode(&chr); err != nil {
		c.jsonParseFailures.Inc()
		return chr, err
	}

	return chr, nil
}

// Collect collects ClusterHealth metrics.
func (c *ClusterHealth) Collect(ch chan<- prometheus.Metric) {
	var err error
	c.totalScrapes.Inc()
	defer func() {
		ch <- c.up
		ch <- c.totalScrapes
		ch <- c.jsonParseFailures
	}()

	clusterHealthResp, err := c.fetchAndDecodeClusterHealth()
	if err != nil {
		c.up.Set(0)
		_ = level.Warn(c.logger).Log(
			"msg", "failed to fetch and decode cluster health",
			"err", err,
		)
		return
	}
	c.up.Set(1)

	for _, metric := range c.metrics {
		ch <- prometheus.MustNewConstMetric(
			metric.Desc,
			metric.Type,
			metric.Value(clusterHealthResp),
			clusterHealthResp.ClusterName,
		)
	}

	for _, color := range colors {
		ch <- prometheus.MustNewConstMetric(
			c.statusMetric.Desc,
			c.statusMetric.Type,
			c.statusMetric.Value(clusterHealthResp, color),
			clusterHealthResp.ClusterName, color,
		)
	}
}
