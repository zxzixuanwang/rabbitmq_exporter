package main

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const (
	ALIVENESS   = "aliveness"
	CONNECTIONS = "connections"
	EXCHANGE    = "exchange"
	FEDERATION  = "federation"
	MEMORY      = "memory"
	NODE        = "node"
	OVERVIEW    = "overview"
	QUEUE       = "queue"
	SHOVEL      = "shovel"
)

var (
	exportersMu       sync.RWMutex
	exporterFactories = make(map[string]map[string]func() Exporter)
	targetList        = make(map[string]targetInfo)
	registerModule    = map[string]func() Exporter{
		ALIVENESS:   newExporterAliveness,
		CONNECTIONS: newExporterConnections,
		EXCHANGE:    newExporterExchange,
		FEDERATION:  newExporterFederation,
		MEMORY:      newExporterMemory,
		NODE:        newExporterNode,
		QUEUE:       newExporterQueue,
		SHOVEL:      newExporterShovel,
	}
)

type targetInfo struct {
	conf     *rabbitExporterConfig
	req      *request
	instance string
}
type contextValues string

const (
	endpointScrapeDuration contextValues = "endpointScrapeDuration"
	endpointUpMetric       contextValues = "endpointUpMetric"
	nodeName               contextValues = "node"
	clusterName            contextValues = "cluster"
	totalQueues            contextValues = "totalQueues"
)

func loadModule() {
	for k, v := range registerModule {
		RegisterExporter(k, v)
	}
}

// RegisterExporter makes an exporter available by the provided name.
func RegisterExporter(name string, f func() Exporter) {
	exportersMu.Lock()
	defer exportersMu.Unlock()
	if f == nil {
		panic("exporterFactory is nil")
	}
	for k := range targetList {
		if _, ok := exporterFactories[k]; !ok {
			exporterFactories[k] = make(map[string]func() Exporter)
		}
		exporterFactories[k][name] = f
	}
}

type exporter struct {
	mutex                        sync.RWMutex
	upMetric                     map[string]*prometheus.GaugeVec
	endpointUpMetric             map[string]*prometheus.GaugeVec
	endpointScrapeDurationMetric map[string]*prometheus.GaugeVec
	exporter                     map[string]map[string]Exporter
	overviewExporter             map[string]*exporterOverview
	lastScrapeOK                 map[string]bool
}

// Exporter interface for prometheus metrics. Collect is fetching the data and therefore can return an error
type Exporter interface {
	Collect(ctx context.Context, ch chan<- prometheus.Metric, instance string) error
	Describe(ch chan<- *prometheus.Desc, instance string)
}

func newExporter() *exporter {
	enabledExporter := make(map[string]map[string]Exporter, len(targetList))
	exporterOverview := make(map[string]*exporterOverview, len(targetList))
	upMetrics := make(map[string]*prometheus.GaugeVec, len(targetList))
	endpointUpMetrics := make(map[string]*prometheus.GaugeVec, len(targetList))
	endpointScrapeDurationMetric := make(map[string]*prometheus.GaugeVec, len(targetList))
	lastScrapeOK := make(map[string]bool, len(targetList))
	for instance, info := range targetList {
		if _, ok := enabledExporter[instance]; !ok {
			enabledExporter[instance] = make(map[string]Exporter)
		}
		for _, e := range info.conf.EnabledExporters {
			if _, ok := exporterFactories[instance][e]; ok {
				enabledExporter[instance][e] = exporterFactories[instance][e]()
			}
		}
		exporterOverview[instance] = newExporterOverview()
		upMetrics[instance] = newGaugeVec("up", "Was the last scrape of rabbitmq successful.", []string{"cluster", "node", "instance"})
		endpointUpMetrics[instance] = newGaugeVec("module_up", "Was the last scrape of rabbitmq successful per module.", []string{"cluster", "node", "module", "instance"})
		endpointScrapeDurationMetric[instance] = newGaugeVec("module_scrape_duration_seconds", "Duration of the last scrape in seconds", []string{"cluster", "node", "module", "instance"})
		lastScrapeOK[instance] = true
	}

	return &exporter{
		upMetric:                     upMetrics,
		endpointUpMetric:             endpointUpMetrics,
		endpointScrapeDurationMetric: endpointScrapeDurationMetric,
		exporter:                     enabledExporter,
		overviewExporter:             exporterOverview,
		lastScrapeOK:                 lastScrapeOK, //return true after start. Value will be updated with each scraping
	}
}

func (e *exporter) LastScrapeOK(instance string) bool {
	e.mutex.Lock() // To protect metrics from concurrent collects.
	defer e.mutex.Unlock()
	return e.lastScrapeOK[instance]
}

func (e *exporter) Describe(ch chan<- *prometheus.Desc) {
	for instance := range targetList {
		e.overviewExporter[instance].Describe(ch, instance)
		for _, ex := range e.exporter[instance] {
			ex.Describe(ch, instance)
		}

		e.upMetric[instance].Describe(ch)
		e.endpointUpMetric[instance].Describe(ch)
		e.endpointScrapeDurationMetric[instance].Describe(ch)
	}
	BuildInfo.Describe(ch)
}

func (e *exporter) Collect(ch chan<- prometheus.Metric) {
	e.mutex.Lock() // To protect metrics from concurrent collects.
	defer e.mutex.Unlock()

	BuildInfo.Collect(ch)

	for instance := range targetList {
		start := time.Now()
		allUp := true
		e.upMetric[instance].Reset()
		e.endpointUpMetric[instance].Reset()
		e.endpointScrapeDurationMetric[instance].Reset()
		if err := e.collectWithDuration(instance, e.overviewExporter[instance], "overview", ch); err != nil {
			log.WithError(err).Warn("retrieving overview failed")
			allUp = false
		}
		for name, ex := range e.exporter[instance] {
			if err := e.collectWithDuration(instance, ex, name, ch); err != nil {
				log.WithError(err).Warn("retrieving " + name + " failed")
				allUp = false
			}
		}

		if allUp {
			e.upMetric[instance].WithLabelValues(e.overviewExporter[instance].NodeInfo(instance).ClusterName, e.overviewExporter[instance].NodeInfo(instance).Node, instance).Set(1)
		} else {
			e.upMetric[instance].WithLabelValues(e.overviewExporter[instance].NodeInfo(instance).ClusterName, e.overviewExporter[instance].NodeInfo(instance).Node, instance).Set(0)
		}
		e.lastScrapeOK[instance] = allUp

		if e.overviewExporter[instance].NodeInfo(instance).ClusterName != "" && e.overviewExporter[instance].NodeInfo(instance).Node != "" {
			e.upMetric[instance].DeleteLabelValues("", "")
		}

		e.upMetric[instance].Collect(ch)
		e.endpointUpMetric[instance].Collect(ch)
		e.endpointScrapeDurationMetric[instance].Collect(ch)
		log.WithFields(log.Fields{
			"instance": instance,
			"duration": time.Since(start),
		}).Info("Metrics updated")
	}
}

func (e *exporter) collectWithDuration(instance string, ex Exporter, name string, ch chan<- prometheus.Metric) error {
	ctx := context.Background()
	ctx = context.WithValue(ctx, endpointScrapeDuration, e.endpointScrapeDurationMetric[instance])
	ctx = context.WithValue(ctx, endpointUpMetric, e.endpointUpMetric[instance])
	//use last know value, could be outdated or empty
	ctx = context.WithValue(ctx, nodeName, e.overviewExporter[instance].NodeInfo(instance).Node)
	ctx = context.WithValue(ctx, clusterName, e.overviewExporter[instance].NodeInfo(instance).ClusterName)
	ctx = context.WithValue(ctx, totalQueues, e.overviewExporter[instance].NodeInfo(instance).TotalQueues)

	startModule := time.Now()
	err := ex.Collect(ctx, ch, instance)

	//use current data
	node := e.overviewExporter[instance].NodeInfo(instance).Node
	cluster := e.overviewExporter[instance].NodeInfo(instance).ClusterName

	if scrapeDuration, ok := ctx.Value(endpointScrapeDuration).(*prometheus.GaugeVec); ok {
		if cluster != "" && node != "" { //values are not available until first scrape of overview succeeded
			scrapeDuration.WithLabelValues(cluster, node, name, instance).Set(time.Since(startModule).Seconds())
		}
	}
	if up, ok := ctx.Value(endpointUpMetric).(*prometheus.GaugeVec); ok {
		if err != nil {
			up.WithLabelValues(cluster, node, name, instance).Set(0)
		} else {
			up.WithLabelValues(cluster, node, name, instance).Set(1)
		}
		if node != "" && cluster != "" {
			up.DeleteLabelValues("", "", name, instance)
		}
	}
	return err
}

func appendInstanceLabel(input []string) []string {
	return append(input, "instance")
}
