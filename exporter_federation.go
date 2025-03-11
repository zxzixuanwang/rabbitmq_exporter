package main

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	federationLabels     = []string{"cluster", "vhost", "node", "queue", "exchange", "self", "status"}
	federationLabelsKeys = []string{"vhost", "status", "node", "queue", "exchange"}
)

type exporterFederation struct {
	stateMetric map[string]*prometheus.GaugeVec
}

func newExporterFederation() Exporter {
	stateMetrics := make(map[string]*prometheus.GaugeVec, len(targetList))
	for instance := range targetList {
		stateMetrics[instance] = newGaugeVec("federation_state", "A metric with a value of constant '1' for each federation in a certain state", appendInstanceLabel(federationLabels))
	}

	return exporterFederation{
		stateMetric: stateMetrics,
	}
}

func (e exporterFederation) Collect(ctx context.Context, ch chan<- prometheus.Metric, instance string) error {
	info := targetList[instance]
	e.stateMetric[instance].Reset()

	federationData, err := info.req.getStatsInfo(*info.conf, "federation-links", federationLabelsKeys)
	if err != nil {
		return err
	}

	cluster := ""
	if n, ok := ctx.Value(clusterName).(string); ok {
		cluster = n
	}
	selfNode := ""
	if n, ok := ctx.Value(nodeName).(string); ok {
		selfNode = n
	}

	for _, federation := range federationData {
		self := selfLabel(*info.conf, federation.labels["node"] == selfNode)
		e.stateMetric[instance].WithLabelValues(cluster, federation.labels["vhost"], federation.labels["node"], federation.labels["queue"], federation.labels["exchange"], self, federation.labels["status"], instance).Set(1)
	}

	e.stateMetric[instance].Collect(ch)

	return nil
}

func (e exporterFederation) Describe(ch chan<- *prometheus.Desc, instance string) {
	e.stateMetric[instance].Describe(ch)
}
