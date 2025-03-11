package main

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	//shovelLabels are the labels for all shovel mertrics
	shovelLabels = []string{"cluster", "vhost", "shovel", "type", "self", "state"}
	//shovelLabelKeys are the important keys to be extracted from json
	shovelLabelKeys = []string{"vhost", "name", "type", "node", "state"}
)

type exporterShovel struct {
	stateMetric map[string]*prometheus.GaugeVec
}

func newExporterShovel() Exporter {
	stateMetrics := make(map[string]*prometheus.GaugeVec, len(targetList))
	for instance := range targetList {
		stateMetrics[instance] = newGaugeVec("shovel_state", "A metric with a value of constant '1' for each shovel in a certain state", appendInstanceLabel(shovelLabels))
	}
	return exporterShovel{
		stateMetric: stateMetrics,
	}
}

func (e exporterShovel) Collect(ctx context.Context, ch chan<- prometheus.Metric, instance string) error {
	info := targetList[instance]
	e.stateMetric[instance].Reset()

	shovelData, err := info.req.getStatsInfo(*info.conf, "shovels", shovelLabelKeys)
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

	for _, shovel := range shovelData {
		self := selfLabel(*info.conf, shovel.labels["node"] == selfNode)
		e.stateMetric[instance].WithLabelValues(cluster, shovel.labels["vhost"], shovel.labels["name"], shovel.labels["type"], self, shovel.labels["state"], instance).Set(1)
	}

	e.stateMetric[instance].Collect(ch)

	return nil
}

func (e exporterShovel) Describe(ch chan<- *prometheus.Desc, instance string) {
	e.stateMetric[instance].Describe(ch)
}
