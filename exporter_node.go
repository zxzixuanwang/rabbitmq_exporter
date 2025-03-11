package main

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	nodeLabels    = []string{"cluster", "node", "self"}
	nodeLabelKeys = []string{"name"}

	nodeGaugeVecFunc = func() map[string]*prometheus.GaugeVec {
		return map[string]*prometheus.GaugeVec{
			"uptime":          newGaugeVec("uptime", "Uptime in milliseconds", appendInstanceLabel(nodeLabels)),
			"running":         newGaugeVec("running", "number of running nodes", appendInstanceLabel(nodeLabels)),
			"mem_used":        newGaugeVec("node_mem_used", "Memory used in bytes", appendInstanceLabel(nodeLabels)),
			"mem_limit":       newGaugeVec("node_mem_limit", "Point at which the memory alarm will go off", appendInstanceLabel(nodeLabels)),
			"mem_alarm":       newGaugeVec("node_mem_alarm", "Whether the memory alarm has gone off", appendInstanceLabel(nodeLabels)),
			"disk_free":       newGaugeVec("node_disk_free", "Disk free space in bytes.", appendInstanceLabel(nodeLabels)),
			"disk_free_alarm": newGaugeVec("node_disk_free_alarm", "Whether the disk alarm has gone off.", appendInstanceLabel(nodeLabels)),
			"disk_free_limit": newGaugeVec("node_disk_free_limit", "Point at which the disk alarm will go off.", appendInstanceLabel(nodeLabels)),
			"fd_used":         newGaugeVec("fd_used", "Used File descriptors", appendInstanceLabel(nodeLabels)),
			"fd_total":        newGaugeVec("fd_available", "File descriptors available", appendInstanceLabel(nodeLabels)),
			"sockets_used":    newGaugeVec("sockets_used", "File descriptors used as sockets.", appendInstanceLabel(nodeLabels)),
			"sockets_total":   newGaugeVec("sockets_available", "File descriptors available for use as sockets", appendInstanceLabel(nodeLabels)),
			"partitions_len":  newGaugeVec("partitions", "Current Number of network partitions. 0 is ok. If the cluster is splitted the value is at least 2", appendInstanceLabel(nodeLabels)),
		}
	}
)

type exporterNode struct {
	nodeMetricsGauge map[string]map[string]*prometheus.GaugeVec
}

func newExporterNode() Exporter {
	nodeGaugeVecActual := make(map[string]map[string]*prometheus.GaugeVec, len(targetList))
	for instance, info := range targetList {
		if len(nodeGaugeVecActual[instance]) < 1 {
			nodeGaugeVecActual[instance] = nodeGaugeVecFunc()
		}

		if len(info.conf.ExcludeMetrics) > 0 {
			for _, metric := range info.conf.ExcludeMetrics {
				if nodeGaugeVecActual[instance][metric] != nil {
					delete(nodeGaugeVecActual[instance], metric)
				}
			}
		}
	}

	return exporterNode{
		nodeMetricsGauge: nodeGaugeVecActual,
	}
}

func (e exporterNode) Collect(ctx context.Context, ch chan<- prometheus.Metric, instance string) error {
	info := targetList[instance]

	selfNode := ""
	if n, ok := ctx.Value(nodeName).(string); ok {
		selfNode = n
	}
	cluster := ""
	if n, ok := ctx.Value(clusterName).(string); ok {
		cluster = n
	}

	nodeData, err := info.req.getStatsInfo(*info.conf, "nodes", nodeLabelKeys)

	if err != nil {
		return err
	}

	for _, gauge := range e.nodeMetricsGauge[instance] {
		gauge.Reset()
	}

	for key, gauge := range e.nodeMetricsGauge[instance] {
		for _, node := range nodeData {
			if value, ok := node.metrics[key]; ok {
				self := selfLabel(*info.conf, node.labels["name"] == selfNode)
				gauge.WithLabelValues(cluster, node.labels["name"], self, instance).Set(value)
			}
		}
	}

	for _, gauge := range e.nodeMetricsGauge[instance] {
		gauge.Collect(ch)
	}

	return nil
}

func (e exporterNode) Describe(ch chan<- *prometheus.Desc, instance string) {
	for _, nodeMetric := range e.nodeMetricsGauge[instance] {
		nodeMetric.Describe(ch)
	}
}
