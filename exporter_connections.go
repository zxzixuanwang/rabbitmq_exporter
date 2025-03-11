package main

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	connectionLabels            = []string{"cluster", "vhost", "node", "peer_host", "user", "self"}
	connectionLabelsStateMetric = []string{"cluster", "vhost", "node", "peer_host", "user", "state", "self"}
	connectionLabelKeys         = []string{"vhost", "node", "peer_host", "user", "state", "node"}
	connectionGaugeVecFunc      = func() map[string]*prometheus.GaugeVec {
		return map[string]*prometheus.GaugeVec{
			"channels":  newGaugeVec("connection_channels", "number of channels in use", appendInstanceLabel(connectionLabels)),
			"recv_oct":  newGaugeVec("connection_received_bytes", "received bytes", appendInstanceLabel(connectionLabels)),
			"recv_cnt":  newGaugeVec("connection_received_packets", "received packets", appendInstanceLabel(connectionLabels)),
			"send_oct":  newGaugeVec("connection_send_bytes", "send bytes", appendInstanceLabel(connectionLabels)),
			"send_cnt":  newGaugeVec("connection_send_packets", "send packets", appendInstanceLabel(connectionLabels)),
			"send_pend": newGaugeVec("connection_send_pending", "Send queue size", appendInstanceLabel(connectionLabels)),
		}
	}
)

type exporterConnections struct {
	connectionMetricsG map[string]map[string]*prometheus.GaugeVec
	stateMetric        *prometheus.GaugeVec
}

func newExporterConnections() Exporter {
	connectionGaugeVecActual := make(map[string]map[string]*prometheus.GaugeVec)
	for instance, info := range targetList {
		if len(connectionGaugeVecActual[instance]) < 1 {
			connectionGaugeVecActual[instance] = connectionGaugeVecFunc()
		}
		if len(info.conf.ExcludeMetrics) > 0 {
			for _, metric := range info.conf.ExcludeMetrics {
				if connectionGaugeVecActual[instance][metric] != nil {
					delete(connectionGaugeVecActual[instance], metric)
				}
			}
		}
	}

	return exporterConnections{
		connectionMetricsG: connectionGaugeVecActual,
		stateMetric:        newGaugeVec("connection_status", "Number of connections in a certain state aggregated per label combination.", appendInstanceLabel(connectionLabelsStateMetric)),
	}
}

func (e exporterConnections) Collect(ctx context.Context, ch chan<- prometheus.Metric, instance string) error {
	info := targetList[instance]
	rabbitConnectionResponses, err := info.req.getStatsInfo(*info.conf, "connections", connectionLabelKeys)

	if err != nil {
		return err
	}
	for _, gauge := range e.connectionMetricsG[instance] {
		gauge.Reset()
	}
	e.stateMetric.Reset()

	selfNode := ""
	if n, ok := ctx.Value(nodeName).(string); ok {
		selfNode = n
	}
	cluster := ""
	if n, ok := ctx.Value(clusterName).(string); ok {
		cluster = n
	}

	for key, gauge := range e.connectionMetricsG[instance] {
		for _, connD := range rabbitConnectionResponses {
			if value, ok := connD.metrics[key]; ok {
				self := selfLabel(*info.conf, connD.labels["node"] == selfNode)
				gauge.WithLabelValues(cluster, connD.labels["vhost"], connD.labels["node"], connD.labels["peer_host"], connD.labels["user"], self, instance).Add(value)
			}
		}
	}

	for _, connD := range rabbitConnectionResponses {
		self := selfLabel(*info.conf, connD.labels["node"] == selfNode)
		e.stateMetric.WithLabelValues(cluster, connD.labels["vhost"], connD.labels["node"], connD.labels["peer_host"], connD.labels["user"], connD.labels["state"], self, instance).Add(1)
	}

	for _, gauge := range e.connectionMetricsG[instance] {
		gauge.Collect(ch)
	}
	e.stateMetric.Collect(ch)

	return nil
}

func (e exporterConnections) Describe(ch chan<- *prometheus.Desc, instance string) {
	for _, nodeMetric := range e.connectionMetricsG[instance] {
		nodeMetric.Describe(ch)
	}
}
