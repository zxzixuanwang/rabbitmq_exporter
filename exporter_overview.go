package main

import (
	"context"

	"github.com/lrita/cmap"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

func init() {
	//RegisterExporter("overview", newExporterOverview)
}

var (
	overviewLabels = []string{"cluster"}

	overviewMetricDescriptionFunc = func() map[string]*prometheus.GaugeVec {
		return map[string]*prometheus.GaugeVec{
			"object_totals.channels":                    newGaugeVec("channels", "Number of channels.", appendInstanceLabel(overviewLabels)),
			"object_totals.connections":                 newGaugeVec("connections", "Number of connections.", appendInstanceLabel(overviewLabels)),
			"object_totals.consumers":                   newGaugeVec("consumers", "Number of message consumers.", appendInstanceLabel(overviewLabels)),
			"object_totals.queues":                      newGaugeVec("queues", "Number of queues in use.", appendInstanceLabel(overviewLabels)),
			"object_totals.exchanges":                   newGaugeVec("exchanges", "Number of exchanges in use.", appendInstanceLabel(overviewLabels)),
			"queue_totals.messages":                     newGaugeVec("queue_messages_global", "Number ready and unacknowledged messages in cluster.", appendInstanceLabel(overviewLabels)),
			"queue_totals.messages_ready":               newGaugeVec("queue_messages_ready_global", "Number of messages ready to be delivered to clients.", appendInstanceLabel(overviewLabels)),
			"queue_totals.messages_unacknowledged":      newGaugeVec("queue_messages_unacknowledged_global", "Number of messages delivered to clients but not yet acknowledged.", appendInstanceLabel(overviewLabels)),
			"message_stats.publish_details.rate":        newGaugeVec("messages_publish_rate", "Rate at which messages are entering the server.", appendInstanceLabel(overviewLabels)),
			"message_stats.deliver_no_ack_details.rate": newGaugeVec("messages_deliver_no_ack_rate", "Rate at which messages are delivered to consumers that use automatic acknowledgements.", appendInstanceLabel(overviewLabels)),
			"message_stats.deliver_details.rate":        newGaugeVec("messages_deliver_rate", "Rate at which messages are delivered to consumers that use manual acknowledgements.", appendInstanceLabel(overviewLabels)),
		}
	}
	rabbitmqVersionMetrics    *cmap.Map[string, *prometheus.GaugeVec]
	rabbitmqVersionMetricFunc = func() *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rabbitmq_version_info",
				Help: "A metric with a constant '1' value labeled by rabbitmq version, erlang version, node, cluster.",
			},
			appendInstanceLabel([]string{"rabbitmq", "erlang", "node", "cluster"}),
		)
	}
)

type exporterOverview struct {
	overviewMetrics *cmap.Map[string, map[string]*prometheus.GaugeVec]
	nodeInfo        *cmap.Map[string, NodeInfo]
}

// NodeInfo presents the name and version of fetched rabbitmq
type NodeInfo struct {
	Node            string
	RabbitmqVersion string
	ErlangVersion   string
	ClusterName     string
	TotalQueues     int
}

func newExporterOverview() *exporterOverview {

	overviewMetricDescriptionActual := new(cmap.Map[string, map[string]*prometheus.GaugeVec])
	nodeInfo := new(cmap.Map[string, NodeInfo])
	rabbitmqVersionMetrics = new(cmap.Map[string, *prometheus.GaugeVec])

	for instance, info := range targetList {
		temp := overviewMetricDescriptionFunc()
		if len(info.conf.ExcludeMetrics) > 0 {
			for _, metric := range info.conf.ExcludeMetrics {
				if temp[metric] != nil {
					delete(temp, metric)
				}
			}
		}
		overviewMetricDescriptionActual.Store(instance, temp)
		nodeInfo.Store(instance, NodeInfo{})
		rabbitmqVersionMetrics.Store(instance, rabbitmqVersionMetricFunc())
	}

	return &exporterOverview{
		overviewMetrics: overviewMetricDescriptionActual,
		nodeInfo:        nodeInfo,
	}
}

func (e exporterOverview) NodeInfo(instance string) NodeInfo {
	info, have := e.nodeInfo.Load(instance)
	if have {
		return info
	} else {
		return NodeInfo{}
	}
}

func (e *exporterOverview) Collect(ctx context.Context, ch chan<- prometheus.Metric, instance string) error {
	info := targetList[instance]

	body, contentType, err := info.req.apiRequest(*info.conf, "overview")
	if err != nil {
		return err
	}

	reply, err := MakeReply(contentType, body)
	if err != nil {
		return err
	}

	rabbitMqOverviewData := reply.MakeMap()
	var node, erlangVersion, rabbitmqVersion, clusterName string
	var totalQueues int
	node, _ = reply.GetString("node")
	erlangVersion, _ = reply.GetString("erlang_version")
	rabbitmqVersion, _ = reply.GetString("rabbitmq_version")
	clusterName, _ = reply.GetString("cluster_name")
	totalQueues = (int)(rabbitMqOverviewData["object_totals.queues"])
	ni := NodeInfo{
		Node:            node,
		RabbitmqVersion: rabbitmqVersion,
		ErlangVersion:   erlangVersion,
		ClusterName:     clusterName,
		TotalQueues:     totalQueues,
	}
	e.nodeInfo.Store(instance, ni)

	tempRVM, haveV := rabbitmqVersionMetrics.Load(instance)
	if haveV {
		tempRVM.Reset()
		tempRVM.WithLabelValues(ni.RabbitmqVersion, ni.ErlangVersion, ni.Node, ni.ClusterName, instance).Set(1)
	}

	log.WithField("overviewData", rabbitMqOverviewData).Debug("Overview data")
	tempGauge, have := e.overviewMetrics.Load(instance)
	if have {
		for key, gauge := range tempGauge {
			if value, ok := rabbitMqOverviewData[key]; ok {
				log.WithFields(log.Fields{"key": key, "value": value}).Debug("Set overview metric for key")
				gauge.Reset()
				gauge.WithLabelValues(ni.ClusterName, instance).Set(value)
			}
		}
	}

	if ch != nil {
		if haveV {
			tempRVM.Collect(ch)
		}

		for _, gauge := range tempGauge {
			gauge.Collect(ch)
		}
	}
	e.overviewMetrics.Store(instance, tempGauge)
	rabbitmqVersionMetrics.Store(instance, tempRVM)

	return nil
}

func (e exporterOverview) Describe(ch chan<- *prometheus.Desc, instance string) {
	gauge, have := rabbitmqVersionMetrics.Load(instance)
	if have {
		gauge.Describe(ch)
	}
	gauges, have := e.overviewMetrics.Load(instance)
	if have {
		for _, gauge := range gauges {
			gauge.Describe(ch)
		}
	}
}
