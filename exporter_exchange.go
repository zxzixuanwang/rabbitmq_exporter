package main

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	exchangeLabels        = []string{"cluster", "vhost", "exchange"}
	exchangeLabelKeys     = []string{"vhost", "name", "type"}
	exchangeBindLabelKeys = []string{"destination_type", "source", "vhost"}
)

type exporterExchange struct {
	exchangeMetrics      map[string]map[string]*prometheus.Desc
	exchangeBindGaugeVec map[string]*prometheus.GaugeVec
}

func newExporterExchange() Exporter {
	exchangeBindGaugeVec := newGaugeVec("exchange_exchange_bindings", "Number of bindings for an exchange. This value is cluster-wide.", []string{"exchange", "type", "vhost", "instance"})

	exchangeCounterVecActual := make(map[string]map[string]*prometheus.Desc)
	exchangeBindGaugeVecActual := make(map[string]*prometheus.GaugeVec)
	for instance, info := range targetList {
		if len(exchangeCounterVecActual[instance]) < 1 {
			exchangeCounterVec := map[string]*prometheus.Desc{
				"message_stats.publish":           newDesc("exchange_messages_published_total", "Count of messages published.", appendInstanceLabel(exchangeLabels)),
				"message_stats.publish_in":        newDesc("exchange_messages_published_in_total", "Count of messages published in to an exchange, i.e. not taking account of routing.", appendInstanceLabel(exchangeLabels)),
				"message_stats.publish_out":       newDesc("exchange_messages_published_out_total", "Count of messages published out of an exchange, i.e. taking account of routing.", appendInstanceLabel(exchangeLabels)),
				"message_stats.confirm":           newDesc("exchange_messages_confirmed_total", "Count of messages confirmed. ", appendInstanceLabel(exchangeLabels)),
				"message_stats.deliver":           newDesc("exchange_messages_delivered_total", "Count of messages delivered in acknowledgement mode to consumers.", appendInstanceLabel(exchangeLabels)),
				"message_stats.deliver_no_ack":    newDesc("exchange_messages_delivered_noack_total", "Count of messages delivered in no-acknowledgement mode to consumers. ", appendInstanceLabel(exchangeLabels)),
				"message_stats.get":               newDesc("exchange_messages_get_total", "Count of messages delivered in acknowledgement mode in response to basic.get.", appendInstanceLabel(exchangeLabels)),
				"message_stats.get_no_ack":        newDesc("exchange_messages_get_noack_total", "Count of messages delivered in no-acknowledgement mode in response to basic.get.", appendInstanceLabel(exchangeLabels)),
				"message_stats.ack":               newDesc("exchange_messages_ack_total", "Count of messages delivered in acknowledgement mode in response to basic.get.", appendInstanceLabel(exchangeLabels)),
				"message_stats.redeliver":         newDesc("exchange_messages_redelivered_total", "Count of subset of messages in deliver_get which had the redelivered flag set.", appendInstanceLabel(exchangeLabels)),
				"message_stats.return_unroutable": newDesc("exchange_messages_returned_total", "Count of messages returned to publisher as unroutable.", appendInstanceLabel(exchangeLabels)),
			}
			exchangeCounterVecActual[instance] = exchangeCounterVec
		}
		exchangeBindGaugeVecActual[instance] = exchangeBindGaugeVec
		if len(info.conf.ExcludeMetrics) > 0 {
			for _, metric := range info.conf.ExcludeMetrics {
				if exchangeCounterVecActual[instance][metric] != nil {
					delete(exchangeCounterVecActual[instance], metric)
				}
			}
		}
	}

	return exporterExchange{
		exchangeMetrics:      exchangeCounterVecActual,
		exchangeBindGaugeVec: exchangeBindGaugeVecActual,
	}
}

func (e exporterExchange) Collect(ctx context.Context, ch chan<- prometheus.Metric, instance string) error {
	info := targetList[instance]
	exchangeData, err := info.req.getStatsInfo(*info.conf, "exchanges", exchangeLabelKeys)
	if err != nil {
		return err
	}
	cluster := ""
	if n, ok := ctx.Value(clusterName).(string); ok {
		cluster = n
	}
	bindInfoTempMap := make(map[string]*StatsInfo)
	for key, countvec := range e.exchangeMetrics[instance] {
		for _, exchange := range exchangeData {
			ename := exchange.labels["name"]
			vname := exchange.labels["vhost"]
			if info.conf.BindExchagesSet[ename] {
				bindInfoTempMap[ename] = &exchange
			}
			if vhostIncluded := info.conf.IncludeVHost.MatchString(vname); !vhostIncluded {
				continue
			}
			if skipVhost := info.conf.SkipVHost.MatchString(vname); skipVhost {
				continue
			}
			if exchangeIncluded := info.conf.IncludeExchanges.MatchString(ename); !exchangeIncluded {
				continue
			}
			if exchangeSkipped := info.conf.SkipExchanges.MatchString(ename); exchangeSkipped {
				continue
			}
			if value, ok := exchange.metrics[key]; ok {
				// log.WithFields(log.Fields{"vhost": exchange.vhost, "exchange": exchange.name, "key": key, "value": value}).Debug("Set exchange metric for key")
				ch <- prometheus.MustNewConstMetric(countvec, prometheus.CounterValue, value, cluster, exchange.labels["vhost"], exchange.labels["name"], instance)
			}
		}
	}
	for k := range info.conf.BindExchagesSet {
		if bindInfoTempMap[k] != nil {
			url := "exchanges/%2F/" + k + "/bindings/source"
			exchangeData, err := info.req.getStatsInfo(*info.conf, url, exchangeBindLabelKeys)
			if err != nil {
				return err
			}

			mqType := bindInfoTempMap[k].labels["type"]
			mqVhost := bindInfoTempMap[k].labels["vhost"]
			e.exchangeBindGaugeVec[instance].WithLabelValues(k, mqType, mqVhost, instance).Set(float64(len(exchangeData)))
			e.exchangeBindGaugeVec[instance].Collect(ch)
		}
	}

	return nil
}

func (e exporterExchange) Describe(ch chan<- *prometheus.Desc, instance string) {
	for _, exchangeMetric := range e.exchangeMetrics[instance] {
		ch <- exchangeMetric
	}
}
