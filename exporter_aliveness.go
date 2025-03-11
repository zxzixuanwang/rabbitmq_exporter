package main

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

var (
	alivenessLabels = []string{"vhost"}

	alivenessGaugeVecFunc = func() map[string]*prometheus.GaugeVec {
		return map[string]*prometheus.GaugeVec{"vhost.aliveness": newGaugeVec("aliveness_test", "vhost aliveness test", appendInstanceLabel(alivenessLabels))}
	}
	rabbitmqAlivenessMetricFunc = func() *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rabbitmq_aliveness_info",
				Help: "A metric with value 1 status:ok else 0 labeled by aliveness test status, error, reason",
			},
			[]string{"instance", "status", "error", "reason"},
		)
	}

	rabbitmqAlivenessMetrics map[string]*prometheus.GaugeVec
)

type exporterAliveness struct {
	alivenessMetrics map[string]map[string]*prometheus.GaugeVec
	alivenessInfo    map[string]AlivenessInfo
}

type AlivenessInfo struct {
	Status string
	Error  string
	Reason string
}

func newExporterAliveness() Exporter {
	alivenessGaugeVecActual := make(map[string]map[string]*prometheus.GaugeVec, len(targetList))
	alivenessInfo := make(map[string]AlivenessInfo, len(targetList))
	rabbitmqAlivenessMetrics = make(map[string]*prometheus.GaugeVec, len(targetList))
	for instance, info := range targetList {
		rabbitmqAlivenessMetrics[instance] = rabbitmqAlivenessMetricFunc()
		alivenessGaugeVecActual[instance] = alivenessGaugeVecFunc()
		alivenessInfo[instance] = AlivenessInfo{}
		if len(info.conf.ExcludeMetrics) > 0 {
			for _, metric := range info.conf.ExcludeMetrics {
				if alivenessGaugeVecActual[instance][metric] != nil {
					delete(alivenessGaugeVecActual[instance], metric)
				}
			}
		}
	}

	return &exporterAliveness{
		alivenessMetrics: alivenessGaugeVecActual,
		alivenessInfo:    alivenessInfo,
	}
}

func (e *exporterAliveness) Collect(ctx context.Context, ch chan<- prometheus.Metric, instance string) error {
	info := targetList[instance]

	body, contentType, err := info.req.apiRequest(*info.conf, "aliveness-test")
	if err != nil {
		return err
	}

	reply, err := MakeReply(contentType, body)
	if err != nil {
		return err
	}

	rabbitMqAlivenessData := reply.MakeMap()
	status, _ := reply.GetString("status")
	errorS, _ := reply.GetString("error")
	reason, _ := reply.GetString("reason")
	ai := AlivenessInfo{
		Status: status,
		Error:  errorS,
		Reason: reason,
	}
	e.alivenessInfo[instance] = ai
	var flag float64 = 0
	if e.alivenessInfo[instance].Status == "ok" {
		flag = 1
	}

	rabbitmqAlivenessMetrics[instance].Reset()
	rabbitmqAlivenessMetrics[instance].WithLabelValues(instance, e.alivenessInfo[instance].Status, e.alivenessInfo[instance].Error, e.alivenessInfo[instance].Reason).Set(flag)
	log.WithField("alivenesswData", rabbitMqAlivenessData).Debug("Aliveness data")
	for key, gauge := range e.alivenessMetrics[instance] {
		if value, ok := rabbitMqAlivenessData[key]; ok {
			log.WithFields(log.Fields{"key": key, "value": value}).Debug("Set aliveness metric for key")
			gauge.WithLabelValues(e.alivenessInfo[instance].Status, instance).Set(value)
		}
	}

	if ch != nil {
		rabbitmqAlivenessMetrics[instance].Collect(ch)
		for _, gauge := range e.alivenessMetrics[instance] {
			gauge.Collect(ch)
		}
	}

	return nil
}

func (e exporterAliveness) Describe(ch chan<- *prometheus.Desc, instance string) {
	gauge, have := rabbitmqVersionMetrics.Load(instance)
	if have {
		gauge.Describe(ch)
	}

	for _, gauge := range e.alivenessMetrics[instance] {
		gauge.Describe(ch)
	}
}
