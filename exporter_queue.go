package main

import (
	"context"
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

var (
	queueLabels    = []string{"cluster", "vhost", "queue", "durable", "policy", "self"}
	queueLabelKeys = []string{"vhost", "name", "durable", "policy", "state", "node", "idle_since"}

	queueGaugeVecFunc = func() map[string]*prometheus.GaugeVec {
		return map[string]*prometheus.GaugeVec{
			"messages_ready":                            newGaugeVec("queue_messages_ready", "Number of messages ready to be delivered to clients.", appendInstanceLabel(queueLabels)),
			"messages_unacknowledged":                   newGaugeVec("queue_messages_unacknowledged", "Number of messages delivered to clients but not yet acknowledged.", appendInstanceLabel(queueLabels)),
			"messages":                                  newGaugeVec("queue_messages", "Sum of ready and unacknowledged messages (queue depth).", appendInstanceLabel(queueLabels)),
			"messages_ready_ram":                        newGaugeVec("queue_messages_ready_ram", "Number of messages from messages_ready which are resident in ram.", appendInstanceLabel(queueLabels)),
			"messages_unacknowledged_ram":               newGaugeVec("queue_messages_unacknowledged_ram", "Number of messages from messages_unacknowledged which are resident in ram.", appendInstanceLabel(queueLabels)),
			"messages_ram":                              newGaugeVec("queue_messages_ram", "Total number of messages which are resident in ram.", appendInstanceLabel(queueLabels)),
			"messages_persistent":                       newGaugeVec("queue_messages_persistent", "Total number of persistent messages in the queue (will always be 0 for transient queues).", appendInstanceLabel(queueLabels)),
			"message_bytes":                             newGaugeVec("queue_message_bytes", "Sum of the size of all message bodies in the queue. This does not include the message properties (including headers) or any overhead.", appendInstanceLabel(queueLabels)),
			"message_bytes_ready":                       newGaugeVec("queue_message_bytes_ready", "Like message_bytes but counting only those messages ready to be delivered to clients.", appendInstanceLabel(queueLabels)),
			"message_bytes_unacknowledged":              newGaugeVec("queue_message_bytes_unacknowledged", "Like message_bytes but counting only those messages delivered to clients but not yet acknowledged.", appendInstanceLabel(queueLabels)),
			"message_bytes_ram":                         newGaugeVec("queue_message_bytes_ram", "Like message_bytes but counting only those messages which are in RAM.", appendInstanceLabel(queueLabels)),
			"message_bytes_persistent":                  newGaugeVec("queue_message_bytes_persistent", "Like message_bytes but counting only those messages which are persistent.", appendInstanceLabel(queueLabels)),
			"consumers":                                 newGaugeVec("queue_consumers", "Number of consumers.", appendInstanceLabel(queueLabels)),
			"consumer_utilisation":                      newGaugeVec("queue_consumer_utilisation", "Fraction of the time (between 0.0 and 1.0) that the queue is able to immediately deliver messages to consumers. This can be less than 1.0 if consumers are limited by network congestion or prefetch count.", appendInstanceLabel(queueLabels)),
			"memory":                                    newGaugeVec("queue_memory", "Bytes of memory consumed by the Erlang process associated with the queue, including stack, heap and internal structures.", appendInstanceLabel(queueLabels)),
			"head_message_timestamp":                    newGaugeVec("queue_head_message_timestamp", "The timestamp property of the first message in the queue, if present. Timestamps of messages only appear when they are in the paged-in state.", appendInstanceLabel(queueLabels)), //https://github.com/rabbitmq/rabbitmq-server/pull/54
			"garbage_collection.min_heap_size":          newGaugeVec("queue_gc_min_heap", "Minimum heap size in words", appendInstanceLabel(queueLabels)),
			"garbage_collection.min_bin_vheap_size":     newGaugeVec("queue_gc_min_vheap", "Minimum binary virtual heap size in words", appendInstanceLabel(queueLabels)),
			"garbage_collection.fullsweep_after":        newGaugeVec("queue_gc_collections_before_fullsweep", "Maximum generational collections before fullsweep", appendInstanceLabel(queueLabels)),
			"slave_nodes_len":                           newGaugeVec("queue_slaves_nodes_len", "Number of slave nodes attached to the queue", appendInstanceLabel(queueLabels)),
			"synchronised_slave_nodes_len":              newGaugeVec("queue_synchronised_slave_nodes_len", "Number of slave nodes in sync to the queue", appendInstanceLabel(queueLabels)),
			"members_len":                               newGaugeVec("queue_member_nodes_len", "Number of quorum queue member nodes for the queue", appendInstanceLabel(queueLabels)),
			"online_len":                                newGaugeVec("queue_online_nodes_len", "Number of online members nodes for the queue", appendInstanceLabel(queueLabels)),
			"message_stats.publish_details.rate":        newGaugeVec("queue_messages_publish_rate", "Rate at which messages are entering the server.", appendInstanceLabel(queueLabels)),
			"message_stats.deliver_no_ack_details.rate": newGaugeVec("queue_messages_deliver_no_ack_rate", "Rate at which messages are delivered to consumers that use automatic acknowledgements.", appendInstanceLabel(queueLabels)),
			"message_stats.deliver_details.rate":        newGaugeVec("queue_messages_deliver_rate", "Rate at which messages are delivered to consumers that use manual acknowledgements.", appendInstanceLabel(queueLabels)),
		}
	}
	limitsGaugeVecFunc = func() map[string]*prometheus.GaugeVec {
		return map[string]*prometheus.GaugeVec{
			"max-length-bytes": newGaugeVec("queue_max_length_bytes", "Total body size for ready messages a queue can contain before it starts to drop them from its head.", appendInstanceLabel(queueLabels)),
			"max-length":       newGaugeVec("queue_max_length", "How many (ready) messages a queue can contain before it starts to drop them from its head.", appendInstanceLabel(queueLabels)),
		}
	}

	queueCounterVecFunc = func() map[string]*prometheus.Desc {
		return map[string]*prometheus.Desc{
			"disk_reads":                   newDesc("queue_disk_reads_total", "Total number of times messages have been read from disk by this queue since it started.", appendInstanceLabel(queueLabels)),
			"disk_writes":                  newDesc("queue_disk_writes_total", "Total number of times messages have been written to disk by this queue since it started.", appendInstanceLabel(queueLabels)),
			"message_stats.publish":        newDesc("queue_messages_published_total", "Count of messages published.", appendInstanceLabel(queueLabels)),
			"message_stats.confirm":        newDesc("queue_messages_confirmed_total", "Count of messages confirmed. ", appendInstanceLabel(queueLabels)),
			"message_stats.deliver":        newDesc("queue_messages_delivered_total", "Count of messages delivered in acknowledgement mode to consumers.", appendInstanceLabel(queueLabels)),
			"message_stats.deliver_no_ack": newDesc("queue_messages_delivered_noack_total", "Count of messages delivered in no-acknowledgement mode to consumers. ", appendInstanceLabel(queueLabels)),
			"message_stats.get":            newDesc("queue_messages_get_total", "Count of messages delivered in acknowledgement mode in response to basic.get.", appendInstanceLabel(queueLabels)),
			"message_stats.get_no_ack":     newDesc("queue_messages_get_noack_total", "Count of messages delivered in no-acknowledgement mode in response to basic.get.", appendInstanceLabel(queueLabels)),
			"message_stats.redeliver":      newDesc("queue_messages_redelivered_total", "Count of subset of messages in deliver_get which had the redelivered flag set.", appendInstanceLabel(queueLabels)),
			"message_stats.return":         newDesc("queue_messages_returned_total", "Count of messages returned to publisher as unroutable.", appendInstanceLabel(queueLabels)),
			"message_stats.ack":            newDesc("queue_messages_ack_total", "Count of messages delivered in acknowledgement mode in response to basic.get.", appendInstanceLabel(queueLabels)),
			"reductions":                   newDesc("queue_reductions_total", "Count of  reductions which take place on this process. .", appendInstanceLabel(queueLabels)),
			"garbage_collection.minor_gcs": newDesc("queue_gc_minor_collections_total", "Number of minor GCs", appendInstanceLabel(queueLabels)),
		}
	}
)

type exporterQueue struct {
	limitsGauge         map[string]map[string]*prometheus.GaugeVec
	queueMetricsGauge   map[string]map[string]*prometheus.GaugeVec
	queueMetricsCounter map[string]map[string]*prometheus.Desc
	stateMetric         map[string]*prometheus.GaugeVec
	idleSinceMetric     map[string]*prometheus.GaugeVec
}

func newExporterQueue() Exporter {
	queueGaugeVecActual := make(map[string]map[string]*prometheus.GaugeVec)
	queueCounterVecActual := make(map[string]map[string]*prometheus.Desc)
	litmitsGaugeVecActual := make(map[string]map[string]*prometheus.GaugeVec)
	stateMetric := make(map[string]*prometheus.GaugeVec)
	idleSinceMetric := make(map[string]*prometheus.GaugeVec)
	for instance, info := range targetList {
		if len(queueGaugeVecActual[instance]) < 1 {
			queueGaugeVecActual[instance] = queueGaugeVecFunc()
		}
		if len(queueCounterVecActual[instance]) < 1 {
			queueCounterVecActual[instance] = queueCounterVecFunc()
		}
		if len(litmitsGaugeVecActual[instance]) < 1 {
			litmitsGaugeVecActual[instance] = limitsGaugeVecFunc()
		}
		litmitsGaugeVecActual[instance] = limitsGaugeVecFunc()
		stateMetric[instance] = newGaugeVec("queue_state", "A metric with a value of constant '1' if the queue is in a certain state", appendInstanceLabel(append(queueLabels, "state")))
		idleSinceMetric[instance] = newGaugeVec("queue_idle_since_seconds", "starttime where the queue switched to idle state; in seconds since epoch (1970).", appendInstanceLabel(queueLabels))
		if len(info.conf.ExcludeMetrics) > 0 {
			for _, metric := range info.conf.ExcludeMetrics {
				if queueGaugeVecActual[instance][metric] != nil {
					delete(queueGaugeVecActual[instance], metric)
				}
				if queueCounterVecActual[instance][metric] != nil {
					delete(queueCounterVecActual[instance], metric)
				}
				if litmitsGaugeVecActual[instance][metric] != nil {
					delete(litmitsGaugeVecActual[instance], metric)
				}
			}
		}
	}

	return exporterQueue{
		limitsGauge:         litmitsGaugeVecActual,
		queueMetricsGauge:   queueGaugeVecActual,
		queueMetricsCounter: queueCounterVecActual,
		stateMetric:         stateMetric,
		idleSinceMetric:     idleSinceMetric,
	}
}

func collectLowerMetric(metricA, metricB string, stats StatsInfo) float64 {
	mA, okA := stats.metrics[metricA]
	mB, okB := stats.metrics[metricB]

	if okA && okB {
		if mA < mB {
			return mA
		} else {
			return mB
		}
	}
	if okA {
		return mA
	}
	if okB {
		return mB
	}
	return -1.0
}

func (e exporterQueue) Collect(ctx context.Context, ch chan<- prometheus.Metric, instance string) error {
	info := targetList[instance]
	for _, gaugevec := range e.queueMetricsGauge[instance] {
		gaugevec.Reset()
	}
	for _, m := range e.limitsGauge[instance] {
		m.Reset()
	}
	e.stateMetric[instance].Reset()
	e.idleSinceMetric[instance].Reset()

	if info.conf.MaxQueues > 0 {
		// Get overview info to check total queues
		totalQueues, ok := ctx.Value(totalQueues).(int)
		if !ok {
			return errors.New("total Queue counter missing")
		}

		if totalQueues > info.conf.MaxQueues {
			log.WithFields(log.Fields{
				"MaxQueues":   info.conf.MaxQueues,
				"TotalQueues": totalQueues,
			}).Debug("MaxQueues exceeded.")
			return nil
		}
	}

	selfNode := ""
	if n, ok := ctx.Value(nodeName).(string); ok {
		selfNode = n
	}
	cluster := ""
	if n, ok := ctx.Value(clusterName).(string); ok {
		cluster = n
	}

	rabbitMqQueueData, err := info.req.getStatsInfo(*info.conf, "queues", queueLabelKeys)
	if err != nil {
		return err
	}

	log.WithField("queueData", rabbitMqQueueData).Debug("Queue data")
	for _, queue := range rabbitMqQueueData {
		qname := queue.labels["name"]
		vname := queue.labels["vhost"]
		if vhostIncluded := info.conf.IncludeVHost.MatchString(vname); !vhostIncluded {
			continue
		}
		if skipVhost := info.conf.SkipVHost.MatchString(vname); skipVhost {
			continue
		}
		if queueIncluded := info.conf.IncludeQueues.MatchString(qname); !queueIncluded {
			continue
		}
		if queueSkipped := info.conf.SkipQueues.MatchString(qname); queueSkipped {
			continue
		}

		self := selfLabel(*info.conf, queue.labels["node"] == selfNode)
		labelValues := []string{cluster, queue.labels["vhost"], queue.labels["name"], queue.labels["durable"], queue.labels["policy"], self}
		instanceLabel := append(labelValues, instance)
		for key, gaugevec := range e.queueMetricsGauge[instance] {
			if value, ok := queue.metrics[key]; ok {
				log.WithFields(log.Fields{"vhost": queue.labels["vhost"], "queue": queue.labels["name"], "key": key, "value": value, "instance": instance}).Debug("Set queue metric for key")
				gaugevec.WithLabelValues(instanceLabel...).Set(value)
			}
		}

		for key, countvec := range e.queueMetricsCounter[instance] {
			if value, ok := queue.metrics[key]; ok {
				ch <- prometheus.MustNewConstMetric(countvec, prometheus.CounterValue, value, instanceLabel...)
			} else {
				ch <- prometheus.MustNewConstMetric(countvec, prometheus.CounterValue, 0, instanceLabel...)
			}
		}

		state := queue.labels["state"]
		idleSince, exists := queue.labels["idle_since"]
		if exists && idleSince != "" {

			if t, err := parseTime(idleSince); err == nil {
				unixSeconds := float64(t.UnixNano()) / 1e9

				if state == "running" { //replace running state with idle if idle_since time is provided. Other states (flow, etc.) are not replaced
					state = "idle"
				}
				e.idleSinceMetric[instance].WithLabelValues(instanceLabel...).Set(unixSeconds)
			} else {
				log.WithError(err).WithField("idle_since", idleSince).Warn("error parsing idle since time")
			}
		}
		e.stateMetric[instance].WithLabelValues(append(append(labelValues, state), instance)...).Set(1)

		if _, ok := e.limitsGauge[instance]["max-length"]; ok {
			if f := collectLowerMetric("arguments.x-max-length", "effective_policy_definition.max-length", queue); f >= 0 {
				e.limitsGauge[instance]["max-length"].WithLabelValues(instanceLabel...).Set(f)
			}
		}

		if _, ok := e.limitsGauge[instance]["max-length-bytes"]; ok {
			if f := collectLowerMetric("arguments.x-max-length-bytes", "effective_policy_definition.max-length-bytes", queue); f >= 0 {
				e.limitsGauge[instance]["max-length-bytes"].WithLabelValues(instanceLabel...).Set(f)
			}
		}

	}

	for _, metric := range e.limitsGauge[instance] {
		metric.Collect(ch)
	}
	for _, gaugevec := range e.queueMetricsGauge[instance] {
		gaugevec.Collect(ch)
	}
	e.stateMetric[instance].Collect(ch)
	e.idleSinceMetric[instance].Collect(ch)

	return nil
}

func (e exporterQueue) Describe(ch chan<- *prometheus.Desc, instance string) {
	for _, metric := range e.limitsGauge[instance] {
		metric.Describe(ch)
	}
	for _, gaugevec := range e.queueMetricsGauge[instance] {
		gaugevec.Describe(ch)
	}
	e.stateMetric[instance].Describe(ch)
	e.idleSinceMetric[instance].Describe(ch)
	for _, countervec := range e.queueMetricsCounter[instance] {
		ch <- countervec
	}
}
