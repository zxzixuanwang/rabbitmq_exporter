package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const (
	defaultLogLevel = log.InfoLevel
)

func initLogger() {
	log.SetLevel(getLogLevel())
	if strings.ToUpper(config.OutputFormat) == "JSON" {
		log.SetFormatter(&log.JSONFormatter{})
	} else {
		// The TextFormatter is default, you don't actually have to do this.
		log.SetFormatter(&log.TextFormatter{})
	}
}

func main() {
	var checkURL = flag.String("check-url", "", "Curl url and return exit code (http: 200 => 0, otherwise 1)")
	var configFile = flag.String("config-file", "conf/rabbitmq.conf", "path to json config")
	flag.Parse()

	if *checkURL != "" { // do a single http get request. Used in docker healthckecks as curl is not inside the image
		curl(*checkURL)
		return
	}

	err := initConfigFromFile(*configFile) //Try parsing config file
	if err != nil {
		panic(err)
	}

	initLogger()
	for _, v := range config.Config {
		c := initClient(v)
		s := strings.Split(v.RabbitURL, "://")
		if len(s) < 1 {
			panic("invalid url")
		}
		targetList[s[1]] = targetInfo{
			conf:     &v,
			req:      c,
			instance: s[1],
		}

		log.WithFields(log.Fields{
			"VERSION":    Version,
			"REVISION":   Revision,
			"BRANCH":     Branch,
			"BUILD_DATE": BuildDate,
			//		"RABBIT_PASSWORD": config.RABBIT_PASSWORD,
		}).Info("Starting RabbitMQ exporter")

		log.WithFields(log.Fields{
			"PUBLISH_ADDR":        config.PublishAddr,
			"PUBLISH_PORT":        config.PublishPort,
			"RABBIT_URL":          v.RabbitURL,
			"RABBIT_USER":         v.RabbitUsername,
			"RABBIT_CONNECTION":   v.RabbitConnection,
			"OUTPUT_FORMAT":       config.OutputFormat,
			"RABBIT_CAPABILITIES": formatCapabilities(v.RabbitCapabilities),
			"RABBIT_EXPORTERS":    v.EnabledExporters,
			"CAFILE":              v.CAFile,
			"CERTFILE":            v.CertFile,
			"KEYFILE":             v.KeyFile,
			"SKIPVERIFY":          v.InsecureSkipVerify,
			"EXCLUDE_METRICS":     v.ExcludeMetrics,
			"SKIP_EXCHANGES":      v.SkipExchanges.String(),
			"INCLUDE_EXCHANGES":   v.IncludeExchanges.String(),
			"SKIP_QUEUES":         v.SkipQueues.String(),
			"INCLUDE_QUEUES":      v.IncludeQueues.String(),
			"SKIP_VHOST":          v.SkipVHost.String(),
			"INCLUDE_VHOST":       v.IncludeVHost.String(),
			"RABBIT_TIMEOUT":      config.Timeout,
			"MAX_QUEUES":          v.MaxQueues,
			//		"RABBIT_PASSWORD": config.RABBIT_PASSWORD,
		}).Info("Active Configuration")
	}
	loadModule()
	// prometheus.MustRegister(tmpSliceExport...)
	exporter := newExporter()

	gatherers := prometheus.Gatherers{
		prometheus.DefaultGatherer,
	}
	nReg := prometheus.NewRegistry()
	nReg.MustRegister(exporter)
	gatherers = append(gatherers, nReg)

	handler := http.NewServeMux()
	handler.Handle("/metrics", promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{}))
	handler.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html>
             <head><title>RabbitMQ Exporter</title></head>
             <body>
             <h1>RabbitMQ Exporter</h1>
             <p><a href='/metrics'>Metrics</a></p>
             </body>
             </html>`))
	})
	handler.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		dead := ""
		/* 		for k, v := range tmpExport {
			if !v.LastScrapeOK() {
				dead += k + ","
			}
		} */
		if len(dead) == 0 {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("unhealthy target: %s", strings.TrimRight(dead, ","))))
		}
	})

	server := &http.Server{Addr: config.PublishAddr + ":" + config.PublishPort, Handler: handler}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	<-runService()
	log.Info("Shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := server.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
	cancel()
}

func getLogLevel() log.Level {
	lvl := strings.ToLower(os.Getenv("LOG_LEVEL"))
	level, err := log.ParseLevel(lvl)
	if err != nil {
		level = defaultLogLevel
	}
	return level
}

func formatCapabilities(caps rabbitCapabilitySet) string {
	var buffer bytes.Buffer
	first := true
	for k := range caps {
		if !first {
			buffer.WriteString(",")
		}
		first = false
		buffer.WriteString(string(k))
	}
	return buffer.String()
}
