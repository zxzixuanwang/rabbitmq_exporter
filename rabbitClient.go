package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

type request struct {
	*http.Client
}

func initClient(oConfig rabbitExporterConfig) *request {
	var roots *x509.CertPool

	if data, err := os.ReadFile(oConfig.CAFile); err == nil {
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM(data) {
			log.WithField("filename", oConfig.CAFile).Error("Adding certificate to rootCAs failed")
		}
	} else {
		var err error
		log.Info("Using default certificate pool")
		roots, err = x509.SystemCertPool()
		if err != nil {
			log.WithError(err).Error("retriving system cert pool failed")
		}

	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: oConfig.InsecureSkipVerify,
			RootCAs:            roots,
		},
	}

	_, errCertFile := os.Stat(oConfig.CertFile)
	_, errKeyFile := os.Stat(oConfig.KeyFile)
	if errCertFile == nil && errKeyFile == nil {
		log.Info("Using client certificate: " + oConfig.CertFile + " and key: " + oConfig.KeyFile)
		if cert, err := tls.LoadX509KeyPair(oConfig.CertFile, oConfig.KeyFile); err == nil {
			tr.TLSClientConfig.ClientAuth = tls.RequireAndVerifyClientCert
			tr.TLSClientConfig.Certificates = []tls.Certificate{cert}
		} else {
			log.WithField("certFile", oConfig.CertFile).
				WithField("keyFile", oConfig.KeyFile).
				Error("Loading client certificate and key failed: ", err)
		}
	}

	return &request{
		&http.Client{
			Transport: tr,
			Timeout:   time.Duration(config.Timeout) * time.Second,
		},
	}
}

func (r *request) apiRequest(config rabbitExporterConfig, endpoint string) ([]byte, string, error) {
	var args string
	enabled, exists := config.RabbitCapabilities[rabbitCapNoSort]
	if enabled && exists {
		args = "?sort="
	}

	if endpoint == "aliveness-test" {
		escapeAlivenessVhost := url.QueryEscape(config.AlivenessVhost)
		args = "/" + escapeAlivenessVhost
	}

	req, err := http.NewRequest("GET", config.RabbitURL+"/api/"+endpoint+args, nil)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "host": config.RabbitURL}).Error("Error while constructing rabbitHost request")
		return nil, "", errors.New("Error while constructing rabbitHost request")
	}

	req.SetBasicAuth(config.RabbitUsername, config.RabbitPassword)
	req.Header.Add("Accept", acceptContentType(config))

	resp, err := r.Do(req)

	if err != nil || resp == nil || resp.StatusCode != 200 {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		log.WithFields(log.Fields{"error": err, "host": config.RabbitURL, "statusCode": status}).Error("Error while retrieving data from rabbitHost")
		return nil, "", errors.New("Error while retrieving data from rabbitHost")
	}

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	content := resp.Header.Get("Content-type")
	if err != nil {
		return nil, "", err
	}
	log.WithFields(log.Fields{"body": string(body), "endpoint": endpoint}).Debug("Metrics loaded")

	return body, content, nil
}

func (r *request) loadMetrics(config rabbitExporterConfig, endpoint string) (RabbitReply, error) {
	body, content, err := r.apiRequest(config, endpoint)
	if err != nil {
		return nil, err
	}
	return MakeReply(content, body)
}

func (r *request) getStatsInfo(config rabbitExporterConfig, apiEndpoint string, labels []string) ([]StatsInfo, error) {
	var q []StatsInfo

	reply, err := r.loadMetrics(config, apiEndpoint)
	if err != nil {
		return q, err
	}

	q = reply.MakeStatsInfo(labels)

	return q, nil
}

func (r *request) getMetricMap(config rabbitExporterConfig, apiEndpoint string) (MetricMap, error) {
	var overview MetricMap

	body, content, err := r.apiRequest(config, apiEndpoint)
	if err != nil {
		return overview, err
	}

	reply, err := MakeReply(content, body)
	if err != nil {
		return overview, err
	}

	return reply.MakeMap(), nil
}

func acceptContentType(config rabbitExporterConfig) string {
	if isCapEnabled(config, rabbitCapBert) {
		return "application/bert, application/json;q=0.1"
	}
	return "application/json"
}
