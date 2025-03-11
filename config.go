package main

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/tkanos/gonfig"
)

var (
	config rabbitExporterConfigs
)

type rabbitExporterConfigs struct {
	Timeout      int                    `json:"timeout"`
	Config       []rabbitExporterConfig `json:"rabbit_exporter_config"`
	OutputFormat string                 `json:"output_format"`
	PublishPort  string                 `json:"publish_port"`
	PublishAddr  string                 `json:"publish_addr"`
}

type rabbitExporterConfig struct {
	RabbitURL                string              `json:"rabbit_url"`
	RabbitUsername           string              `json:"rabbit_user"`
	RabbitPassword           string              `json:"rabbit_pass"`
	RabbitConnection         string              `json:"rabbit_connection"`
	CAFile                   string              `json:"ca_file"`
	CertFile                 string              `json:"cert_file"`
	KeyFile                  string              `json:"key_file"`
	InsecureSkipVerify       bool                `json:"insecure_skip_verify"`
	ExcludeMetrics           []string            `json:"exlude_metrics"`
	SkipExchanges            *regexp.Regexp      `json:"-"`
	IncludeExchanges         *regexp.Regexp      `json:"-"`
	SkipQueues               *regexp.Regexp      `json:"-"`
	IncludeQueues            *regexp.Regexp      `json:"-"`
	SkipVHost                *regexp.Regexp      `json:"-"`
	IncludeVHost             *regexp.Regexp      `json:"-"`
	IncludeExchangesString   string              `json:"include_exchanges"`
	SkipExchangesString      string              `json:"skip_exchanges"`
	IncludeQueuesString      string              `json:"include_queues"`
	SkipQueuesString         string              `json:"skip_queues"`
	SkipVHostString          string              `json:"skip_vhost"`
	IncludeVHostString       string              `json:"include_vhost"`
	RabbitCapabilitiesString string              `json:"rabbit_capabilities"`
	RabbitCapabilities       rabbitCapabilitySet `json:"-"`
	AlivenessVhost           string              `json:"aliveness_vhost"`
	EnabledExporters         []string            `json:"enabled_exporters"`
	MaxQueues                int                 `json:"max_queues"`
	BindExchagesList         []string            `json:"bind_exchange_list"`
	BindExchagesSet          map[string]bool     `json:"-"`
}

type rabbitCapability string
type rabbitCapabilitySet map[rabbitCapability]bool

const (
	rabbitCapNoSort rabbitCapability = "no_sort"
	rabbitCapBert   rabbitCapability = "bert"
)

var allRabbitCapabilities = rabbitCapabilitySet{
	rabbitCapNoSort: true,
	rabbitCapBert:   true,
}

func initConfigFromFile(configFile string) error {
	config = rabbitExporterConfigs{}
	err := gonfig.GetConf(configFile, &config)
	if err != nil {
		return err
	}
	reg, err := regexp.Compile("https?://[a-zA-Z.0-9]+")
	if err != nil {
		panic(err)
	}
	for i := 0; i < len(config.Config); i++ {
		conf := config.Config[i]
		if url := conf.RabbitURL; url != "" {
			if !reg.MatchString(strings.ToLower(url)) {
				panic(fmt.Errorf("rabbit URL must start with http:// or https://"))
			}
		}

		config.Config[i].SkipExchanges = regexp.MustCompile(conf.SkipExchangesString)
		config.Config[i].IncludeExchanges = regexp.MustCompile(conf.IncludeExchangesString)
		config.Config[i].SkipQueues = regexp.MustCompile(conf.SkipQueuesString)
		config.Config[i].IncludeQueues = regexp.MustCompile(conf.IncludeQueuesString)
		config.Config[i].SkipVHost = regexp.MustCompile(conf.SkipVHostString)
		config.Config[i].IncludeVHost = regexp.MustCompile(conf.IncludeVHostString)
		config.Config[i].RabbitCapabilities = parseCapabilities(conf.RabbitCapabilitiesString)
		config.Config[i].BindExchagesSet = make(map[string]bool, len(conf.BindExchagesList))
		for _, v := range conf.BindExchagesList {
			config.Config[i].BindExchagesSet[v] = true
		}

	}
	return nil
}

func parseCapabilities(raw string) rabbitCapabilitySet {
	result := make(rabbitCapabilitySet)
	candidates := strings.Split(raw, ",")
	for _, maybeCapStr := range candidates {
		maybeCap := rabbitCapability(strings.TrimSpace(maybeCapStr))
		enabled, present := allRabbitCapabilities[maybeCap]
		if enabled && present {
			result[maybeCap] = true
		}
	}
	return result
}

func isCapEnabled(config rabbitExporterConfig, cap rabbitCapability) bool {
	exists, enabled := config.RabbitCapabilities[cap]
	return exists && enabled
}

func selfLabel(config rabbitExporterConfig, isSelf bool) string {
	if config.RabbitConnection == "loadbalancer" {
		return "lb"
	} else if isSelf {
		return "1"
	} else {
		return "0"
	}
}
