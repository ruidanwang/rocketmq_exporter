package collector

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const namespace = "rocketMQ"

var (
	scrapeDurationDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_duration_seconds"),
		"rocketMQ_exporter: Duration of a collector scrape.",
		[]string{"collector"},
		nil,
	)
	scrapeSuccessDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_success"),
		"rocketMQ_exporter: Whether a collector succeeded.",
		[]string{"collector"},
		nil,
	)
)

const (
	defaultEnabled  = true
	defaultDisabled = false
)

var (
	factories        = make(map[string]func(logger log.Logger) (Collector, error))
	collectorState   = make(map[string]*bool)
	forcedCollectors = map[string]bool{} // collectors which have been explicitly enabled or disabled
)

func registerCollector(collector string, isDefaultEnabled bool, factory func(logger log.Logger) (Collector, error)) {
	var helpDefaultState string
	if isDefaultEnabled {
		helpDefaultState = "enabled"
	} else {
		helpDefaultState = "disabled"
	}

	flagName := fmt.Sprintf("collector.%s", collector)
	flagHelp := fmt.Sprintf("Enable the %s collector (default: %s).", collector, helpDefaultState)
	defaultValue := fmt.Sprintf("%v", isDefaultEnabled)

	flag := kingpin.Flag(flagName, flagHelp).Default(defaultValue).Action(collectorFlagAction(collector)).Bool()
	collectorState[collector] = flag

	factories[collector] = factory
}

// RocketMQCollector implements the prometheus.Collector interface.
type RocketMQCollector struct {
	Collectors map[string]Collector
	logger     log.Logger
}

func DisableDefaultCollectors() {
	for c := range collectorState {
		if _, ok := forcedCollectors[c]; !ok {
			*collectorState[c] = false
		}
	}
}

func collectorFlagAction(collector string) func(ctx *kingpin.ParseContext) error {
	return func(ctx *kingpin.ParseContext) error {
		forcedCollectors[collector] = true
		return nil
	}
}

// NewRocketMQCollector creates a new NewRocketMQCollector.
func NewRocketMQCollector(logger log.Logger, filters ...string) (*RocketMQCollector, error) {
	f := make(map[string]bool)
	for _, filter := range filters {
		enabled, exist := collectorState[filter]
		if !exist {
			return nil, fmt.Errorf("missing collector: %s", filter)
		}
		if !*enabled {
			return nil, fmt.Errorf("disabled collector: %s", filter)
		}
		f[filter] = true
	}
	collectors := make(map[string]Collector)
	for key, enabled := range collectorState {
		if *enabled {
			collector, err := factories[key](log.With(logger, "collector", key))
			if err != nil {
				return nil, err
			}
			if len(f) == 0 || f[key] {
				collectors[key] = collector
			}
		}
	}
	return &RocketMQCollector{Collectors: collectors, logger: logger}, nil
}

// Describe implements the prometheus.Collector interface.
func (n RocketMQCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- scrapeDurationDesc
	ch <- scrapeSuccessDesc
}

// Collect implements the prometheus.Collector interface.
func (n RocketMQCollector) Collect(ch chan<- prometheus.Metric) {
	wg := sync.WaitGroup{}
	wg.Add(len(n.Collectors))
	for name, c := range n.Collectors {
		go func(name string, c Collector) {
			execute(name, c, ch, n.logger)
			wg.Done()
		}(name, c)
	}
	wg.Wait()
}

func execute(name string, c Collector, ch chan<- prometheus.Metric, logger log.Logger) {
	begin := time.Now()
	err := c.Update(ch)
	duration := time.Since(begin)
	var success float64

	if err != nil {
		if IsNoDataError(err) {
			level.Debug(logger).Log("msg", "collector returned no data", "name", name, "duration_seconds", duration.Seconds(), "err", err)
		} else {
			level.Error(logger).Log("msg", "collector failed", "name", name, "duration_seconds", duration.Seconds(), "err", err)
		}
		success = 0
	} else {
		level.Debug(logger).Log("msg", "collector succeeded", "name", name, "duration_seconds", duration.Seconds())
		success = 1
	}
	ch <- prometheus.MustNewConstMetric(scrapeDurationDesc, prometheus.GaugeValue, duration.Seconds(), name)
	ch <- prometheus.MustNewConstMetric(scrapeSuccessDesc, prometheus.GaugeValue, success, name)
}

// Collector is the interface a collector has to implement.
type Collector interface {
	// Get new metrics and expose them via prometheus registry.
	Update(ch chan<- prometheus.Metric) error
}

type typedDesc struct {
	desc      *prometheus.Desc
	valueType prometheus.ValueType
}

func (d *typedDesc) mustNewConstMetric(value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(d.desc, d.valueType, value, labels...)
}

// ErrNoData indicates the collector found no data to collect, but had no other error.
var ErrNoData = errors.New("collector returned no data")

func IsNoDataError(err error) bool {
	return err == ErrNoData
}
