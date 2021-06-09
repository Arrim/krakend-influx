package influxdb

import (
	"context"
	"os"
	"time"

	"github.com/arrim/krakend-influx/counter"
	"github.com/arrim/krakend-influx/gauge"
	"github.com/arrim/krakend-influx/histogram"
	ginmetrics "github.com/devopsfaith/krakend-metrics/gin"
	"github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/luraproject/lura/config"
	"github.com/luraproject/lura/logging"
)

const Namespace = "github_com/letgoapp/krakend-influx"

type clientWrapper struct {
	influxClient influxdb2.Client
	writerApi    api.WriteAPI
	collector    *ginmetrics.Metrics
	logger       logging.Logger
}

func New(ctx context.Context, extraConfig config.ExtraConfig, metricsCollector *ginmetrics.Metrics, logger logging.Logger) error {
	logger.Debug("creating a new influxdb client")
	cfg, ok := configGetter(extraConfig).(influxConfig)
	if !ok {
		logger.Debug("no config for the influxdb client. Aborting")
		return errNoConfig
	}

	influxdbClient := influxdb2.NewClientWithOptions(
		cfg.address,
		cfg.token,
		influxdb2.DefaultOptions().
			SetBatchSize(cfg.batchSize),
	)
	writeAPI := influxdbClient.WriteAPI(cfg.org, cfg.bucket)

	errorsCh := writeAPI.Errors()
	go func() {
		for err := range errorsCh {
			logger.Error("writing to influx:", err.Error())
		}
	}()

	t := time.NewTicker(cfg.ttl)

	cw := clientWrapper{
		influxClient: influxdbClient,
		writerApi:    writeAPI,
		collector:    metricsCollector,
		logger:       logger,
	}

	go cw.keepUpdated(ctx, t.C)

	logger.Debug("influxdb client up and running")

	return nil
}

func (cw clientWrapper) keepUpdated(ctx context.Context, ticker <-chan time.Time) {
	hostname, err := os.Hostname()
	if err != nil {
		cw.logger.Error("influxdb resolving the local hostname:", err.Error())
	}
	for {
		select {
		case <-ticker:
		case <-ctx.Done():
			return
		}

		cw.logger.Debug("Preparing influxdb points")

		snapshot := cw.collector.Snapshot()

		if shouldSendPoints := len(snapshot.Counters) > 0 || len(snapshot.Gauges) > 0; !shouldSendPoints {
			cw.logger.Debug("no metrics to send to influx")
			continue
		}

		now := time.Unix(0, snapshot.Time)

		for _, p := range counter.Points(hostname, now, snapshot.Counters, cw.logger) {
			cw.writerApi.WritePoint(p)
		}

		for _, p := range gauge.Points(hostname, now, snapshot.Gauges, cw.logger) {
			cw.writerApi.WritePoint(p)
		}

		for _, p := range histogram.Points(hostname, now, snapshot.Histograms, cw.logger) {
			cw.writerApi.WritePoint(p)
		}

		cw.writerApi.Flush()
	}
}
