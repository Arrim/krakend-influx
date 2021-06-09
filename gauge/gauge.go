package gauge

import (
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/luraproject/lura/logging"
)

func Points(hostname string, now time.Time, counters map[string]int64, logger logging.Logger) []*write.Point {
	res := make([]*write.Point, 4)

	in := map[string]interface{}{
		"gauge": int(counters["krakend.router.connected-gauge"]),
	}
	incoming := influxdb2.NewPoint("router", map[string]string{"host": hostname, "direction": "in"}, in, now)
	res[0] = incoming

	out := map[string]interface{}{
		"gauge": int(counters["krakend.router.disconnected-gauge"]),
	}
	outgoing := influxdb2.NewPoint("router", map[string]string{"host": hostname, "direction": "out"}, out, now)
	res[1] = outgoing

	debug := map[string]interface{}{}
	runtime := map[string]interface{}{}

	for k, v := range counters {
		if k == "krakend.router.connected-gauge" || k == "krakend.router.disconnected-gauge" {
			continue
		}
		if k[:22] == "krakend.service.debug." {
			debug[k[22:]] = int(v)
			continue
		}
		if k[:24] == "krakend.service.runtime." {
			runtime[k[24:]] = int(v)
			continue
		}
		logger.Debug("unknown gauge key:", k)
	}

	debugPoint := influxdb2.NewPoint("debug", map[string]string{"host": hostname}, debug, now)
	res[2] = debugPoint

	runtimePoint := influxdb2.NewPoint("runtime", map[string]string{"host": hostname}, runtime, now)
	res[3] = runtimePoint

	return res
}
