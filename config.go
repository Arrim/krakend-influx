package influxdb

import (
	"errors"
	"time"

	"github.com/devopsfaith/krakend/config"
)

const defaultBufferSize = 0

type influxConfig struct {
	address   string
	bucket    string
	org       string
	token     string
	ttl       time.Duration
	batchSize uint
}

func configGetter(extraConfig config.ExtraConfig) interface{} {
	value, ok := extraConfig[Namespace]
	if !ok {
		return nil
	}

	castedConfig, ok := value.(map[string]interface{})
	if !ok {
		return nil
	}

	cfg := influxConfig{}

	if value, ok := castedConfig["address"]; ok {
		cfg.address = value.(string)
	}

	if value, ok := castedConfig["org"]; ok {
		cfg.org = value.(string)
	}

	if value, ok := castedConfig["token"]; ok {
		cfg.token = value.(string)
	}

	if value, ok := castedConfig["batch_size"]; ok {
		if s, ok := value.(float64); ok {
			cfg.batchSize = uint(s)
		}
	}

	if value, ok := castedConfig["ttl"]; ok {
		s, ok := value.(string)

		if !ok {
			return nil
		}
		var err error
		cfg.ttl, err = time.ParseDuration(s)

		if err != nil {
			return nil
		}
	}

	if value, ok := castedConfig["bucket"]; ok {
		cfg.bucket = value.(string)
	} else {
		cfg.bucket = "krakend"
	}

	return cfg
}

var errNoConfig = errors.New("influxdb: unable to load custom config")
