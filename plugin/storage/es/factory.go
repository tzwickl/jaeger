// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package es

import (
	"flag"

	"github.com/spf13/viper"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/pkg/es"
	"github.com/jaegertracing/jaeger/pkg/es/config"
	esDepStore "github.com/jaegertracing/jaeger/plugin/storage/es/dependencystore"
	esSpanStore "github.com/jaegertracing/jaeger/plugin/storage/es/spanstore"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
)

var kafkaBrokers = []string {"localhost:9092"}

// Factory implements storage.Factory for Elasticsearch backend.
type Factory struct {
	Options *Options

	metricsFactory metrics.Factory
	logger         *zap.Logger

	primaryConfig config.ClientBuilder
	primaryClient es.Client

	kafkaProducer  sarama.AsyncProducer
}

// NewFactory creates a new Factory.
func NewFactory() *Factory {
	return &Factory{
		Options: NewOptions("es"), // TODO add "es-archive" once supported
	}
}

// AddFlags implements plugin.Configurable
func (f *Factory) AddFlags(flagSet *flag.FlagSet) {
	f.Options.AddFlags(flagSet)
}

// InitFromViper implements plugin.Configurable
func (f *Factory) InitFromViper(v *viper.Viper) {
	f.Options.InitFromViper(v)
	f.primaryConfig = f.Options.GetPrimary()
}

// Initialize implements storage.Factory
func (f *Factory) Initialize(metricsFactory metrics.Factory, logger *zap.Logger) error {
	f.metricsFactory, f.logger = metricsFactory, logger

	primaryClient, err := f.primaryConfig.NewClient(logger, metricsFactory)
	if err != nil {
		return err
	}
	f.primaryClient = primaryClient
	// TODO init archive (cf. https://github.com/jaegertracing/jaeger/pull/604)

	f.kafkaProducer, err = createKafkaProducer(kafkaBrokers, f)

	if err != nil {
		panic(err)
	}

	return nil
}

func createKafkaProducer (kafkaConn []string, f *Factory) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionNone
	var err error
	producer, err := sarama.NewAsyncProducer(kafkaConn, config)
	if err != nil {
		return nil, err
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Kill)
	go func() {
		<-c
		if err := producer.Close(); err != nil {
			f.logger.Fatal("Error closing async producer", zap.String("Error", err.Error()))
		}
		f.logger.Info("Async Producer closed")
	}()
	go func() {
		for err := range producer.Errors() {
			f.logger.Fatal("Failed to write message to topic:", zap.String("Error", err.Err.Error()))
		}
	}()

	return producer, nil
}

// CreateSpanReader implements storage.Factory
func (f *Factory) CreateSpanReader() (spanstore.Reader, error) {
	cfg := f.primaryConfig
	return esSpanStore.NewSpanReader(f.primaryClient, f.logger, cfg.GetMaxSpanAge(), f.metricsFactory), nil
}

// CreateSpanWriter implements storage.Factory
func (f *Factory) CreateSpanWriter() (spanstore.Writer, error) {
	cfg := f.primaryConfig
	return esSpanStore.NewSpanWriter(f.kafkaProducer, f.primaryClient, f.logger, f.metricsFactory, cfg.GetNumShards(), cfg.GetNumReplicas()), nil
}

// CreateDependencyReader implements storage.Factory
func (f *Factory) CreateDependencyReader() (dependencystore.Reader, error) {
	return esDepStore.NewDependencyStore(f.primaryClient, f.logger), nil
}
