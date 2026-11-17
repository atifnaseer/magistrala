// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package main contains the ejad-store main function. ejad-store forwards the
// messages of channels flagged with attributes.store_messages to the writers
// stream; see package ejad/store.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/absmach/magistrala"
	"github.com/absmach/magistrala/ejad/store"
	mglog "github.com/absmach/magistrala/logger"
	"github.com/absmach/magistrala/pkg/messaging"
	broker "github.com/absmach/magistrala/pkg/messaging/fluxmq"
	"github.com/absmach/magistrala/pkg/messaging/writers"
	"github.com/absmach/magistrala/pkg/server"
	httpserver "github.com/absmach/magistrala/pkg/server/http"
	"github.com/absmach/magistrala/pkg/uuid"
	"github.com/caarlos0/env/v11"
	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
)

const (
	svcName        = "ejad-store"
	envPrefixHTTP  = "MG_EJAD_STORE_HTTP_"
	defSvcHTTPPort = "9019"
	// subscriberID names the consumer group on the "m" stream. It is fixed so
	// a restart resumes the group instead of starting a new one.
	subscriberID = "ejad-store"
)

type config struct {
	LogLevel         string        `env:"MG_EJAD_STORE_LOG_LEVEL"          envDefault:"info"`
	BrokerURL        string        `env:"MG_EJAD_STORE_BROKER_URL"         envDefault:"amqps://ejad-store@localhost:5685/"`
	BrokerClientCert string        `env:"MG_EJAD_STORE_BROKER_CLIENT_CERT" envDefault:""`
	BrokerClientKey  string        `env:"MG_EJAD_STORE_BROKER_CLIENT_KEY"  envDefault:""`
	BrokerCACerts    string        `env:"MG_EJAD_STORE_BROKER_CA_CERTS"    envDefault:""`
	AtomURL          string        `env:"ATOM_URL"                         envDefault:"http://localhost:8080"`
	AtomToken        string        `env:"ATOM_SERVICE_TOKEN"               envDefault:""`
	AtomTimeout      time.Duration `env:"ATOM_TIMEOUT"                     envDefault:"10s"`
	RulesURL         string        `env:"MG_EJAD_STORE_RE_URL"             envDefault:""`
	RefreshInterval  time.Duration `env:"MG_EJAD_STORE_REFRESH_INTERVAL"   envDefault:"5s"`
	PublishTimeout   time.Duration `env:"MG_EJAD_STORE_PUBLISH_TIMEOUT"    envDefault:"10s"`
	InstanceID       string        `env:"MG_EJAD_STORE_INSTANCE_ID"        envDefault:""`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	cfg := config{}
	if err := env.Parse(&cfg); err != nil {
		log.Fatalf("failed to load %s service configuration : %s", svcName, err)
	}

	logger, err := mglog.New(os.Stdout, cfg.LogLevel)
	if err != nil {
		log.Fatalf("failed to init logger: %s", err.Error())
	}

	var exitCode int
	defer mglog.ExitWithError(&exitCode)

	if cfg.AtomToken == "" {
		logger.Error("ATOM_SERVICE_TOKEN is required")
		exitCode = 1
		return
	}
	if cfg.RefreshInterval <= 0 {
		logger.Error("MG_EJAD_STORE_REFRESH_INTERVAL must be positive")
		exitCode = 1
		return
	}
	if cfg.InstanceID == "" {
		if cfg.InstanceID, err = uuid.New().ID(); err != nil {
			logger.Error(fmt.Sprintf("failed to generate instanceID: %s", err))
			exitCode = 1
			return
		}
	}

	httpServerConfig := server.Config{Port: defSvcHTTPPort}
	if err := env.ParseWithOptions(&httpServerConfig, env.Options{Prefix: envPrefixHTTP}); err != nil {
		logger.Error(fmt.Sprintf("failed to load %s HTTP server configuration : %s", svcName, err))
		exitCode = 1
		return
	}

	httpClient := &http.Client{Timeout: cfg.AtomTimeout}
	var rules store.RuleChecker
	if cfg.RulesURL != "" {
		rules = store.NewRulesClient(cfg.RulesURL, cfg.AtomToken, httpClient)
	} else {
		logger.Warn("MG_EJAD_STORE_RE_URL is empty: flagged channels are forwarded even if a rule stores them")
	}
	registry := store.NewRegistry(store.NewAtomSource(cfg.AtomURL, cfg.AtomToken, httpClient), rules, logger)

	// Both connections go to the mTLS local listener as the ejad-store
	// principal: the subscriber needs the "m" stream grant, and the publisher
	// must be trusted so the device's protocol and identity reach the writers
	// instead of this service's AMQP connection.
	var brokerOpts []messaging.Option
	if cfg.BrokerClientCert != "" || cfg.BrokerClientKey != "" || cfg.BrokerCACerts != "" {
		brokerOpts = append(brokerOpts, broker.InternalMetadata(cfg.BrokerClientCert, cfg.BrokerClientKey, cfg.BrokerCACerts))
	}

	pub, err := writers.NewPublisher(ctx, cfg.BrokerURL, brokerOpts...)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to connect writers publisher: %s", err))
		exitCode = 1
		return
	}
	defer pub.Close()

	subOpts := append([]messaging.Option{broker.ConnectionName("ejad-store-sub")}, brokerOpts...)
	sub, err := broker.NewPubSub(ctx, cfg.BrokerURL, logger, subOpts...)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to connect message subscriber: %s", err))
		exitCode = 1
		return
	}
	defer sub.Close()

	// Know the flagged channels before consuming, so the first messages of a
	// stored channel are not dropped.
	if err := waitForRegistry(ctx, registry, cfg.RefreshInterval, logger); err != nil {
		exitCode = 1
		return
	}
	g.Go(func() error {
		registry.Run(ctx, cfg.RefreshInterval)
		return nil
	})

	if err := sub.Subscribe(ctx, messaging.SubscriberConfig{
		ID:             subscriberID,
		ClientID:       subscriberID,
		Topic:          "#",
		Handler:        store.NewForwarder(registry, pub, cfg.PublishTimeout),
		DeliveryPolicy: messaging.DeliverNewPolicy,
		AckPolicy:      messaging.AckExplicit,
	}); err != nil {
		logger.Error(fmt.Sprintf("failed to subscribe to the message stream: %s", err))
		exitCode = 1
		return
	}
	logger.Info("ejad-store forwarding flagged channels to the writers stream",
		slog.Int("channels", len(registry.Channels())))

	hs := httpserver.NewServer(ctx, cancel, svcName, httpServerConfig, makeHandler(cfg.InstanceID, registry), logger)

	g.Go(func() error {
		return hs.Start()
	})

	g.Go(func() error {
		return server.StopSignalHandler(ctx, cancel, logger, svcName, hs)
	})

	if err := g.Wait(); err != nil {
		logger.Error(fmt.Sprintf("%s service terminated: %s", svcName, err))
	}
}

func waitForRegistry(ctx context.Context, registry *store.Registry, interval time.Duration, logger *slog.Logger) error {
	for {
		err := registry.Refresh(ctx)
		if err == nil {
			return nil
		}
		logger.Warn("waiting for Atom to list flagged channels", slog.Any("error", err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func makeHandler(instanceID string, registry *store.Registry) http.Handler {
	r := chi.NewRouter()
	r.Get("/health", magistrala.Health(svcName, instanceID))
	r.Handle("/metrics", promhttp.Handler())
	// The ids of the channels currently forwarded, for operators.
	r.Get("/channels", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string][]string{"channels": registry.Channels()})
	})
	return r
}
