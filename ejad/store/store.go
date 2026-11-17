// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/absmach/magistrala/pkg/messaging"
)

// Registry holds the set of channels whose messages are forwarded. Refresh
// rebuilds it from the channel flags and the rules engine; Stored is safe to
// call from the message handler at any time.
type Registry struct {
	source ChannelSource
	rules  RuleChecker
	logger *slog.Logger

	mu         sync.RWMutex
	stored     map[string]bool
	ruleStored map[string]bool
}

// NewRegistry returns an empty registry. rules may be nil, which turns the
// rule safety net off.
func NewRegistry(source ChannelSource, rules RuleChecker, logger *slog.Logger) *Registry {
	return &Registry{
		source:     source,
		rules:      rules,
		logger:     logger,
		stored:     map[string]bool{},
		ruleStored: map[string]bool{},
	}
}

// Stored reports whether messages of channel are forwarded.
func (r *Registry) Stored(channel string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stored[channel]
}

// Channels returns the forwarded channel ids, sorted.
func (r *Registry) Channels() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return sortedKeys(r.stored)
}

// Refresh reloads the flagged channels. If Atom cannot be read the previous
// set is kept. If the rules engine cannot be read for a workspace, a channel
// keeps the rule state it had last time, and a channel never checked before is
// forwarded: a duplicate write is rejected by the writer's primary key, while
// a skipped message would be lost.
func (r *Registry) Refresh(ctx context.Context) error {
	channels, err := r.source.FlaggedChannels(ctx)
	if err != nil {
		return fmt.Errorf("list flagged channels: %w", err)
	}

	byWorkspace := map[string][]string{}
	for _, ch := range channels {
		byWorkspace[ch.Workspace] = append(byWorkspace[ch.Workspace], ch.ID)
	}

	r.mu.RLock()
	prevRule := r.ruleStored
	r.mu.RUnlock()

	stored := map[string]bool{}
	ruleStored := map[string]bool{}
	for workspace, ids := range byWorkspace {
		if r.rules == nil {
			for _, id := range ids {
				stored[id] = true
			}
			continue
		}
		byRule, err := r.rules.RuleStoredChannels(ctx, workspace)
		if err != nil {
			r.logger.Warn("could not check store rules; keeping last known state",
				slog.String("workspace", workspace), slog.Any("error", err))
			byRule = prevRule
		}
		for _, id := range ids {
			if byRule[id] {
				ruleStored[id] = true
				continue
			}
			stored[id] = true
		}
	}

	r.mu.Lock()
	prevStored, prevRuleStored := r.stored, r.ruleStored
	r.stored, r.ruleStored = stored, ruleStored
	r.mu.Unlock()

	for _, id := range sortedKeys(stored) {
		if !prevStored[id] {
			r.logger.Info("storing channel messages", slog.String("channel", id))
		}
	}
	for _, id := range sortedKeys(prevStored) {
		if !stored[id] && !ruleStored[id] {
			r.logger.Info("no longer storing channel messages", slog.String("channel", id))
		}
	}
	for _, id := range sortedKeys(ruleStored) {
		if !prevRuleStored[id] {
			r.logger.Info("skipping flagged channel: a rules engine rule already stores it", slog.String("channel", id))
		}
	}
	return nil
}

// Run refreshes the registry every interval until ctx is done.
func (r *Registry) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.Refresh(ctx); err != nil && ctx.Err() == nil {
				r.logger.Warn("refresh failed; keeping current channels", slog.Any("error", err))
			}
		}
	}
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k, v := range m {
		if v {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// Forwarder is the message handler: it republishes messages of stored
// channels to the writers stream and drops the rest.
type Forwarder struct {
	registry *Registry
	pub      messaging.Publisher
	timeout  time.Duration
}

var _ messaging.MessageHandler = (*Forwarder)(nil)

// NewForwarder returns a handler that publishes through pub, a publisher on
// the "writers" prefix.
func NewForwarder(registry *Registry, pub messaging.Publisher, timeout time.Duration) *Forwarder {
	return &Forwarder{registry: registry, pub: pub, timeout: timeout}
}

// Handle forwards msg unchanged (publisher, protocol, created, device and
// metadata included) when its channel is stored. A failed publish is nacked so
// the message is delivered again.
func (f *Forwarder) Handle(msg *messaging.Message) error {
	if msg == nil || !f.registry.Stored(msg.GetChannel()) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), f.timeout)
	defer cancel()
	if err := f.pub.Publish(ctx, messaging.EncodeMessageTopic(msg), msg); err != nil {
		return messaging.NewError(fmt.Errorf("forward to writers: %w", err), messaging.Nack)
	}
	return nil
}

// Cancel implements messaging.MessageHandler.
func (f *Forwarder) Cancel() error {
	return nil
}
