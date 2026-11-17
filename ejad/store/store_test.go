// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/absmach/magistrala/ejad/store"
	"github.com/absmach/magistrala/pkg/messaging"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type fakeSource struct {
	channels []store.Channel
	err      error
}

func (f *fakeSource) FlaggedChannels(context.Context) ([]store.Channel, error) {
	return f.channels, f.err
}

type fakeRules struct {
	stored map[string]map[string]bool
	err    error
}

func (f *fakeRules) RuleStoredChannels(_ context.Context, ws string) (map[string]bool, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.stored[ws], nil
}

type published struct {
	topic string
	msg   *messaging.Message
}

type fakePub struct {
	sent []published
	err  error
}

func (f *fakePub) Publish(_ context.Context, topic string, msg *messaging.Message) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, published{topic, msg})
	return nil
}

func (f *fakePub) Close() error { return nil }

func TestRefreshSkipsRuleStoredChannels(t *testing.T) {
	src := &fakeSource{channels: []store.Channel{
		{ID: "a", Workspace: "w1"},
		{ID: "b", Workspace: "w1"},
		{ID: "c", Workspace: "w2"},
	}}
	rules := &fakeRules{stored: map[string]map[string]bool{"w1": {"b": true}}}
	reg := store.NewRegistry(src, rules, discard)
	if err := reg.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := reg.Channels(); !slices.Equal(got, []string{"a", "c"}) {
		t.Fatalf("channels = %v, want [a c]", got)
	}
}

func TestRefreshWithoutRuleChecker(t *testing.T) {
	src := &fakeSource{channels: []store.Channel{{ID: "a", Workspace: "w1"}}}
	reg := store.NewRegistry(src, nil, discard)
	if err := reg.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reg.Stored("a") {
		t.Fatal("a should be stored")
	}
}

func TestRefreshKeepsStateWhenAtomFails(t *testing.T) {
	src := &fakeSource{channels: []store.Channel{{ID: "a", Workspace: "w1"}}}
	reg := store.NewRegistry(src, nil, discard)
	if err := reg.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	src.err = errors.New("down")
	if err := reg.Refresh(context.Background()); err == nil {
		t.Fatal("want error")
	}
	if !reg.Stored("a") {
		t.Fatal("a should still be stored")
	}
}

func TestRefreshKeepsRuleStateWhenRulesFail(t *testing.T) {
	src := &fakeSource{channels: []store.Channel{{ID: "a", Workspace: "w1"}, {ID: "b", Workspace: "w1"}}}
	rules := &fakeRules{stored: map[string]map[string]bool{"w1": {"a": true}}}
	reg := store.NewRegistry(src, rules, discard)
	if err := reg.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	rules.err = errors.New("down")
	src.channels = append(src.channels, store.Channel{ID: "c", Workspace: "w1"})
	if err := reg.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	// a stays skipped (known rule), b stays stored, c is new and forwarded.
	if got := reg.Channels(); !slices.Equal(got, []string{"b", "c"}) {
		t.Fatalf("channels = %v, want [b c]", got)
	}
}

func TestRefreshDropsUnflaggedChannel(t *testing.T) {
	src := &fakeSource{channels: []store.Channel{{ID: "a", Workspace: "w1"}}}
	reg := store.NewRegistry(src, nil, discard)
	_ = reg.Refresh(context.Background())
	src.channels = nil
	_ = reg.Refresh(context.Background())
	if reg.Stored("a") {
		t.Fatal("a should no longer be stored")
	}
}

func TestForwarder(t *testing.T) {
	src := &fakeSource{channels: []store.Channel{{ID: "ch", Workspace: "ws"}}}
	reg := store.NewRegistry(src, nil, discard)
	_ = reg.Refresh(context.Background())
	pub := &fakePub{}
	fwd := store.NewForwarder(reg, pub, time.Second)

	stored := &messaging.Message{Workspace: "ws", Channel: "ch", Subtopic: "temp/x", Protocol: "http", Publisher: "dev"}
	if err := fwd.Handle(stored); err != nil {
		t.Fatal(err)
	}
	if err := fwd.Handle(&messaging.Message{Workspace: "ws", Channel: "other"}); err != nil {
		t.Fatal(err)
	}
	if len(pub.sent) != 1 {
		t.Fatalf("published %d messages, want 1", len(pub.sent))
	}
	if pub.sent[0].topic != "ws/c/ch/temp/x" || pub.sent[0].msg != stored {
		t.Fatalf("published %q %+v", pub.sent[0].topic, pub.sent[0].msg)
	}

	pub.err = errors.New("broker down")
	err := fwd.Handle(stored)
	var msgErr messaging.Error
	if !errors.As(err, &msgErr) || msgErr.Ack() != messaging.Nack {
		t.Fatalf("err = %v, want a Nack messaging.Error", err)
	}
}

func TestAtomSource(t *testing.T) {
	var gotAuth string
	var gotVars map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		var body struct {
			Variables map[string]any `json:"variables"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotVars = body.Variables
		_, _ = w.Write([]byte(`{"data":{"resources":{"total":2,"items":[{"id":"c1","tenantId":"w1"},{"id":"c2","tenantId":null}]}}}`))
	}))
	defer srv.Close()

	src := store.NewAtomSource(srv.URL+"/", "tok", srv.Client())
	got, err := src.FlaggedChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []store.Channel{{ID: "c1", Workspace: "w1"}}) {
		t.Fatalf("channels = %+v", got)
	}
	if gotAuth != "Bearer tok" {
		t.Fatalf("auth = %q", gotAuth)
	}
	attrs, _ := gotVars["attributes"].(map[string]any)
	if attrs[store.StoreAttribute] != true {
		t.Fatalf("attributes filter = %v", gotVars["attributes"])
	}
}

func TestAtomSourceGraphQLError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":null,"errors":[{"message":"forbidden"}]}`))
	}))
	defer srv.Close()
	if _, err := store.NewAtomSource(srv.URL, "t", srv.Client()).FlaggedChannels(context.Background()); err == nil {
		t.Fatal("want error")
	}
}

func TestRulesClient(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"total":4,"rules":[
			{"input_channel":"a","status":"enabled","outputs":[{"type":"save_senml"}]},
			{"input_channel":"b","status":"disabled","outputs":[{"type":"save_senml"}]},
			{"input_channel":"c","status":"enabled","outputs":[{"type":"alarms"}]},
			{"input_channel":"d","status":"enabled","outputs":[{"type":"email"},{"type":"save_senml"}]}
		]}`))
	}))
	defer srv.Close()

	got, err := store.NewRulesClient(srv.URL, "t", srv.Client()).RuleStoredChannels(context.Background(), "w1")
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/w1/rules" {
		t.Fatalf("path = %q", gotPath)
	}
	if len(got) != 2 || !got["a"] || !got["d"] {
		t.Fatalf("rule-stored = %v, want a and d", got)
	}
}

func TestRulesClientHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"message":"unauthorized"}`, http.StatusUnauthorized)
	}))
	defer srv.Close()
	if _, err := store.NewRulesClient(srv.URL, "t", srv.Client()).RuleStoredChannels(context.Background(), "w1"); err == nil {
		t.Fatal("want error")
	}
}
