// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package store forwards the messages of chosen channels to the writers
// stream, so the message writers save them.
//
// It replaces the per-channel "store" rule of the Ejad IoT Platform UI: the
// rules engine edition in use allows a single rule, which caps storage at one
// channel. Instead, a channel is stored when its Atom resource carries
// attributes.store_messages = true. The forwarder consumes the "m" message
// stream as a local FluxMQ principal and republishes every message of a
// flagged channel to "writers/<workspace>/c/<channel>[/<subtopic>]", the same
// stream the rules engine's save_senml output feeds. The writers' own topic
// filters and transformers decide what is saved, exactly as for a rule.
//
// A flagged channel that still has an enabled rule with a save_senml output is
// skipped, so a channel is never written by both paths.
package store
