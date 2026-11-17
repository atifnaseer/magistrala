// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// StoreAttribute is the channel attribute that turns storing on.
const StoreAttribute = "store_messages"

const (
	channelKind = "channel"
	atomPage    = 100
)

// Channel is a channel whose messages are to be stored.
type Channel struct {
	ID        string
	Workspace string
}

// ChannelSource lists the channels flagged for storage.
type ChannelSource interface {
	FlaggedChannels(ctx context.Context) ([]Channel, error)
}

// AtomSource reads flagged channels from Atom's GraphQL API with a service
// token. The token's platform-wide read grant lets one query cover every
// workspace.
type AtomSource struct {
	url    string
	token  string
	client *http.Client
}

// NewAtomSource returns a ChannelSource backed by Atom at baseURL.
func NewAtomSource(baseURL, token string, client *http.Client) *AtomSource {
	return &AtomSource{
		url:    strings.TrimRight(baseURL, "/") + "/graphql",
		token:  token,
		client: client,
	}
}

const flaggedChannelsQuery = `query EjadStoreChannels($attributes: JSON!, $limit: Int!, $offset: Int!) {
  resources(kind: "` + channelKind + `", attributesContains: $attributes, limit: $limit, offset: $offset) {
    total
    items { id tenantId }
  }
}`

type graphQLError struct {
	Message string `json:"message"`
}

type resourcesPage struct {
	Data *struct {
		Resources struct {
			Total int `json:"total"`
			Items []struct {
				ID       string  `json:"id"`
				TenantID *string `json:"tenantId"`
			} `json:"items"`
		} `json:"resources"`
	} `json:"data"`
	Errors []graphQLError `json:"errors"`
}

// FlaggedChannels returns every live channel with store_messages = true.
func (a *AtomSource) FlaggedChannels(ctx context.Context) ([]Channel, error) {
	var channels []Channel
	for offset := 0; ; offset += atomPage {
		page, err := a.page(ctx, offset)
		if err != nil {
			return nil, err
		}
		items := page.Data.Resources.Items
		for _, item := range items {
			if item.TenantID == nil || *item.TenantID == "" {
				continue
			}
			channels = append(channels, Channel{ID: item.ID, Workspace: *item.TenantID})
		}
		if len(items) < atomPage || offset+len(items) >= page.Data.Resources.Total {
			return channels, nil
		}
	}
}

func (a *AtomSource) page(ctx context.Context, offset int) (resourcesPage, error) {
	body, err := json.Marshal(map[string]any{
		"query": flaggedChannelsQuery,
		"variables": map[string]any{
			"attributes": map[string]any{StoreAttribute: true},
			"limit":      atomPage,
			"offset":     offset,
		},
	})
	if err != nil {
		return resourcesPage{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url, bytes.NewReader(body))
	if err != nil {
		return resourcesPage{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+a.token)

	res, err := a.client.Do(req)
	if err != nil {
		return resourcesPage{}, fmt.Errorf("atom: %w", err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return resourcesPage{}, fmt.Errorf("atom: %w", err)
	}
	if res.StatusCode != http.StatusOK {
		return resourcesPage{}, fmt.Errorf("atom answered %d: %s", res.StatusCode, snippet(raw))
	}
	var page resourcesPage
	if err := json.Unmarshal(raw, &page); err != nil {
		return resourcesPage{}, fmt.Errorf("atom: decode response: %w", err)
	}
	if len(page.Errors) > 0 {
		return resourcesPage{}, fmt.Errorf("atom: %s", page.Errors[0].Message)
	}
	if page.Data == nil {
		return resourcesPage{}, fmt.Errorf("atom: empty response")
	}
	return page, nil
}

func snippet(raw []byte) string {
	s := strings.TrimSpace(string(raw))
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}
