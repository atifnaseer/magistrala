// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	saveSenML   = "save_senml"
	ruleEnabled = "enabled"
	rulesPage   = 100
)

// RuleChecker reports which channels of a workspace are already stored by a
// rules engine rule.
type RuleChecker interface {
	RuleStoredChannels(ctx context.Context, workspace string) (map[string]bool, error)
}

// RulesClient asks the rules engine's HTTP API.
type RulesClient struct {
	url    string
	token  string
	client *http.Client
}

// NewRulesClient returns a RuleChecker backed by the rules engine at baseURL.
func NewRulesClient(baseURL, token string, client *http.Client) *RulesClient {
	return &RulesClient{
		url:    strings.TrimRight(baseURL, "/"),
		token:  token,
		client: client,
	}
}

type rule struct {
	InputChannel string `json:"input_channel"`
	Status       string `json:"status"`
	Outputs      []struct {
		Type string `json:"type"`
	} `json:"outputs"`
}

type rulesResponse struct {
	Total int    `json:"total"`
	Rules []rule `json:"rules"`
}

// storesMessages is true for an enabled rule that saves SenML, whatever its
// topic filter or logic: such a rule writes (some of) the channel's messages,
// so forwarding them as well would write them twice.
func (r rule) storesMessages() bool {
	if r.Status != ruleEnabled || r.InputChannel == "" {
		return false
	}
	for _, out := range r.Outputs {
		if out.Type == saveSenML {
			return true
		}
	}
	return false
}

// RuleStoredChannels returns the channels in workspace that an enabled
// save_senml rule already stores.
func (c *RulesClient) RuleStoredChannels(ctx context.Context, workspace string) (map[string]bool, error) {
	stored := map[string]bool{}
	for offset := 0; ; offset += rulesPage {
		page, err := c.page(ctx, workspace, offset)
		if err != nil {
			return nil, err
		}
		for _, r := range page.Rules {
			if r.storesMessages() {
				stored[r.InputChannel] = true
			}
		}
		if len(page.Rules) < rulesPage || offset+len(page.Rules) >= page.Total {
			return stored, nil
		}
	}
}

func (c *RulesClient) page(ctx context.Context, workspace string, offset int) (rulesResponse, error) {
	query := url.Values{
		"status": {"all"},
		"offset": {strconv.Itoa(offset)},
		"limit":  {strconv.Itoa(rulesPage)},
	}
	endpoint := fmt.Sprintf("%s/%s/rules?%s", c.url, url.PathEscape(workspace), query.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return rulesResponse{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)

	res, err := c.client.Do(req)
	if err != nil {
		return rulesResponse{}, fmt.Errorf("rules engine: %w", err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return rulesResponse{}, fmt.Errorf("rules engine: %w", err)
	}
	if res.StatusCode != http.StatusOK {
		return rulesResponse{}, fmt.Errorf("rules engine answered %d: %s", res.StatusCode, snippet(raw))
	}
	var page rulesResponse
	if err := json.Unmarshal(raw, &page); err != nil {
		return rulesResponse{}, fmt.Errorf("rules engine: decode response: %w", err)
	}
	return page, nil
}
