// Package gate implements the server's read-only AgentOps Gate approval client.
package gate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Aly700/capstan/internal/engine"
)

type Client struct {
	baseURL   string
	apiKey    string
	http      *http.Client
	configErr error
}

var _ engine.GateClient = (*Client)(nil)

// New uses the supplied transport without mutating the caller's HTTP client.
// Redirects are rejected so X-API-Key is never sent to a redirected destination.
func New(baseURL, apiKey string, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{}
	}
	client := *hc
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c := &Client{baseURL: strings.TrimRight(baseURL, "/"), apiKey: apiKey, http: &client}
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		c.configErr = errors.New("gate: invalid base URL")
	}
	return c
}

func (c *Client) ApprovalStatus(ctx context.Context, id string) (engine.GateApproval, error) {
	if c.configErr != nil {
		return engine.GateApproval{}, c.configErr
	}
	if id == "" {
		return engine.GateApproval{}, errors.New("gate: empty approval id")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/approvals/"+url.PathEscape(id), nil)
	if err != nil {
		return engine.GateApproval{}, errors.New("gate: invalid approval URL")
	}
	req.Header.Set("X-API-Key", c.apiKey)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return engine.GateApproval{}, fmt.Errorf("gate: approval request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return engine.GateApproval{}, fmt.Errorf("gate: approval HTTP status %d", resp.StatusCode)
	}
	const maxResponseBytes = 1 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return engine.GateApproval{}, fmt.Errorf("gate: read approval: %w", err)
	}
	if len(body) > maxResponseBytes {
		return engine.GateApproval{}, errors.New("gate: approval response too large")
	}
	// The other ApprovalResponse fields (id, decisionId, expiresAt, createdAt) are
	// intentionally unused. Pending decisions may have null decidedBy/decidedAt.
	var wire struct {
		Status    string    `json:"status"`
		DecidedBy string    `json:"decidedBy"`
		DecidedAt time.Time `json:"decidedAt"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return engine.GateApproval{}, errors.New("gate: invalid approval JSON")
	}
	switch wire.Status {
	case "PENDING", "APPROVED", "DENIED", "EXPIRED":
	default:
		return engine.GateApproval{}, errors.New("gate: invalid approval status")
	}
	return engine.GateApproval{Status: wire.Status, DecidedBy: wire.DecidedBy, DecidedAt: wire.DecidedAt}, nil
}
