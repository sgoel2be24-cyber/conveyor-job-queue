package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// maxCapturedBody bounds how much of an error response is kept.
const maxCapturedBody = 1024

// WebhookRequest is the payload shape the Webhook handler expects.
//
//	{"url": "https://example.com/hook", "method": "POST", "body": {...}}
type WebhookRequest struct {
	URL     string            `json:"url"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
}

// Webhook delivers a job as an HTTP request, treating any non-2xx response as a
// failure so the job retries.
type Webhook struct {
	Client *http.Client
}

// NewWebhook returns a Webhook handler with a default client.
//
// The client has no overall timeout: cancellation is driven by the job's
// context, which the broker ties to the lease. A fixed timeout here would
// either cut short a job whose lease was renewed, or outlive a lease that had
// already been reassigned.
func NewWebhook() *Webhook {
	return &Webhook{Client: &http.Client{}}
}

// Name implements Handler.
func (h *Webhook) Name() string { return "webhook" }

// Execute implements Handler.
func (h *Webhook) Execute(ctx context.Context, j Job) error {
	var spec WebhookRequest
	if err := json.Unmarshal(j.Payload, &spec); err != nil {
		// Malformed payloads will never succeed, but they still go through the
		// normal retry path rather than being special-cased; the dead-letter
		// queue is where permanently-broken jobs are meant to end up.
		return fmt.Errorf("payload is not a webhook request: %w", err)
	}
	if spec.URL == "" {
		return fmt.Errorf("webhook payload has no url")
	}

	method := spec.Method
	if method == "" {
		method = http.MethodPost
	}
	var body io.Reader
	if len(spec.Body) > 0 {
		body = bytes.NewReader(spec.Body)
	}

	req, err := http.NewRequestWithContext(ctx, method, spec.URL, body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	if len(spec.Body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range spec.Headers {
		req.Header.Set(k, v)
	}

	// Hand the receiver what it needs to recognize a redelivery. Conveyor
	// guarantees at-least-once, so this request may genuinely arrive twice; a
	// receiver that keys off this header can make the second one a no-op. See
	// Job.DedupKey.
	req.Header.Set("Idempotency-Key", j.DedupKey())
	req.Header.Set("Conveyor-Job-Id", j.ID)
	req.Header.Set("Conveyor-Attempt", strconv.Itoa(j.Attempt))

	client := h.Client
	if client == nil {
		client = &http.Client{}
	}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, spec.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxCapturedBody))
		return fmt.Errorf("%s %s returned %s after %s: %s",
			method, spec.URL, resp.Status, time.Since(start).Round(time.Millisecond),
			truncate(string(snippet), maxCapturedBody))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	return nil
}
