package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestShellRunsCommand(t *testing.T) {
	h := NewShell()
	if err := h.Execute(context.Background(), Job{ID: "j1", Payload: []byte("exit 0")}); err != nil {
		t.Errorf("successful command reported an error: %v", err)
	}
}

func TestShellNonZeroExitIsFailure(t *testing.T) {
	h := NewShell()
	err := h.Execute(context.Background(), Job{ID: "j1", Payload: []byte("echo 'it broke' >&2; exit 3")})
	if err == nil {
		t.Fatal("a command exiting 3 was reported as success")
	}
	// The captured output is what an operator reads in `dlq list`.
	if !strings.Contains(err.Error(), "it broke") {
		t.Errorf("error %q does not include the command's output", err)
	}
}

func TestShellExposesJobMetadata(t *testing.T) {
	h := NewShell()
	err := h.Execute(context.Background(), Job{
		ID:             "job-123",
		Queue:          "emails",
		Attempt:        2,
		IdempotencyKey: "order-42",
		Payload: []byte(`test "$CONVEYOR_JOB_ID" = job-123 &&
		                 test "$CONVEYOR_QUEUE" = emails &&
		                 test "$CONVEYOR_ATTEMPT" = 2 &&
		                 test "$CONVEYOR_DEDUP_KEY" = order-42`),
	})
	if err != nil {
		t.Errorf("job metadata was not passed through to the command: %v", err)
	}
}

// TestShellCancellationStopsCommand covers the best-effort abort a worker
// performs when its lease expires.
func TestShellCancellationStopsCommand(t *testing.T) {
	h := NewShell()
	h.GracefulTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := h.Execute(ctx, Job{ID: "j1", Payload: []byte("sleep 30")})
	elapsed := time.Since(start)

	if err == nil {
		t.Error("a cancelled command reported success")
	}
	if elapsed > 5*time.Second {
		t.Errorf("cancellation took %s; the command was not actually stopped", elapsed)
	}
}

func TestWebhookPostsAndSucceedsOn2xx(t *testing.T) {
	var (
		mu         sync.Mutex
		gotKey     string
		gotJob     string
		gotBody    string
		gotAttempt string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		mu.Lock()
		gotKey = r.Header.Get("Idempotency-Key")
		gotJob = r.Header.Get("Conveyor-Job-Id")
		gotAttempt = r.Header.Get("Conveyor-Attempt")
		gotBody = string(buf)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	payload, _ := json.Marshal(WebhookRequest{
		URL:  srv.URL,
		Body: json.RawMessage(`{"hello":"world"}`),
	})

	h := NewWebhook()
	if err := h.Execute(context.Background(), Job{
		ID:             "job-123",
		IdempotencyKey: "order-42",
		Attempt:        3,
		Payload:        payload,
	}); err != nil {
		t.Fatalf("webhook returning 202 reported an error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	// Forwarding a dedup key is what lets a receiver collapse the duplicate
	// deliveries that at-least-once makes possible.
	if gotKey != "order-42" {
		t.Errorf("Idempotency-Key = %q, want order-42", gotKey)
	}
	if gotJob != "job-123" {
		t.Errorf("Conveyor-Job-Id = %q, want job-123", gotJob)
	}
	if gotAttempt != "3" {
		t.Errorf("Conveyor-Attempt = %q, want 3", gotAttempt)
	}
	if !strings.Contains(gotBody, "hello") {
		t.Errorf("body = %q, want the submitted JSON", gotBody)
	}
}

func TestWebhookDedupKeyFallsBackToJobID(t *testing.T) {
	var mu sync.Mutex
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotKey = r.Header.Get("Idempotency-Key")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	payload, _ := json.Marshal(WebhookRequest{URL: srv.URL})
	if err := NewWebhook().Execute(context.Background(), Job{ID: "job-abc", Payload: payload}); err != nil {
		t.Fatalf("execute: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if gotKey != "job-abc" {
		t.Errorf("Idempotency-Key = %q, want the job ID when no key was supplied", gotKey)
	}
}

func TestWebhookNon2xxIsFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("upstream exploded"))
	}))
	defer srv.Close()

	payload, _ := json.Marshal(WebhookRequest{URL: srv.URL})
	err := NewWebhook().Execute(context.Background(), Job{ID: "j1", Payload: payload})
	if err == nil {
		t.Fatal("a 500 response was reported as success")
	}
	if !strings.Contains(err.Error(), "upstream exploded") {
		t.Errorf("error %q does not include the response body", err)
	}
}

func TestWebhookRejectsBadPayload(t *testing.T) {
	h := NewWebhook()
	if err := h.Execute(context.Background(), Job{ID: "j1", Payload: []byte("not json")}); err == nil {
		t.Error("a non-JSON payload was accepted")
	}
	payload, _ := json.Marshal(WebhookRequest{})
	if err := h.Execute(context.Background(), Job{ID: "j1", Payload: payload}); err == nil {
		t.Error("a payload with no URL was accepted")
	}
}

func TestDefaultRegistry(t *testing.T) {
	reg := Default()
	for _, name := range []string{"shell", "webhook"} {
		h, ok := reg.Get(name)
		if !ok {
			t.Errorf("default registry has no %q handler", name)
			continue
		}
		if h.Name() != name {
			t.Errorf("handler registered as %q reports its name as %q", name, h.Name())
		}
	}
	if _, ok := reg.Get("nope"); ok {
		t.Error("registry returned a handler for an unregistered name")
	}
	if len(reg.Names()) != 2 {
		t.Errorf("Names() = %v, want two entries", reg.Names())
	}
}

func TestDedupKey(t *testing.T) {
	if got := (Job{ID: "id", IdempotencyKey: "key"}).DedupKey(); got != "key" {
		t.Errorf("DedupKey = %q, want the idempotency key", got)
	}
	if got := (Job{ID: "id"}).DedupKey(); got != "id" {
		t.Errorf("DedupKey = %q, want the job ID as fallback", got)
	}
}
