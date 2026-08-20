package main

import (
	"net/http"
	"strings"
	"sync"

	"github.com/sgoel2be24-cyber/conveyor-job-queue/internal/genproto/conveyor/v1/conveyorv1connect"
)

// sharedHTTPClient is built once and reused, so every call from this process
// goes through the same connection pool.
//
// It speaks unencrypted HTTP/2 with prior knowledge, which the broker serves.
// That matters for more than the streaming Lease RPC: HTTP/2 multiplexes every
// concurrent request onto a single connection, whereas HTTP/1.1 needs one
// connection per in-flight request and Go's default pool keeps only two idle.
// Under concurrent load that degenerates into a TCP handshake per request --
// which, when `bench` was first run against this CLI, cost more than everything
// the broker itself was doing.
var sharedHTTPClient = sync.OnceValue(func() *http.Client {
	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	return &http.Client{Transport: transport}
})

// newClient dials a broker. addr may be given bare ("localhost:7777") or as a
// full URL.
func newClient(addr string) conveyorv1connect.BrokerServiceClient {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	return conveyorv1connect.NewBrokerServiceClient(sharedHTTPClient(), addr)
}
