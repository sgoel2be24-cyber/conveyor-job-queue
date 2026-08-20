package main

import (
	"net/http"
	"strings"

	"github.com/sgoel2be24-cyber/conveyor-job-queue/internal/genproto/conveyor/v1/conveyorv1connect"
)

// newClient dials a broker. addr may be given bare ("localhost:7777") or as a
// full URL.
func newClient(addr string) conveyorv1connect.BrokerServiceClient {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	return conveyorv1connect.NewBrokerServiceClient(http.DefaultClient, addr)
}
