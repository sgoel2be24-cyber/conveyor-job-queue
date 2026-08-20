package broker

import "net/http"

// Protocols returns the HTTP protocol set the broker serves on.
//
// HTTP/2 matters because Lease is a long-lived server stream, and it has to
// work over a plaintext local socket -- there is no TLS on a broker listening
// on localhost. That used to require golang.org/x/net/http2/h2c, which is
// deprecated now that net/http supports unencrypted HTTP/2 directly.
//
// HTTP/1.1 stays enabled so the Connect endpoints remain reachable with plain
// curl, which is most of the reason for preferring Connect over gRPC here.
func Protocols() *http.Protocols {
	p := new(http.Protocols)
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	return p
}
