// Package testutil hosts in-process test infrastructure.
package testutil

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// RunNATS starts an embedded JetStream server for one test.
func RunNATS(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	opts := &server.Options{Port: -1, JetStream: true, StoreDir: t.TempDir()}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready")
	}
	t.Cleanup(srv.Shutdown)
	// ReadyForConnections is necessary but not sufficient under load: with
	// dozens of embedded servers starting across parallel packages under
	// -race, the first dial still loses the handshake to an i/o timeout
	// often enough to redden a run (~2/30 observed). Retrying the connect
	// turns that scheduling artifact into a short wait instead of a test
	// failure; the timeout below still bounds a genuinely dead server.
	nc, err := nats.Connect(srv.ClientURL(),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(10),
		nats.ReconnectWait(100*time.Millisecond),
		nats.Timeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return nc, js
}
