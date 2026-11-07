// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

//go:build !ts_omit_serve

package ipnlocal

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"tailscale.com/ipn"
)

func TestServeProxyResponseBeforeFinalRequestEOF(t *testing.T) {
	requestBody := strings.Repeat("request", 4096)
	responseBody := strings.Repeat("response", 16384)
	backend := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		_, err := io.CopyN(io.Discard, req.Body, int64(len(requestBody)))
		if err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}
		rw.Header().Set("Content-Length", strconv.Itoa(len(responseBody)))
		_, _ = io.WriteString(rw, responseBody)
	}))
	t.Cleanup(backend.Close)

	b := newTestBackend(t)
	conf := &ipn.ServeConfig{Web: map[ipn.HostPort]*ipn.WebServerConfig{
		"example.ts.net:443": {Handlers: map[string]*ipn.HTTPHandler{"/": {Proxy: backend.URL}}},
	}}
	if err := b.SetServeConfig(conf, ""); err != nil {
		t.Fatal(err)
	}

	eofResult := make(chan error, 1)
	frontend := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		responseStarted := make(chan struct{})
		eofChecked := make(chan struct{})
		req.Body = &serveFinalEOFBody{
			ReadCloser:      req.Body,
			remaining:       req.ContentLength,
			ctx:             req.Context(),
			responseStarted: responseStarted,
			eofChecked:      eofChecked,
			eofResult:       eofResult,
		}
		req.TLS = &tls.ConnectionState{ServerName: "example.ts.net"}
		req = req.WithContext(serveHTTPContextKey.WithValue(req.Context(), &serveHTTPContext{DestPort: 443, SrcAddr: netip.MustParseAddrPort("1.2.3.4:1234")}))
		b.serveWebHandler(&serveFirstWriteBarrier{
			ResponseWriter:  rw,
			responseStarted: responseStarted,
			eofChecked:      eofChecked,
		}, req)
	}))
	t.Cleanup(frontend.Close)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(frontend.URL, "application/json", strings.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("read response: %v", err)
	}
	if string(body) != responseBody {
		t.Errorf("response length = %d, want %d", len(body), len(responseBody))
	}
	select {
	case err := <-eofResult:
		if !errors.Is(err, io.EOF) {
			t.Errorf("final request read = %v, want EOF", err)
		}
	case <-t.Context().Done():
		t.Fatal("final request EOF was not checked")
	}
}

// serveFinalEOFBody holds the transport's final EOF check until response writing has
// started, so HTTP/1's automatic request-body close cannot hide behind timing.
type serveFinalEOFBody struct {
	io.ReadCloser

	remaining       int64
	ctx             context.Context
	responseStarted <-chan struct{}
	eofChecked      chan struct{}
	eofResult       chan<- error
}

func (b *serveFinalEOFBody) Read(p []byte) (int, error) {
	if b.remaining > 0 {
		n, err := b.ReadCloser.Read(p)
		b.remaining -= int64(n)
		if n > 0 && err == io.EOF {
			err = nil
		}
		return n, err
	}

	select {
	case <-b.responseStarted:
	case <-b.ctx.Done():
		b.eofResult <- b.ctx.Err()
		close(b.eofChecked)
		return 0, b.ctx.Err()
	}
	n, err := b.ReadCloser.Read(p)
	b.eofResult <- err
	close(b.eofChecked)
	return n, err
}

type serveFirstWriteBarrier struct {
	http.ResponseWriter

	once            sync.Once
	responseStarted chan struct{}
	eofChecked      <-chan struct{}
}

func (w *serveFirstWriteBarrier) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.once.Do(func() {
		close(w.responseStarted)
		<-w.eofChecked
	})
	return n, err
}

func (w *serveFirstWriteBarrier) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
