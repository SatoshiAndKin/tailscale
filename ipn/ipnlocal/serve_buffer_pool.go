// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

//go:build !ts_omit_serve

package ipnlocal

import "tailscale.com/syncs"

const serveProxyBufferSize = 32 << 10

// serveProxyBuffers shares response-copy scratch buffers across Serve requests.
// sync.Pool can discard idle buffers during GC; active requests each own their
// buffer until httputil.ReverseProxy returns it.
var serveProxyBuffers serveProxyBufferPool

type serveProxyBufferPool struct {
	pool syncs.Pool[*[serveProxyBufferSize]byte]
}

func (p *serveProxyBufferPool) Get() []byte {
	b := p.pool.Get()
	if b == nil {
		b = new([serveProxyBufferSize]byte)
	}
	return b[:]
}

func (p *serveProxyBufferPool) Put(b []byte) {
	if cap(b) != serveProxyBufferSize {
		return
	}
	p.pool.Put((*[serveProxyBufferSize]byte)(b[:serveProxyBufferSize]))
}
