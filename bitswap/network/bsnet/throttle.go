package bsnet

import (
	"context"
	"io"

	"golang.org/x/time/rate"
)

// throttledWriter wraps an io.Writer with a token-bucket rate limiter.
// Data larger than burst is chunked; tokens are consumed before each write.
type throttledWriter struct {
	w       io.Writer
	ctx     context.Context
	limiter *rate.Limiter
}

func newThrottledWriter(ctx context.Context, w io.Writer, limiter *rate.Limiter) *throttledWriter {
	return &throttledWriter{w: w, ctx: ctx, limiter: limiter}
}

func (tw *throttledWriter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		chunk := len(p)
		if burst := tw.limiter.Burst(); chunk > burst {
			chunk = burst
		}

		if err := tw.limiter.WaitN(tw.ctx, chunk); err != nil {
			return written, err
		}

		n, err := tw.w.Write(p[:chunk])
		written += n
		if err != nil {
			return written, err
		}
		p = p[n:]
	}
	return written, nil
}

// throttledReader wraps an io.Reader with a token-bucket rate limiter.
// Read buffer is capped at burst size; tokens are consumed after read
// based on actual bytes read.
type throttledReader struct {
	r       io.Reader
	ctx     context.Context
	limiter *rate.Limiter
}

func newThrottledReader(ctx context.Context, r io.Reader, limiter *rate.Limiter) *throttledReader {
	return &throttledReader{r: r, ctx: ctx, limiter: limiter}
}

func (tr *throttledReader) Read(p []byte) (int, error) {
	if burst := tr.limiter.Burst(); len(p) > burst {
		p = p[:burst]
	}

	n, err := tr.r.Read(p)
	if n > 0 {
		if waitErr := tr.limiter.WaitN(tr.ctx, n); waitErr != nil {
			return n, waitErr
		}
	}
	return n, err
}
