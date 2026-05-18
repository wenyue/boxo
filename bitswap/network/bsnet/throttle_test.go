package bsnet

import (
	"bytes"
	"context"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestThrottledWriter_RateLimit(t *testing.T) {
	var buf bytes.Buffer
	// 10KB/s with 1KB burst
	limiter := rate.NewLimiter(10*1024, 1024)
	tw := newThrottledWriter(context.Background(), &buf, limiter)

	data := make([]byte, 5*1024) // 5KB
	start := time.Now()
	n, err := tw.Write(data)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != len(data) {
		t.Fatalf("expected %d bytes written, got %d", len(data), n)
	}
	// At 10KB/s, 5KB should take ~0.4s (5 chunks of 1KB, first is free from burst)
	if elapsed < 350*time.Millisecond {
		t.Fatalf("write completed too fast: %v (expected >= 350ms)", elapsed)
	}
}

func TestThrottledWriter_ContextCancel(t *testing.T) {
	var buf bytes.Buffer
	limiter := rate.NewLimiter(100, 100) // very slow: 100 bytes/sec
	ctx, cancel := context.WithCancel(context.Background())
	tw := newThrottledWriter(ctx, &buf, limiter)

	// Cancel context immediately
	cancel()

	data := make([]byte, 1024)
	_, err := tw.Write(data)
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
}

func TestThrottledReader_RateLimit(t *testing.T) {
	data := make([]byte, 5*1024) // 5KB
	r := bytes.NewReader(data)
	// 10KB/s with 1KB burst
	limiter := rate.NewLimiter(10*1024, 1024)
	tr := newThrottledReader(context.Background(), r, limiter)

	buf := make([]byte, 8*1024)
	totalRead := 0
	start := time.Now()
	for totalRead < len(data) {
		n, err := tr.Read(buf)
		totalRead += n
		if err != nil {
			break
		}
	}
	elapsed := time.Since(start)

	if totalRead != len(data) {
		t.Fatalf("expected %d bytes read, got %d", len(data), totalRead)
	}
	// At 10KB/s, 5KB should take ~0.4s
	if elapsed < 350*time.Millisecond {
		t.Fatalf("read completed too fast: %v (expected >= 350ms)", elapsed)
	}
}

func TestThrottledWriter_UnlimitedRate(t *testing.T) {
	var buf bytes.Buffer
	// rate.Inf means no throttling
	limiter := rate.NewLimiter(rate.Inf, 64*1024)
	tw := newThrottledWriter(context.Background(), &buf, limiter)

	data := make([]byte, 1024*1024) // 1MB
	start := time.Now()
	n, err := tw.Write(data)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != len(data) {
		t.Fatalf("expected %d bytes written, got %d", len(data), n)
	}
	// With Inf rate, should complete nearly instantly
	if elapsed > 1*time.Second {
		t.Fatalf("unlimited write took too long: %v", elapsed)
	}
}

func TestThrottledWriter_DataIntegrity(t *testing.T) {
	var buf bytes.Buffer
	limiter := rate.NewLimiter(50*1024, 1024) // 50KB/s
	tw := newThrottledWriter(context.Background(), &buf, limiter)

	// Write known pattern
	data := make([]byte, 4*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	n, err := tw.Write(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != len(data) {
		t.Fatalf("expected %d bytes written, got %d", len(data), n)
	}

	// Verify data integrity
	written := buf.Bytes()
	if !bytes.Equal(data, written) {
		t.Fatal("written data does not match original")
	}
}
