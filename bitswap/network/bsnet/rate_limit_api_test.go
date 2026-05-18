package bsnet

import (
	"testing"

	"golang.org/x/time/rate"
)

func TestNewFromIpfsHost_UsesByteRateOptions(t *testing.T) {
	network := NewFromIpfsHost(nil,
		WithUploadRateLimit(1024),
		WithDownloadRateLimit(128*1024),
	).(*impl)

	if got := network.uploadLimiter.Limit(); got != rate.Limit(1024) {
		t.Fatalf("upload limit = %v, want %v", got, rate.Limit(1024))
	}
	if got := network.uploadLimiter.Burst(); got != 64*1024 {
		t.Fatalf("upload burst = %d, want %d", got, 64*1024)
	}
	if got := network.downloadLimiter.Limit(); got != rate.Limit(128*1024) {
		t.Fatalf("download limit = %v, want %v", got, rate.Limit(128*1024))
	}
	if got := network.downloadLimiter.Burst(); got != 128*1024 {
		t.Fatalf("download burst = %d, want %d", got, 128*1024)
	}
}

func TestSetUploadLimit_UsesByteRateValues(t *testing.T) {
	network := &impl{
		uploadLimiter: rate.NewLimiter(rate.Inf, 64*1024),
	}

	network.SetUploadLimit(1024)
	if got := network.uploadLimiter.Limit(); got != rate.Limit(1024) {
		t.Fatalf("limit = %v, want %v", got, rate.Limit(1024))
	}
	if got := network.uploadLimiter.Burst(); got != 64*1024 {
		t.Fatalf("burst = %d, want %d", got, 64*1024)
	}

	network.SetUploadLimit(0)
	if got := network.uploadLimiter.Limit(); got != rate.Inf {
		t.Fatalf("limit = %v, want %v", got, rate.Inf)
	}
}
