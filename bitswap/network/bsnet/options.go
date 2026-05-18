package bsnet

import (
	"github.com/ipfs/boxo/bitswap/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	"golang.org/x/time/rate"
)

type NetOpt func(*Settings)

type Settings struct {
	ProtocolPrefix      protocol.ID
	SupportedProtocols  []protocol.ID
	connEvtMgr          *network.ConnectEventManager
	UploadBytesPerSec   int64
	DownloadBytesPerSec int64
}

func Prefix(prefix protocol.ID) NetOpt {
	return func(settings *Settings) {
		settings.ProtocolPrefix = prefix
	}
}

func SupportedProtocols(protos []protocol.ID) NetOpt {
	return func(settings *Settings) {
		settings.SupportedProtocols = protos
	}
}

// WithConnectEventManager allows to set the ConnectEventManager. Upon
// Start(), we will run SetListeners(). If not provided, an event manager will
// be created internally. This allows re-using the event manager among several
// Network instances.
func WithConnectEventManager(evm *network.ConnectEventManager) NetOpt {
	return func(settings *Settings) {
		settings.connEvtMgr = evm
	}
}

// WithUploadRateLimit sets the outbound Bitswap rate limit in bytes per second.
// A value <= 0 disables rate limiting.
func WithUploadRateLimit(bytesPerSec int64) NetOpt {
	return func(settings *Settings) {
		settings.UploadBytesPerSec = bytesPerSec
	}
}

// WithDownloadRateLimit sets the inbound Bitswap rate limit in bytes per second.
// A value <= 0 disables rate limiting.
func WithDownloadRateLimit(bytesPerSec int64) NetOpt {
	return func(settings *Settings) {
		settings.DownloadBytesPerSec = bytesPerSec
	}
}

const defaultRateLimitBurst = 64 * 1024

func newRateLimiter(bytesPerSec int64) *rate.Limiter {
	limiter := rate.NewLimiter(rate.Inf, defaultRateLimitBurst)
	applyRateLimit(limiter, bytesPerSec)
	return limiter
}

func applyRateLimit(limiter *rate.Limiter, bytesPerSec int64) {
	if bytesPerSec <= 0 {
		limiter.SetLimit(rate.Inf)
		limiter.SetBurst(defaultRateLimitBurst)
		return
	}

	limiter.SetLimit(rate.Limit(bytesPerSec))
	limiter.SetBurst(rateLimitBurst(bytesPerSec))
}

func rateLimitBurst(bytesPerSec int64) int {
	if bytesPerSec <= defaultRateLimitBurst {
		return defaultRateLimitBurst
	}

	maxInt := int64(^uint(0) >> 1)
	if bytesPerSec > maxInt {
		return int(maxInt)
	}

	return int(bytesPerSec)
}
