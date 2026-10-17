package session

import (
	"context"
	"testing"
	"time"

	bsbpm "github.com/ipfs/boxo/bitswap/client/internal/blockpresencemanager"
	notifications "github.com/ipfs/boxo/bitswap/client/internal/notifications"
	bssim "github.com/ipfs/boxo/bitswap/client/internal/sessioninterestmanager"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

type gatedDiscovery struct {
	started   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
}

func (f *gatedDiscovery) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	go func() {
		defer close(out)
		close(f.started)
		<-ctx.Done()
		close(f.cancelled)
		<-f.release
	}()
	return out
}

type gatedNotifications struct {
	notifications.PubSub
	started   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
}

func (n *gatedNotifications) Subscribe(ctx context.Context, keys ...cid.Cid) <-chan blocks.Block {
	out := make(chan blocks.Block)
	go func() {
		defer close(out)
		close(n.started)
		<-ctx.Done()
		close(n.cancelled)
		<-n.release
	}()
	return out
}

func TestCloseAndWaitJoinsDiscoveryAndNotificationCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	finder := &gatedDiscovery{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	notif := &gatedNotifications{notifications.New(false), make(chan struct{}), make(chan struct{}), make(chan struct{})}
	t.Cleanup(notif.Shutdown)
	sim := bssim.New()
	manager := newMockSessionMgr(sim)
	s := New(ctx, manager, 1, newFakeSessionPeerManager(), finder, sim, newFakePeerManager(), bsbpm.New(), notif, time.Hour, time.Hour, "", nil)
	t.Cleanup(func() {
		for _, gate := range []chan struct{}{finder.release, notif.release} {
			select {
			case <-gate:
			default:
				close(gate)
			}
		}
		s.CloseAndWait()
	})
	key := random.BlocksOfSize(1, blockSize)[0].Cid()
	fetched := make(chan error, 1)
	go func() { _, err := s.GetBlock(ctx, key); fetched <- err }()
	select {
	case <-notif.started:
	case <-ctx.Done():
		t.Fatal("request did not start")
	}
	s.findMorePeers(s.ctx, key)
	select {
	case <-finder.started:
	case <-ctx.Done():
		t.Fatal("discovery did not start")
	}
	joined := make(chan struct{})
	go func() { s.CloseAndWait(); close(joined) }()
	select {
	case <-finder.cancelled:
	case <-ctx.Done():
		t.Fatal("discovery was not cancelled")
	}
	select {
	case <-notif.cancelled:
	case <-ctx.Done():
		t.Fatal("subscription was not cancelled")
	}
	select {
	case <-joined:
		t.Fatal("returned before child cleanup")
	default:
	}
	close(finder.release)
	select {
	case <-joined:
		t.Fatal("returned before subscription cleanup")
	default:
	}
	close(notif.release)
	select {
	case <-joined:
	case <-ctx.Done():
		t.Fatal("completion did not settle")
	}
	select {
	case err := <-fetched:
		require.Error(t, err)
	case <-ctx.Done():
		t.Fatal("request did not settle")
	}
	require.True(t, manager.removeSessionCalled())
	// Multiple completion waiters observe the same settled session.
	s.CloseAndWait()
	_, err := s.GetBlocks(context.Background(), []cid.Cid{key})
	require.ErrorIs(t, err, context.Canceled)
}
