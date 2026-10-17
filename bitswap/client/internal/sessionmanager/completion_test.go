package sessionmanager

import (
	"context"
	"testing"
	"time"

	bsbpm "github.com/ipfs/boxo/bitswap/client/internal/blockpresencemanager"
	"github.com/ipfs/boxo/bitswap/client/internal/notifications"
	bssession "github.com/ipfs/boxo/bitswap/client/internal/session"
	bssim "github.com/ipfs/boxo/bitswap/client/internal/sessioninterestmanager"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestCancelledConstructionCannotReregisterCompletedSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	notif := notifications.New(false)
	defer notif.Shutdown()
	removed := make(chan struct{})
	factory := func(ctx context.Context, sm bssession.SessionManager, id uint64,
		sprm bssession.SessionPeerManager, sim *bssim.SessionInterestManager,
		pm bssession.PeerManager, bpm *bsbpm.BlockPresenceManager,
		notif notifications.PubSub, searchDelay, rebroadcastDelay time.Duration, self peer.ID,
	) Session {
		session := &fakeSession{id: id, sm: sm}
		go func() {
			<-ctx.Done()
			sm.RemoveSession(id)
			close(removed)
		}()
		// Expose the old registration race: completion used to be allowed
		// before the constructor had returned its session to the manager.
		select {
		case <-removed:
		case <-time.After(50 * time.Millisecond):
		}
		return session
	}
	sm := New(factory, bssim.New(), peerManagerFactory, bsbpm.New(), &fakePeerManager{}, notif, "")
	session := sm.NewSession(ctx, time.Hour, time.Hour)
	select {
	case <-removed:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled session did not leave the manager")
	}
	sm.sessLk.Lock()
	_, retained := sm.sessions[session.ID()]
	sm.sessLk.Unlock()
	if retained {
		t.Fatal("completed session was registered after its removal")
	}
}

type closingSession struct {
	*fakeSession
	closed bool
}

func (s *closingSession) Close() {
	s.closed = true
	s.fakeSession.Close()
}

func TestNewSessionAfterShutdownIsClosed(t *testing.T) {
	notif := notifications.New(false)
	defer notif.Shutdown()
	var created *closingSession
	factory := func(ctx context.Context, sm bssession.SessionManager, id uint64,
		sprm bssession.SessionPeerManager, sim *bssim.SessionInterestManager,
		pm bssession.PeerManager, bpm *bsbpm.BlockPresenceManager,
		notif notifications.PubSub, searchDelay, rebroadcastDelay time.Duration, self peer.ID,
	) Session {
		created = &closingSession{fakeSession: &fakeSession{id: id, sm: sm}}
		return created
	}
	sm := New(factory, bssim.New(), peerManagerFactory, bsbpm.New(), &fakePeerManager{}, notif, "")
	sm.Shutdown()
	sm.NewSession(context.Background(), time.Hour, time.Hour)
	if !created.closed {
		t.Fatal("session created after manager shutdown remained live")
	}
}
