package blockservice

import (
	"context"
	"sync"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange"
	"github.com/ipfs/boxo/exchange/offline"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-test/random"
)

type completingFetcher struct {
	exchange.Fetcher
	closed chan struct{}
	once   sync.Once
}

func (f *completingFetcher) CloseAndWait() { f.once.Do(func() { close(f.closed) }) }

func TestOwnedSessionJoinsOnlyItsOwnExchange(t *testing.T) {
	ctx := context.Background()
	local := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	remote := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	blocks := random.BlocksOfSize(2, blockSize)
	if err := remote.PutMany(ctx, blocks); err != nil {
		t.Fatal(err)
	}
	shared := offline.Exchange(remote)
	first := &completingFetcher{Fetcher: shared, closed: make(chan struct{})}
	second := &completingFetcher{Fetcher: shared, closed: make(chan struct{})}
	exchange := &fakeSessionExchange{Interface: shared, session: first}
	service := New(local, exchange)
	borrowed := NewSession(ctx, service)
	if _, err := borrowed.GetBlock(ctx, blocks[0].Cid()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := borrowed.CloseAndWait(); err != nil {
			t.Error(err)
		}
	})
	exchange.session = second
	owned := NewOwnedSession(EmbedSessionInContext(ctx, borrowed), service)
	if _, err := owned.GetBlock(ctx, blocks[1].Cid()); err != nil {
		t.Fatal(err)
	}
	if err := owned.CloseAndWait(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-second.closed:
	default:
		t.Fatal("owned exchange was not joined")
	}
	select {
	case <-first.closed:
		t.Fatal("borrowed session was closed")
	default:
	}
}
