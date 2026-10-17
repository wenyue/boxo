package mfs

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

type unreadableChildDAG struct {
	ipld.DAGService
	blocked cid.Cid
	failure error
}

func (d *unreadableChildDAG) Get(ctx context.Context, key cid.Cid) (ipld.Node, error) {
	if key.Equals(d.blocked) {
		return nil, d.failure
	}
	return d.DAGService.Get(ctx, key)
}

func TestUnreadableLinksArePreservedByCreation(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		for _, operation := range []string{"file", "directory", "parent"} {
			for _, failure := range []error{
				ipld.ErrNotFound{},
				&os.PathError{Op: "read", Path: "old-block", Err: os.ErrPermission},
				&os.PathError{Op: "read", Path: "old-block", Err: os.ErrNotExist},
				errors.New("invalid existing node"),
			} {
				t.Run(operation+"/"+failure.Error()+map[bool]string{false: "/basic", true: "/hamt"}[sharded], func(t *testing.T) {
					ctx := context.Background()
					store := getDagserv(t)
					old := dag.NodeWithData(ft.FilePBData([]byte("retained bytes"), 14))
					replacement := dag.NodeWithData(ft.FilePBData([]byte("new bytes"), 9))
					if err := store.Add(ctx, old); err != nil {
						t.Fatal(err)
					}
					options := []uio.DirectoryOption{}
					if sharded {
						options = append(options, uio.WithMaxLinks(1), uio.WithMaxHAMTFanout(16))
					}
					directory, err := uio.NewDirectory(store, options...)
					if err != nil {
						t.Fatal(err)
					}
					for _, name := range []string{"taken", "other"} {
						if err := directory.AddChild(ctx, name, old); err != nil {
							t.Fatal(err)
						}
					}
					raw, err := directory.GetNode()
					if err != nil {
						t.Fatal(err)
					}
					root, err := NewRoot(ctx, &unreadableChildDAG{store, old.Cid(), failure}, raw.(*dag.ProtoNode), nil, nil)
					if err != nil {
						t.Fatal(err)
					}
					switch operation {
					case "file":
						err = root.GetDirectory().AddChild("taken", replacement)
					case "directory":
						_, err = root.GetDirectory().Mkdir("taken")
					case "parent":
						err = Mkdir(root, "/taken/nested", MkdirOpts{Mkparents: true})
					}
					if !errors.Is(err, failure) {
						t.Fatalf("lost original failure: %v", err)
					}
					retained, err := root.GetDirectory().GetNode()
					if err != nil {
						t.Fatal(err)
					}
					if !retained.Cid().Equals(raw.Cid()) {
						t.Fatal("failed creation replaced an existing link")
					}
				})
			}
		}
	}
}

func TestConcurrentFirstBindingHasOneWinner(t *testing.T) {
	ctx := context.Background()
	store := getDagserv(t)
	root, err := NewRoot(ctx, store, emptyDirNode(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	outcomes := make(chan error, 2)
	var writers sync.WaitGroup
	for _, data := range []string{"first", "second"} {
		writers.Add(1)
		go func(data string) {
			defer writers.Done()
			outcomes <- root.GetDirectory().AddChild("new", dag.NodeWithData(ft.FilePBData([]byte(data), uint64(len(data)))))
		}(data)
	}
	writers.Wait()
	close(outcomes)
	winners := 0
	for err := range outcomes {
		if err == nil {
			winners++
		} else if !errors.Is(err, ErrDirExists) {
			t.Fatal(err)
		}
	}
	if winners != 1 {
		t.Fatalf("expected one winner, got %d", winners)
	}
}
