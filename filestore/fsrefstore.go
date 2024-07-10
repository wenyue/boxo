package filestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/ipfs/boxo/filestore/pb"

	gogoproto "github.com/gogo/protobuf/proto"
	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	posinfo "github.com/ipfs/boxo/filestore/posinfo"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsns "github.com/ipfs/go-datastore/namespace"
	dsq "github.com/ipfs/go-datastore/query"
	ipld "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
	proto "google.golang.org/protobuf/proto"
)

// FilestorePrefix identifies the key prefix for FileManager blocks.

const (
	MaxFilePosNum = 20
)

var (
	FilestorePrefix = ds.NewKey("filestore")

	ErrUrlstoreNotSupported = errors.New("urlstore is not supported")
)

// FileManager is a blockstore implementation which stores special
// blocks FilestoreNode type. These nodes only contain a reference
// to the actual location of the block data in the filesystem
// (a path and an offset).
type FileManager struct {
	AllowFiles bool
	AllowUrls  bool
	ds         ds.Batching
	root       string
}

// CorruptReferenceError implements the error interface.
// It is used to indicate that the block contents pointed
// by the referencing blocks cannot be retrieved (i.e. the
// file is not found, or the data changed as it was being read).
type CorruptReferenceError struct {
	Code Status
	Err  error
}

// Error() returns the error message in the CorruptReferenceError
// as a string.
func (c CorruptReferenceError) Error() string {
	return c.Err.Error()
}

// NewFileManager initializes a new file manager with the given
// datastore and root. All FilestoreNodes paths are relative to the
// root path given here, which is prepended for any operations.
func NewFileManager(ds ds.Batching, root string) *FileManager {
	return &FileManager{ds: dsns.Wrap(ds, FilestorePrefix), root: root}
}

// AllKeysChan returns a channel from which to read the keys stored in
// the FileManager. If the given context is cancelled the channel will be
// closed.
//
// All CIDs returned are of type Raw.
func (f *FileManager) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	q := dsq.Query{KeysOnly: true}

	res, err := f.ds.Query(ctx, q)
	if err != nil {
		return nil, err
	}

	out := make(chan cid.Cid, dsq.KeysOnlyBufSize)
	go func() {
		defer close(out)
		for {
			v, ok := res.NextSync()
			if !ok {
				return
			}

			k := ds.RawKey(v.Key)
			mhash, err := dshelp.DsKeyToMultihash(k)
			if err != nil {
				logger.Errorf("decoding cid from filestore: %s", err)
				continue
			}

			select {
			case out <- cid.NewCidV1(cid.Raw, mhash):
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

// DeleteBlock deletes the reference-block from the underlying
// datastore. It does not touch the referenced data.
func (f *FileManager) DeleteBlock(ctx context.Context, c cid.Cid) error {
	err := f.ds.Delete(ctx, dshelp.MultihashToDsKey(c.Hash()))
	if err == ds.ErrNotFound {
		return ipld.ErrNotFound{Cid: c}
	}
	return err
}

// Get reads a block from the datastore. Reading a block
// is done in two steps: the first step retrieves the reference
// block from the datastore. The second step uses the stored
// path and offsets to read the raw block data directly from disk.
func (f *FileManager) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	dobj, err := f.getDataObj(ctx, c.Hash())
	if err != nil {
		return nil, err
	}
	out, err := f.readDataObj(ctx, c.Hash(), dobj)
	if err != nil {
		return nil, err
	}

	return blocks.NewBlockWithCid(out, c)
}

// GetSize gets the size of the block from the datastore.
//
// This method may successfully return the size even if returning the block
// would fail because the associated file is no longer available.
func (f *FileManager) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	dobj, err := f.getDataObj(ctx, c.Hash())
	if err != nil {
		return -1, err
	}
	return int(dobj.GetSize()), nil
}

func (f *FileManager) readDataObj(
	ctx context.Context, m mh.Multihash, d *pb.ExtDataObj) ([]byte, error) {
	if f.AllowUrls {
		return nil, ErrUrlstoreNotSupported
	}
	fmt.Println("readDataObj ", m.String())
	return f.readAndFixFileDataObj(ctx, m, d)
}

func (f *FileManager) getOrigDataObj(ctx context.Context, m mh.Multihash) (*pb.DataObj, error) {
	o, err := f.ds.Get(ctx, dshelp.MultihashToDsKey(m))
	switch err {
	case ds.ErrNotFound:
		return nil, ipld.ErrNotFound{Cid: cid.NewCidV1(cid.Raw, m)}
	case nil:
		//
	default:
		return nil, err
	}

	return unmarshalOrigDataObj(o)
}

func (f *FileManager) getDataObj(ctx context.Context, m mh.Multihash) (*pb.ExtDataObj, error) {
	o, err := f.ds.Get(ctx, dshelp.MultihashToDsKey(m))
	switch err {
	case ds.ErrNotFound:
		return nil, ipld.ErrNotFound{Cid: cid.NewCidV1(cid.Raw, m)}
	case nil:
		//
	default:
		return nil, err
	}

	return unmarshalDataObj(o)
}

func unmarshalOrigDataObj(data []byte) (*pb.DataObj, error) {
	var dobj pb.DataObj
	if err := gogoproto.Unmarshal(data, &dobj); err != nil {
		return nil, err
	}

	return &dobj, nil
}

func unmarshalDataObj(data []byte) (*pb.ExtDataObj, error) {
	var dobj pb.ExtDataObj
	if err := proto.Unmarshal(data, &dobj); err != nil {
		return nil, err
	}

	return &dobj, nil
}

func (f *FileManager) updateFileDataObj(
	ctx context.Context, m mh.Multihash, d *pb.ExtDataObj) error {
	if len(d.PosList) == 0 {
		return f.ds.Delete(ctx, dshelp.MultihashToDsKey(m))
	} else {
		data, err := proto.Marshal(d)
		if err != nil {
			return err
		}
		return f.ds.Put(ctx, dshelp.MultihashToDsKey(m), data)
	}
}

func (f *FileManager) readAndFixFileDataObj(
	ctx context.Context, m mh.Multihash, d *pb.ExtDataObj) ([]byte, error) {
	if !f.AllowFiles {
		return nil, ErrFilestoreNotEnabled
	}

	readData := func(fp *pb.FilePos, bs uint64) ([]byte, *CorruptReferenceError) {
		p := filepath.FromSlash(fp.GetFilePath())
		abspath := filepath.Join(f.root, p)

		fi, err := os.Open(abspath)
		if os.IsNotExist(err) {
			return nil, &CorruptReferenceError{StatusFileNotFound, err}
		} else if err != nil {
			return nil, &CorruptReferenceError{StatusFileError, err}
		}
		defer fi.Close()

		_, err = fi.Seek(int64(fp.GetOffset()), io.SeekStart)
		if err != nil {
			return nil, &CorruptReferenceError{StatusFileError, err}
		}

		outbuf := make([]byte, bs)
		_, err = io.ReadFull(fi, outbuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, &CorruptReferenceError{StatusFileChanged, err}
		} else if err != nil {
			return nil, &CorruptReferenceError{StatusFileError, err}
		}

		// Work with CIDs for this, as they are a nice wrapper and things
		// will not break if multihashes underlying types change.
		origCid := cid.NewCidV1(cid.Raw, m)
		outcid, err := origCid.Prefix().Sum(outbuf)
		if err != nil {
			return nil, &CorruptReferenceError{StatusOtherError, err}
		}

		if !origCid.Equals(outcid) {
			return nil, &CorruptReferenceError{
				StatusFileChanged,
				fmt.Errorf("data in file did not match. %s offset %d", fp.GetFilePath(), fp.GetOffset()),
			}
		}

		return outbuf, nil
	}

	errPoses := make([]*pb.FilePos, 0)
	var referr *CorruptReferenceError
	for index, fp := range d.GetPosList() {
		var outbuf []byte
		outbuf, referr = readData(fp, d.GetSize())
		if referr != nil {
			switch referr.Code {
			case StatusFileError:
				errPoses = append(errPoses, fp)
			case StatusFileNotFound, StatusFileChanged, StatusOtherError:
				//
			default:
				logger.Error("unexpected error: %v", referr)
			}
		} else {
			if index != 0 {
				d.PosList = append(d.PosList[index:], errPoses...)
				if err := f.updateFileDataObj(ctx, m, d); err != nil {
					return nil, err
				}
			}
			fmt.Println("readAndFixFileDataObj ", m.String())
			return outbuf, nil
		}
	}
	d.PosList = errPoses
	if err := f.updateFileDataObj(ctx, m, d); err != nil {
		return nil, err
	}
	return nil, referr
}

// Has returns if the FileManager is storing a block reference. It would check if the file exists.
func (f *FileManager) Has(ctx context.Context, c cid.Cid) (bool, error) {
	// NOTE: interesting thing to consider. Has doesnt validate the data.
	// So the data on disk could be invalid, and we could think we have it.
	fmt.Println("Has ", c.String())
	m := c.Hash()
	dsk := dshelp.MultihashToDsKey(m)
	has, err := f.ds.Has(ctx, dsk)
	if !has || err != nil {
		return has, err
	}
	d, err := f.getDataObj(ctx, m)
	if err != nil {
		return false, err
	}
	errPoses := make([]*pb.FilePos, 0)
	for index, fp := range d.GetPosList() {
		p := filepath.FromSlash(fp.GetFilePath())
		abspath := filepath.Join(f.root, p)
		_, err := os.Stat(abspath)
		if err != nil {
			if !os.IsNotExist(err) {
				errPoses = append(errPoses, fp)
			}
		} else {
			if index != 0 {
				d.PosList = append(d.PosList[index:], errPoses...)
				if err := f.updateFileDataObj(ctx, m, d); err != nil {
					return false, err
				}
			}
			return true, nil
		}
	}
	d.PosList = errPoses
	if err := f.updateFileDataObj(ctx, m, d); err != nil {
		return false, err
	}
	return false, nil
}

type putter interface {
	Put(context.Context, ds.Key, []byte) error
}

// Put adds a new reference block to the FileManager. It does not check
// that the reference is valid.
func (f *FileManager) Put(ctx context.Context, b *posinfo.FilestoreNode) error {
	return f.putTo(ctx, b, f.ds)
}

func (f *FileManager) putTo(ctx context.Context, b *posinfo.FilestoreNode, to putter) error {
	if IsURL(b.PosInfo.FullPath) {
		if !f.AllowUrls {
			return ErrUrlstoreNotEnabled
		}
		return ErrUrlstoreNotSupported
	}

	if !f.AllowFiles {
		return ErrFilestoreNotEnabled
	}
	//lint:ignore SA1019 // ignore staticcheck
	if !filepath.HasPrefix(b.PosInfo.FullPath, f.root) {
		return fmt.Errorf("cannot add filestore references outside ipfs root (%s)", f.root)
	}

	p, err := filepath.Rel(f.root, b.PosInfo.FullPath)
	if err != nil {
		return err
	}

	dobj, err := f.getDataObj(ctx, b.Cid().Hash())
	bs := uint64(len(b.RawData()))
	switch err.(type) {
	case nil:
		if dobj.Size != bs {
			return fmt.Errorf("data size mismatch. %d != %d", dobj.Size, bs)
		}
	case ipld.ErrNotFound:
		dobj = &pb.ExtDataObj{Size: bs}
	default:
		return err
	}

	fp := pb.FilePos{FilePath: filepath.ToSlash(p), Offset: b.PosInfo.Offset}
	for _, pos := range dobj.PosList {
		if pos.GetFilePath() == fp.GetFilePath() && pos.GetOffset() == fp.GetOffset() {
			return nil
		}
	}

	if len(dobj.PosList) < MaxFilePosNum {
		dobj.PosList = append(dobj.PosList, &fp)
	} else {
		dobj.PosList[len(dobj.PosList)-1] = &fp
	}
	data, err := proto.Marshal(dobj)
	if err != nil {
		return err
	}

	return to.Put(ctx, dshelp.MultihashToDsKey(b.Cid().Hash()), data)
}

// PutMany is like Put() but takes a slice of blocks instead,
// allowing it to create a batch transaction.
func (f *FileManager) PutMany(ctx context.Context, bs []*posinfo.FilestoreNode) error {
	batch, err := f.ds.Batch(ctx)
	if err != nil {
		return err
	}

	for _, b := range bs {
		if err := f.putTo(ctx, b, batch); err != nil {
			return err
		}
	}

	return batch.Commit(ctx)
}

func (f *FileManager) MigrateToExt(ctx context.Context) error {
	cidCh, err := f.AllKeysChan(ctx)
	if err != nil {
		return err
	}

	// Create a wait group to wait for all workers to finish
	var wg sync.WaitGroup

	// Create 8 workers
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			batch, err := f.ds.Batch(ctx)
			if err != nil {
				logger.Error("create batch error: %v", err)
				return
			}

			for cid := range cidCh {
				m := cid.Hash()
				if _, err := f.getDataObj(ctx, m); err == nil {
					// Already migrated.
					continue
				}
				// Read original data object.
				dobj, err := f.getOrigDataObj(ctx, m)
				if err != nil {
					batch.Delete(ctx, dshelp.MultihashToDsKey(m))
					continue
				}
				// Convert to ext data object.
				extdobj := &pb.ExtDataObj{
					PosList: []*pb.FilePos{{FilePath: dobj.FilePath, Offset: dobj.Offset}},
					Size:    dobj.Size_,
				}
				// Write ext data object.
				data, err := proto.Marshal(extdobj)
				if err != nil {
					logger.Error("marshal extdobj error: %v", err)
					batch.Delete(ctx, dshelp.MultihashToDsKey(m))
					continue
				}
				if err := batch.Put(ctx, dshelp.MultihashToDsKey(m), data); err != nil {
					logger.Error("put extdobj error: %v", err)
					batch.Delete(ctx, dshelp.MultihashToDsKey(m))
					continue
				}
			}

			batch.Commit(ctx)
		}()
	}

	// Wait for all workers to finish
	wg.Wait()

	return nil
}

// IsURL returns true if the string represents a valid URL that the
// urlstore can handle.  More specifically it returns true if a string
// begins with 'http://' or 'https://'.
func IsURL(str string) bool {
	return (len(str) > 7 && str[0] == 'h' && str[1] == 't' && str[2] == 't' && str[3] == 'p') &&
		((len(str) > 8 && str[4] == 's' && str[5] == ':' && str[6] == '/' && str[7] == '/') ||
			(str[4] == ':' && str[5] == '/' && str[6] == '/'))
}
