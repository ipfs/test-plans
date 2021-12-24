package main

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"os"

	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car/v2"
	carbs "github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/storage/bsadapter"
)

func GenerateUnixFSCarFile(ctx context.Context, rng *rand.Rand, size uint64) (*os.File, error) {
	file, err := os.CreateTemp("", "unixfs-file")

	if err != nil {
		return nil, err
	}
	if _, err := io.CopyN(file, rng, int64(size)); err != nil {
		return nil, err
	}

	outputBS, err := os.CreateTemp("", "unixfs-file-bs")
	if err != nil {
		return nil, err
	}

	if err := UnixFSCARBlockstore(ctx, outputBS.Name(), file.Name()); err != nil {
		return nil, err
	}

	return outputBS, nil
}

func GenerateUnixFSFileBS(ctx context.Context, rng *rand.Rand, size uint64) (*carbs.ReadOnly, error) {
	file, err := GenerateUnixFSCarFile(ctx, rng, size)
	if err != nil {
		return nil, err
	}
	return LoadCARBlockstore(file)
}

func GenerateDeepDagBS(ctx context.Context, rng *rand.Rand, approxBlockSize int, depth int) (blockstore.Blockstore, error) {
	outputBS, err := os.CreateTemp("", "deep-dag-bs")
	if err != nil {
		return nil, err
	}

	rwbs, err := carbs.OpenReadWrite(outputBS.Name(), nil, car.StoreIdentityCIDs(true))
	if err != nil {
		return nil, err
	}

	lsys := cidlink.DefaultLinkSystem()
	lsys.SetWriteStorage(&bsadapter.Adapter{Wrapped: rwbs})

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(io.LimitReader(rng, int64(approxBlockSize))); err != nil {
		return nil, err
	}

	leafLnk, err := lsys.Store(
		linking.LinkContext{Ctx: ctx},
		cidlink.LinkPrototype{Prefix: cid.Prefix{
			Version:  1,    // Usually '1'.
			Codec:    0x55, // 0x71 means "raw" -- See the multicodecs table: https://github.com/multiformats/multicodec/
			MhType:   0x12, // 0x12 means "sha2-256" -- See the multicodecs table: https://github.com/multiformats/multicodec/
			MhLength: 32,   // sha2-256 hash has a 32-byte sum.
		}},
		basicnode.NewBytes(buf.Bytes()), // And here's our data.
	)
	if err != nil {
		return nil, err
	}

	nextCid := leafLnk.(cidlink.Link)

	lp := cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version:  1,    // Usually '1'.
		Codec:    0x71, // 0x71 means "dag-cbor" -- See the multicodecs table: https://github.com/multiformats/multicodec/
		MhType:   0x12, // 0x12 means "sha2-256" -- See the multicodecs table: https://github.com/multiformats/multicodec/
		MhLength: 32,   // sha2-256 hash has a 32-byte sum.
	}}

	type Node struct {
		Height     int64
		Next       cidlink.Link
		ExtraBytes []byte
	}

	for i := 1; i < depth; i++ {
		buf := new(bytes.Buffer)
		if _, err := buf.ReadFrom(io.LimitReader(rng, int64(approxBlockSize))); err != nil {
			return nil, err
		}

		next := &Node{
			Height:     int64(i),
			Next:       nextCid,
			ExtraBytes: buf.Bytes(),
		}
		node := bindnode.Wrap(next, nil)

		lnk, err := lsys.Store(
			linking.LinkContext{}, // The zero value is fine.  Configure it it you want cancellability or other features.
			lp,                    // The LinkPrototype says what codec and hashing to use.
			node,                  // And here's our data.
		)
		if err != nil {
			return nil, err
		}
		nextCid = lnk.(cidlink.Link)
	}

	if err := rwbs.Finalize(); err != nil {
		return nil, err
	}

	return LoadCARBlockstore(outputBS)
}
