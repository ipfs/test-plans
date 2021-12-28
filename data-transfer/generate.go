package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"

	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"

	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car/v2"
	carbs "github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/storage/bsadapter"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/testground/sdk-go/runtime"
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

func GenerateDeepDagCarFile(ctx context.Context, rng *rand.Rand, approxBlockSize int, depth int) (*os.File, error) {
	outputBS, err := os.CreateTemp("", "deep-dag-bs")
	if err != nil {
		return nil, err
	}

	// make a cid with the right length that we eventually will patch with the root.
	hasher, err := multihash.GetHasher(multihash.SHA2_256)
	if err != nil {
		return nil, err
	}
	digest := hasher.Sum([]byte{})
	hash, err := multihash.Encode(digest, multihash.SHA2_256)
	if err != nil {
		return nil, err
	}
	proxyRoot := cid.NewCidV1(uint64(multicodec.DagCbor), hash)
	rwbs, err := carbs.OpenReadWrite(outputBS.Name(), []cid.Cid{proxyRoot}, car.StoreIdentityCIDs(true))
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

	ts := schema.TypeSystem{}
	ts.Init()
	ts.Accumulate(schema.SpawnBytes("Bytes"))
	ts.Accumulate(schema.SpawnInt("Int"))
	ts.Accumulate(schema.SpawnLink("Link"))
	ts.Accumulate(schema.SpawnStruct("Node",
		[]schema.StructField{
			schema.SpawnStructField("Height", "Int", false, false),
			schema.SpawnStructField("Next", "Link", false, false),
			schema.SpawnStructField("ExtraBytes", "Bytes", false, false),
		},
		schema.SpawnStructRepresentationMap(nil),
	))

	schemaType := ts.TypeByName("Node")

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
		node := bindnode.Wrap(next, schemaType)

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
	if err := car.ReplaceRootsInFile(outputBS.Name(), []cid.Cid{nextCid.Cid}); err != nil {
		return nil, err
	}

	return outputBS, nil
}

func GetBlockstoreFromConfig(ctx context.Context, runenv *runtime.RunEnv) (blockstore.Blockstore, cid.Cid, error) {
	f, err := GetCARFileFromConfig(ctx, runenv)
	if err != nil {
		return nil, cid.Undef, err
	}

	dataBS, err := LoadCARBlockstore(f)
	if err != nil {
		return nil, cid.Undef, err
	}

	roots, err := dataBS.Roots()
	if err != nil {
		return nil, cid.Undef, err
	}
	if len(roots) != 1 {
		return nil, cid.Undef, fmt.Errorf("there should be exactly one root, instead there are %d", len(roots))
	}
	rootCid := roots[0]

	return dataBS, rootCid, nil
}

func GetCARFileFromConfig(ctx context.Context, runenv *runtime.RunEnv) (*os.File, error) {
	dagtype := runenv.StringParam("dagtype")
	switch dagtype {
	case "unixfs-file":
		dagparam := runenv.StringParam("dagparams")
		fileSize, err := humanize.ParseBytes(dagparam)
		if err != nil {
			return nil, err
		}
		f, err := GenerateUnixFSCarFile(ctx, rand.New(rand.NewSource(0)), fileSize)
		if err != nil {
			return nil, err
		}

		return f, nil
	case "deep-ipld":
		dagparam := runenv.StringParam("dagparams")

		type Params struct {
			Padding string
			Depth int
		}

		var p Params
		if err := json.Unmarshal([]byte(dagparam), &p); err != nil {
			return nil, err
		}

		depth := p.Depth
		approxBlockSize, err := humanize.ParseBytes(p.Padding)
		if err != nil {
			return nil, err
		}

		runenv.RecordMessage("blockstore depth %d", depth)

		f, err := GenerateDeepDagCarFile(ctx, rand.New(rand.NewSource(0)), int(approxBlockSize), depth)
		if err != nil {
			return nil, err
		}
		return f, nil
	default:
		panic(fmt.Sprintf("unsupported dagtype %s", dagtype))
	}
}