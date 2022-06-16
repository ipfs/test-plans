package manifetch

import (
	"context"
	"fmt"
	"io"

	"github.com/fxamacker/cbor/v2"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-msgio"
)

type Client struct {
	h host.Host
}

func NewClient(h host.Host) *Client {
	return &Client{h: h}
}

// Get gets CIDs from a root CID from a specific peer.
// If we establish a connection, a TreeIter will be returned.
func (c *Client) Get(ctx context.Context, peer peer.ID, root cid.Cid) (*TreeIter, error) {
	stream, err := c.h.NewStream(ctx, peer, manifetchID)
	if err != nil {
		return nil, err
	}

	w := msgio.NewVarintWriter(stream)
	// TODO see why if we close the writer the reader won't work
	// defer w.Close()

	bdata, err := cbor.Marshal(&Request{Root: root.String()})
	if err != nil {
		return nil, err
	}

	if err := w.WriteMsg(bdata); err != nil {
		return nil, err
	}

	r := msgio.NewVarintReaderSize(stream, network.MessageSizeMax)

	resp, err := r.ReadMsg()
	if err != nil {
		return nil, err
	}

	res := &Response{}
	if err := cbor.Unmarshal(resp, res); err != nil {
		return nil, err
	}

	// TODO handle heeaders
	fmt.Println("HEADER RESPONSE", res.Headers)

	return NewTreeIter(r)
}

// NewTreeIter will return a TreeIter that will iterate over results from a manifetch request.
// The first element is loaded on memory when creating the object due to API limitations.
// A specific order is not guaranteed.
func NewTreeIter(reader msgio.ReadCloser) (*TreeIter, error) {
	iter := &TreeIter{
		reader: reader,
	}

	return iter, iter.read()

}

type TreeIter struct {
	reader msgio.ReadCloser

	previous *Node
	next     bool
}

// HasNext returns true if there are still objects to be read, and false if not.
func (i *TreeIter) HasNext() bool {
	return i.next
}

func (i *TreeIter) read() error {
	resp, err := i.reader.ReadMsg()

	if err != nil && err == io.EOF {
		i.previous = nil
		i.next = false
		return nil
	}
	if err != nil {
		return err
	}

	res := &Node{}
	if err := cbor.Unmarshal(resp, res); err != nil {
		return err
	}

	i.previous = res
	i.next = true

	return nil
}

// Next will return the next object in the queue, and load the following one if any.
// If Next() is called after HasNext() was false, the returned object will be nil.
func (i *TreeIter) Next() (*Node, error) {
	out := i.previous
	err := i.read()
	return out, err
}

// Close will close the internal reader.
// If Close() is called before exausting the iterator,
// these pending objects won't be accesible anymore.
func (i *TreeIter) Close() error {
	return i.reader.Close()
}
