package main

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-core/host"
	"strings"
	"time"

	"github.com/testground/sdk-go/network"
	"github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"

	"github.com/dustin/go-humanize"
	"github.com/libp2p/go-libp2p"
	p2pnet "github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	mplex "github.com/libp2p/go-libp2p-mplex"
	noise "github.com/libp2p/go-libp2p-noise"
	quic "github.com/libp2p/go-libp2p-quic-transport"
	tls "github.com/libp2p/go-libp2p-tls"
	yamux "github.com/libp2p/go-libp2p-yamux"
	"github.com/libp2p/go-tcp-transport"
	ws "github.com/libp2p/go-ws-transport"
	"github.com/multiformats/go-multiaddr"
)

type PeerMap struct {
	ID    peer.ID
	Addrs []string
	Seq   int64
}

type Libp2pTestInfo struct {
	h     host.Host
	seq   int64
	peers map[int64]peer.AddrInfo
}

func libp2pTest(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	ti, err := setupBaseLibp2pTest(ctx, runenv, initCtx)
	if err != nil {
		return err
	}

	proto := protocol.ID("/test-tcp-transfer/0.0.1")

	protocolRegistered := sync.State("protocol-registered")
	switch ti.seq {
	case 1:
		file, err := GetCARFileFromConfig(ctx, runenv)
		if err != nil {
			return err
		}

		ti.h.SetStreamHandler(proto, func(stream p2pnet.Stream) {
			dur, err := serverProtocol(runenv, stream, file)
			if err != nil {
				runenv.RecordMessage("Server failure: %v", err)
				return
			}
			runenv.RecordMessage("Server finished: %s", dur)
		})

		initCtx.SyncClient.MustSignalAndWait(ctx, protocolRegistered, runenv.TestInstanceCount)
	case 2:
		initCtx.SyncClient.MustSignalAndWait(ctx, protocolRegistered, runenv.TestInstanceCount)

		ai1 := ti.peers[1]
		if err := ti.h.Connect(ctx, ai1); err != nil {
			return err
		}
		stream, err := ti.h.NewStream(ctx, ai1.ID, proto)
		if err != nil {
			return err
		}
		dur, err := clientProtocol(runenv, stream)
		if err != nil {
			return err
		}
		runenv.RecordMessage("Client finished: %s", dur)
	default:
		return fmt.Errorf("expected at most two test instances")
	}

	state := sync.State("test-done")
	initCtx.SyncClient.MustSignalAndWait(ctx, state, runenv.TestInstanceCount)

	return nil
}

func setupBaseLibp2pTest(ctx context.Context, runenv *runtime.RunEnv, initCtx *run.InitContext) (*Libp2pTestInfo, error) {
	runenv.RecordMessage("before sync.MustBoundClient")
	client := initCtx.SyncClient
	netclient := initCtx.NetClient

	ip := netclient.MustGetDataNetworkIP()

	latStr := runenv.StringParam("latency")
	latency, err := time.ParseDuration(latStr)
	if err != nil {
		return nil, err
	}
	bwStr := runenv.StringParam("bandwidth")
	bandwidth, err := humanize.ParseBytes(bwStr)
	if err != nil {
		return nil, err
	}

	runenv.RecordMessage("latency is set at %v", latency)
	runenv.RecordMessage("bandwidth is set at %v", humanize.Bytes(bandwidth))

	config := &network.Config{
		// Control the "default" network. At the moment, this is the only network.
		Network: "default",

		// Enable this network. Setting this to false will disconnect this test
		// instance from this network. You probably don't want to do that.
		Enable: true,
		Default: network.LinkShape{
			Latency:   latency,
			Bandwidth: bandwidth * 8,
		},
		CallbackState: "network-configured",
		RoutingPolicy: network.DenyAll,
	}

	runenv.RecordMessage("before netclient.MustConfigureNetwork")
	netclient.MustConfigureNetwork(ctx, config)

	seq := client.MustSignalAndWait(ctx, "ip-allocation", runenv.TestInstanceCount)

	var libp2pOpts []libp2p.Option
	var listenStrings []string
	for _, t := range strings.Split(runenv.StringParam("transports"), ",") {
		switch t {
		case "tcp":
			listenStrings = append(listenStrings, fmt.Sprintf("/ip4/%s/tcp/0", ip))
			libp2pOpts = append(libp2pOpts, libp2p.Transport(tcp.NewTCPTransport))
		case "quic":
			listenStrings = append(listenStrings, fmt.Sprintf("/ip4/%s/udp/0/quic", ip))
			libp2pOpts = append(libp2pOpts, libp2p.Transport(quic.NewTransport))
		case "ws":
			listenStrings = append(listenStrings, fmt.Sprintf("/ip4/%s/tcp/0/ws", ip))
			libp2pOpts = append(libp2pOpts, libp2p.Transport(ws.New))
		}
	}
	libp2pOpts = append(libp2pOpts, libp2p.ListenAddrStrings(listenStrings...))

	for _, s := range strings.Split(runenv.StringParam("security"), ",") {
		switch s {
		case "tls":
			libp2pOpts = append(libp2pOpts, libp2p.Security(tls.ID, tls.New))
		case "noise":
			libp2pOpts = append(libp2pOpts, libp2p.Security(noise.ID, noise.New))
		}
	}

	for _, m := range strings.Split(runenv.StringParam("muxers"), ",") {
		switch m {
		case "yamux":
			libp2pOpts = append(libp2pOpts, libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport))
		case "mplex":
			libp2pOpts = append(libp2pOpts, libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport))
		}
	}

	h, err := libp2p.New(libp2pOpts...)
	if err != nil {
		return nil, err
	}

	runenv.RecordMessage("I am %d, peerID %v", seq, h.ID())

	// Obtain our own address info, and use the sync service to publish it to a
	// 'peersTopic' topic, where others will read from.
	var (
		ai = &peer.AddrInfo{ID: h.ID(), Addrs: h.Addrs()}

		// the peers topic where all instances will advertise their AddrInfo.
		peersTopic = sync.NewTopic("peers", new(PeerMap))

		// initialize a slice to store the AddrInfos of all other peers in the run.
		peers = make(map[int64]peer.AddrInfo, runenv.TestInstanceCount)
	)

	// Publish our own.
	stringAddrs := make([]string, 0, len(ai.Addrs))
	for _, addr := range ai.Addrs {
		stringAddrs = append(stringAddrs, addr.String())
	}
	initCtx.SyncClient.MustPublish(ctx, peersTopic, &PeerMap{Seq: seq, ID: ai.ID, Addrs: stringAddrs})

	// Now subscribe to the peers topic and consume all addresses, storing them
	// in the peers slice.
	peersCh := make(chan *PeerMap)
	sctx, scancel := context.WithCancel(ctx)
	sub := initCtx.SyncClient.MustSubscribe(sctx, peersTopic, peersCh)

	// Receive the expected number of AddrInfos.
	for len(peers) < runenv.TestInstanceCount {
		select {
		case pm := <-peersCh:
			runenv.RecordMessage("received %+v", pm)
			addrs := make([]multiaddr.Multiaddr, 0, len(pm.Addrs))
			for _, addr := range pm.Addrs {
				runenv.RecordMessage("received %s", addr)
				ma, err := multiaddr.NewMultiaddr(addr)
				if err != nil {
					scancel()
					return nil, err
				}
				addrs = append(addrs, ma)
			}
			peers[pm.Seq] = peer.AddrInfo{
				ID:    pm.ID,
				Addrs: addrs,
			}
		case err := <-sub.Done():
			scancel()
			return nil, err
		}
	}
	scancel() // cancels the Subscription.

	runenv.RecordMessage("before reconfiguring network")
	netclient.MustConfigureNetwork(ctx, config)

	return &Libp2pTestInfo{
		h:     h,
		seq:   seq,
		peers: peers,
	}, nil
}
