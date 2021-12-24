package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/multiformats/go-varint"
	"github.com/testground/sdk-go/network"
	"github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"
	"io"
	"math/rand"
	"net"
	"os"
	"time"
)

func tcptest(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	runenv.RecordMessage("before sync.MustBoundClient")
	client := initCtx.SyncClient
	netclient := initCtx.NetClient

	oldAddrs, err := net.InterfaceAddrs()
	if err != nil {
		return err
	}

	latStr := runenv.StringParam("latency")
	latency, err := time.ParseDuration(latStr)
	if err != nil {
		return err
	}
	bwStr := runenv.StringParam("bandwidth")
	bandwidth, err := humanize.ParseBytes(bwStr)
	if err != nil {
		return err
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
			Bandwidth: bandwidth,
		},
		CallbackState: "network-configured",
		RoutingPolicy: network.DenyAll,
	}

	runenv.RecordMessage("before netclient.MustConfigureNetwork")
	netclient.MustConfigureNetwork(ctx, config)

	seq := client.MustSignalAndWait(ctx, "ip-allocation", runenv.TestInstanceCount)

	// Make sure that the IP addresses don't change unless we request it.
	if newAddrs, err := net.InterfaceAddrs(); err != nil {
		return err
	} else if !sameAddrs(oldAddrs, newAddrs) {
		return fmt.Errorf("interfaces changed")
	}

	runenv.RecordMessage("I am %d", seq)

	ipC := byte((seq >> 8) + 1)
	ipD := byte(seq)

	config.IPv4 = runenv.TestSubnet
	config.IPv4.IP = append(config.IPv4.IP[0:2:2], ipC, ipD)
	config.IPv4.Mask = []byte{255, 255, 255, 0}
	config.CallbackState = "ip-changed"

	var (
		listener *net.TCPListener
		conn     *net.TCPConn
	)

	fileSize, err := humanize.ParseBytes("10 MB")
	if err != nil {
		return err
	}

	if seq == 1 {
		listener, err = net.ListenTCP("tcp4", &net.TCPAddr{Port: 1234})
		if err != nil {
			return err
		}
		defer listener.Close()
	}

	runenv.RecordMessage("before reconfiguring network")
	netclient.MustConfigureNetwork(ctx, config)

	switch seq {
	case 1:
		conn, err = listener.AcceptTCP()
	case 2:
		conn, err = net.DialTCP("tcp4", nil, &net.TCPAddr{
			IP:   append(config.IPv4.IP[:3:3], 1),
			Port: 1234,
		})
	default:
		return fmt.Errorf("expected at most two test instances")
	}
	if err != nil {
		return err
	}

	defer conn.Close()

	// trying to measure latency here.
	/*err = conn.SetNoDelay(true)
	if err != nil {
		return err
	}*/

	switch seq {
	case 1:
		file, err := GenerateUnixFSCarFile(ctx, rand.New(rand.NewSource(0)), fileSize)
		if err != nil {
			return err
		}
		dur, err := serverProtocol(runenv, conn, file)
		if err != nil {
			return err
		}
		runenv.RecordMessage("Server finished: %s", dur)
	case 2:
		dur, err := clientProtocol(runenv, conn)
		if err != nil {
			return err
		}
		runenv.RecordMessage("Client finished: %s", dur)
	}

	state := sync.State("test-done")
	client.MustSignalAndWait(ctx, state, runenv.TestInstanceCount)

	return nil
}

func serverProtocol(runenv *runtime.RunEnv, protocolConn io.ReadWriter, file *os.File) (time.Duration, error) {
	buf := make([]byte, 1)

	runenv.RecordMessage("waiting until ready")

	// wait till both sides are ready
	_, err := protocolConn.Write([]byte{0})
	if err != nil {
		return 0, err
	}

	_, err = protocolConn.Read(buf)
	if err != nil {
		return 0, err
	}

	runenv.RecordMessage("start sending transfer")
	start := time.Now()
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}

	runenv.RecordMessage("start sending file of size %d", stat.Size())

	w := bufio.NewWriter(protocolConn)
	if _, err := w.Write(varint.ToUvarint(uint64(stat.Size()))); err != nil {
		return 0, err
	}

	if _, err := io.CopyN(w, file, stat.Size()); err != nil {
		return 0, err
	}
	if err := w.Flush(); err != nil {
		return 0, err
	}

	return time.Since(start), nil
}

func clientProtocol(runenv *runtime.RunEnv, protocolConn io.ReadWriter) (time.Duration, error) {
	buf := make([]byte, 1)

	runenv.RecordMessage("waiting until ready")

	// wait till both sides are ready
	_, err := protocolConn.Write([]byte{0})
	if err != nil {
		return 0, err
	}

	_, err = protocolConn.Read(buf)
	if err != nil {
		return 0, err
	}

	runenv.RecordMessage("start receiving transfer")
	start := time.Now()

	r := bufio.NewReader(protocolConn)
	length64, err := varint.ReadUvarint(r)
	if err != nil {
		return 0, err
	}
	length := int(length64)
	if length < 0 {
		return 0, io.ErrShortBuffer
	}

	runenv.RecordMessage("waiting to download file of size %d", length)

	buf = make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}

	return time.Since(start), nil
}

func sameAddrs(a, b []net.Addr) bool {
	if len(a) != len(b) {
		return false
	}
	aset := make(map[string]bool, len(a))
	for _, addr := range a {
		aset[addr.String()] = true
	}
	for _, addr := range b {
		if !aset[addr.String()] {
			return false
		}
	}
	return true
}
