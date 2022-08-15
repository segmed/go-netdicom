package main

import (
	"net"
	"testing"
	"time"

	"github.com/grailbio/go-netdicom"
	"github.com/grailbio/go-netdicom/sopclass"
	"github.com/stretchr/testify/require"
)

func TestClient_Echo(t *testing.T) {
	var port = MustAllocateFreePort()
	go func() {
		runSCP(net.JoinHostPort("localhost", port), "", true, nil, nil)
	}()
	waitForReply(t, port)
}

// waitForReply sends a C-ECHO command and waits until the server responses or failed.
func waitForReply(t testing.TB, port string) {
	const (
		maxRetries    = 10
		sleepDuration = time.Millisecond * 1000
	)
	params := netdicom.ServiceUserParams{MaxPDUSize: 0, SOPClasses: sopclass.VerificationClasses}
	su, err := netdicom.NewServiceUser(params)
	require.NoError(t, err)
	su.Connect(net.JoinHostPort("localhost", port))
	defer su.Release()

	for i := 0; i < maxRetries; i++ {
		if err := su.CEcho(); err == nil {
			break
		}
		time.Sleep(sleepDuration)
	}
	require.NoError(t, su.CEcho())
}

// MustAllocateFreePort allocates a free port by trying to acquire a random port and immediately release it.
func MustAllocateFreePort() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	if err = l.Close(); err != nil {
		panic(err)
	}
	_, portStr, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		panic(err)
	}
	return portStr
}
