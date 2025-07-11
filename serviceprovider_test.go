package netdicom

import (
	"testing"
	"time"

	"github.com/grailbio/go-dicom/dicomlog"
	"github.com/grailbio/go-netdicom/dimse"
	"github.com/grailbio/go-netdicom/sopclass"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ServiceProviderClose(t *testing.T) {
	dicomlog.SetLevel(0)
	// setup 1: make a service provider
	sp, err := NewServiceProvider(ServiceProviderParams{
		CEcho: func(conn ConnectionState) dimse.Status {
			return dimse.Status{Status: dimse.StatusSuccess}
		},
	}, "127.0.0.1:")
	require.NoError(t, err)
	t.Logf("service provider is listening on addr: %s", sp.ListenAddr().String())

	done := make(chan struct{})
	go func() {
		defer close(done)

		tErr := sp.Run()
		assert.ErrorIs(t, tErr, ErrServerClosed)
	}()

	// setup 2: make a service user
	su, err := NewServiceUser(ServiceUserParams{SOPClasses: sopclass.VerificationClasses})
	require.NoError(t, err)
	require.NoError(t, su.Connect(sp.ListenAddr().String()))
	defer su.Release()
	require.NoError(t, su.CEcho())
	// echo

	// action
	require.NoError(t, sp.Close())
	select {
	case <-done:
		t.Log("service provider is closed")
	case <-time.After(time.Second * 10):
		t.Fatal("timeout")
	}

	// assert: c-echo again
	require.Error(t, su.CEcho())
}
