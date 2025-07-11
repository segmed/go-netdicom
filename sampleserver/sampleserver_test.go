package main

import (
	"crypto/rand"
	"log"
	"net"
	"sync"
	"testing"

	godicom "github.com/grailbio/go-dicom"
	"github.com/grailbio/go-dicom/dicomtag"
	"github.com/grailbio/go-netdicom"
	"github.com/grailbio/go-netdicom/dimse"
	"github.com/grailbio/go-netdicom/sopclass"
	"github.com/stretchr/testify/require"
)

func runSCPTest(t testing.TB, port, aeTitle string, remoteAEs map[string]string, isSync bool) {
	ss := server{
		mu:       &sync.Mutex{},
		datasets: map[string]*godicom.DataSet{},
	}
	t.Logf("Listening on %s", port)

	params := netdicom.ServiceProviderParams{
		AETitle:   aeTitle,
		RemoteAEs: remoteAEs,
		CEcho: func(connState netdicom.ConnectionState) dimse.Status {
			log.Printf("Received C-ECHO")
			return dimse.Success
		},
	}
	if isSync {
		params.CStore = func(connState netdicom.ConnectionState, transferSyntaxUID string,
			sopClassUID string,
			sopInstanceUID string,
			data []byte) dimse.Status {
			return ss.onCStore(transferSyntaxUID, sopClassUID, sopInstanceUID, data)
		}
	} else {
		params.CStoreStream = func(connState netdicom.ConnectionState, transferSyntaxUID string,
			sopClassUID string,
			sopInstanceUID string,
			dataCh chan []byte) dimse.Status {
			for range dataCh {
			}
			return dimse.Success
		}
	}
	sp, err := netdicom.NewServiceProvider(params, port)
	require.NoError(t, err)
	require.NoError(t, sp.Run())
}

// newTestClient returns a dicom Client for testing
func newTestClient(t testing.TB, isSync bool) *netdicom.ServiceUser {
	var (
		port = MustAllocateFreePort()
		host = "localhost"
	)
	go func() {
		runSCPTest(t, net.JoinHostPort("localhost", port), "serverAE", map[string]string{
			"clientAE": net.JoinHostPort(host, port),
			"serverAE": net.JoinHostPort(host, port),
		}, isSync)
	}()
	waitForReply(t, port)
	params := netdicom.ServiceUserParams{SOPClasses: sopclass.VerificationClasses}
	client, err := netdicom.NewServiceUser(params)
	require.NoError(t, err)
	client.Connect(net.JoinHostPort("localhost", port))
	require.NoError(t, client.CEcho())
	return client
}

func newTestDataset(t testing.TB) *godicom.DataSet {
	pixelData := make([]byte, 100*1000*1000) // 100MB
	rand.Read(pixelData)
	var image godicom.PixelDataInfo
	image.Frames = append(image.Frames, pixelData)
	var tags = map[dicomtag.Tag]interface{}{
		dicomtag.MediaStorageSOPClassUID:    "1.2.840.10008.1.1",
		dicomtag.MediaStorageSOPInstanceUID: "1.2.826.0.1.3680043.2.1143.1590429688519720198888333603882344634",
		dicomtag.SOPInstanceUID:             "1.2.826.0.1.3680043.2.1143.1590429688519720198888333603882344634",
		dicomtag.StudyInstanceUID:           "1.2.826.0.1.3680043.2.1143.1590429688519720198888333603882344634",
		dicomtag.TransferSyntaxUID:          "1.2.840.10008.1.2",
		dicomtag.PixelData:                  image,
	}
	var elements []*godicom.Element
	for tag, val := range tags {
		elem, err := godicom.NewElement(tag, val)
		require.NoError(t, err)
		elements = append(elements, elem)
	}
	ds := godicom.DataSet{Elements: elements}
	return &ds
}

func BenchmarkCStoreSync(b *testing.B) {
	c := newTestClient(b, true)
	defer c.Release()

	ds := newTestDataset(b)
	for i := 0; i < b.N; i++ {
		require.NoError(b, c.CStore(ds))
	}
}

func BenchmarkCStoreStream(b *testing.B) {
	c := newTestClient(b, false)
	defer c.Release()

	ds := newTestDataset(b)
	for i := 0; i < b.N; i++ {
		c.CStore(ds)
	}
}
