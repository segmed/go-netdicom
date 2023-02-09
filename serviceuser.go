package netdicom

// This file implements the ServiceUser (i.e., a DICOM DIMSE client) class.

//go:generate stringer -type QRLevel

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/grailbio/go-dicom"
	"github.com/grailbio/go-dicom/dicomio"
	"github.com/grailbio/go-dicom/dicomlog"
	"github.com/grailbio/go-dicom/dicomtag"
	"github.com/grailbio/go-dicom/dicomuid"

	"github.com/grailbio/go-netdicom/dimse"
)

type serviceUserStatus int

const (
	serviceUserInitial = iota
	serviceUserAssociationActive
	serviceUserClosed
)

// ServiceUser encapsulates implements the client side of DICOM network protocol.
//
//  user, err := netdicom.NewServiceUser(netdicom.ServiceUserParams{SOPClasses: sopclass.QRFindClasses})
//  // Connect to server 1.2.3.4, port 8888
//  user.Connect("1.2.3.4:8888")
//  // Send test.dcm to the server
//  ds, err := dicom.ReadDataSetFromFile("test.dcm", dicom.ReadOptions{})
//  err := user.CStore(ds)
//  // Disconnect
//  user.Release()
//
// The ServiceUser class is thread compatible. That is, you cannot call C*
// methods - say CStore and CFind requests - concurrently from two goroutines.
// You must wait for CStore to finish before issuing CFind.
type ServiceUser struct {
	label    string // For  logging
	upcallCh chan upcallEvent

	mu   *sync.Mutex
	cond *sync.Cond // Broadcast when status changes.
	disp *serviceDispatcher

	// Following fields are guarded by mu.
	status serviceUserStatus
	cm     *contextManager // Set only after the handshake completes.
	// activeCommands map[uint16]*userCommandState // List of commands running
}

// ServiceUserParams defines parameters for a ServiceUser.
type ServiceUserParams struct {
	// Application-entity title of the peer. If empty, set to "unknown-called-ae"
	CalledAETitle string
	// Application-entity title of the client. If empty, set to
	// "unknown-calling-ae"
	CallingAETitle string

	// List of SOPUIDs wanted by the client. The value is typically one of
	// the constants listed in sopclass package.
	SOPClasses []string

	// List of Transfer syntaxes supported by the user.  If you know the
	// transer syntax of the file you are going to copy, set that here.
	// Otherwise, you'll need to re-encode the data w/ the given transfer
	// syntax yourself.
	//
	// TODO(saito) Support reencoding internally on C_STORE, etc. The DICOM
	// spec is particularly moronic here, since we could just have specified
	// the transfer syntax per data sent.
	TransferSyntaxes []string

	// Maximum PDU receive size (in bytes)
	MaxPDUSize uint32
}

func validateServiceUserParams(params *ServiceUserParams) error {
	if params.CalledAETitle == "" {
		params.CalledAETitle = "unknown-called-ae"
	}
	if params.CallingAETitle == "" {
		params.CallingAETitle = "unknown-calling-ae"
	}
	if len(params.SOPClasses) == 0 {
		return fmt.Errorf("Empty ServiceUserParams.SOPClasses")
	}
	if len(params.TransferSyntaxes) == 0 {
		params.TransferSyntaxes = dicomio.StandardTransferSyntaxes
	} else {
		for i, uid := range params.TransferSyntaxes {
			canonicalUID, err := dicomio.CanonicalTransferSyntaxUID(uid)
			if err != nil {
				return err
			}
			params.TransferSyntaxes[i] = canonicalUID
		}
	}
	return nil
}

// NewServiceUser creates a new ServiceUser. The caller must call either
// Connect() or SetConn() before calling any other method, such as Cstore.
func NewServiceUser(params ServiceUserParams) (*ServiceUser, error) {
	if err := validateServiceUserParams(&params); err != nil {
		return nil, err
	}
	mu := &sync.Mutex{}
	label := newUID("user")
	su := &ServiceUser{
		label:    label,
		upcallCh: make(chan upcallEvent, 128),
		disp:     newServiceDispatcher(label),
		mu:       mu,
		cond:     sync.NewCond(mu),
		status:   serviceUserInitial,
	}
	go runStateMachineForServiceUser(params, su.upcallCh, su.disp.downcallCh, label)
	go func() {
		for event := range su.upcallCh {
			if event.eventType == upcallEventHandshakeCompleted {
				su.mu.Lock()
				doassert(su.cm == nil)
				su.status = serviceUserAssociationActive
				su.cond.Broadcast()
				su.cm = event.cm
				doassert(su.cm != nil)
				su.mu.Unlock()
				continue
			}
			doassert(event.eventType == upcallEventData)
			su.disp.handleEvent(event)
		}
		dicomlog.Vprintf(1, "dicom.serviceUser: dispatcher finished")
		su.disp.close()
		su.mu.Lock()
		su.cond.Broadcast()
		su.status = serviceUserClosed
		su.mu.Unlock()
	}()
	return su, nil
}

func (su *ServiceUser) waitUntilReady() error {
	su.mu.Lock()
	defer su.mu.Unlock()
	for su.status <= serviceUserInitial {
		su.cond.Wait()
	}
	if su.status != serviceUserAssociationActive {
		// Will get an error when waiting for a response.
		dicomlog.Vprintf(0, "dicom.serviceUser: Connection failed")
		return fmt.Errorf("dicom.serviceUser: Connection failed")
	}
	return nil
}

// ConnectContext connects to the server at the given "host:port". Either Connect or
// SetConn must be before calling CStore, etc.
func (su *ServiceUser) ConnectContext(ctx context.Context, serverAddr string) error {
	if su.status != serviceUserInitial {
		panic(fmt.Sprintf("dicom.serviceUser: Connect called with wrong state: %v", su.status))
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", serverAddr)
	if err != nil {
		dicomlog.Vprintf(0, "dicom.serviceUser: Connect(%s): %v", serverAddr, err)
		su.disp.downcallCh <- stateEvent{event: evt17, pdu: nil, err: err}
		return fmt.Errorf("net.DialTimeout: %w", err)
	} else {
		su.disp.downcallCh <- stateEvent{event: evt02, pdu: nil, err: nil, conn: conn}
	}
	return nil
}

// Connect connects to the server at the given "host:port". Either Connect or
// SetConn must be before calling CStore, etc.
func (su *ServiceUser) Connect(serverAddr string) error {
	return su.ConnectContext(context.Background(), serverAddr)
}

// SetConn instructs ServiceUser to use the given network connection to talk to
// the server. Either Connect or SetConn must be before calling CStore, etc.
func (su *ServiceUser) SetConn(conn net.Conn) {
	doassert(su.status == serviceUserInitial)
	su.disp.downcallCh <- stateEvent{event: evt02, pdu: nil, err: nil, conn: conn}
}

// CEcho send a C-ECHO request to the remote AE and waits for a
// response. Returns nil iff the remote AE responds ok.
func (su *ServiceUser) CEcho() error {
	err := su.waitUntilReady()
	if err != nil {
		return err
	}
	context, err := su.cm.lookupByAbstractSyntaxUID(dicomuid.VerificationSOPClass)
	if err != nil {
		return err
	}
	cs, err := su.disp.newCommand(su.cm, context)
	if err != nil {
		return err
	}
	defer su.disp.deleteCommand(cs)

	cs.sendMessage(
		&dimse.CEchoRq{
			AffectedSOPClassUID: context.abstractSyntaxUID,
			MessageID:           cs.messageID,
			CommandDataSetType:  dimse.CommandDataSetTypeNull,
		}, nil)
	event, ok := <-cs.upcallCh
	if !ok {
		return fmt.Errorf("Failed to receive C-ECHO response")
	}
	resp, ok := event.command.(*dimse.CEchoRsp)
	if !ok {
		return fmt.Errorf("Invalid response for C-ECHO: %v", event.command)
	}
	if resp.Status.Status != dimse.StatusSuccess {
		err = fmt.Errorf("Non-OK status in C-ECHO response: %+v", resp.Status)
	}
	return err
}

// CStore issues a C-STORE request to transfer "ds" in remove peer.  It blocks
// until the operation finishes.
//
// REQUIRES: Connect() or SetConn has been called.
func (su *ServiceUser) CStore(ds *dicom.DataSet, opts ...dicom.WriteOption) error {
	optsSet := &dicom.WriteOptSet{}
	for _, opt := range opts {
		opt(optsSet)
	}
	err := su.waitUntilReady()
	if err != nil {
		return err
	}
	doassert(su.cm != nil)

	var sopClassUID string
	if sopClassUIDElem, err := ds.FindElementByTag(dicomtag.MediaStorageSOPClassUID); err != nil {
		return err
	} else if sopClassUID, err = sopClassUIDElem.GetString(); err != nil {
		return err
	}
	context, err := su.cm.lookupByAbstractSyntaxUID(sopClassUID)
	if err != nil {
		return err
	}
	cs, err := su.disp.newCommand(su.cm, context)
	if err != nil {
		return err
	}
	if err != nil {
		dicomlog.Vprintf(0, "dicom.serviceUser: C-STORE: sop class %v not found in context %v", sopClassUID, err)
		return err
	}
	defer su.disp.deleteCommand(cs)
	return runCStoreOnAssociation(cs.upcallCh, su.disp.downcallCh, su.cm, cs.messageID, ds, optsSet)
}

// QRLevel is used to specify the element hierarchy assumed during C-FIND,
// C-GET, and C-MOVE. P3.4, C.3.
//
// http://dicom.nema.org/Dicom/2013/output/chtml/part04/sect_C.3.html
type QRLevel int
type qrOpType int

const (
	// QRLevelPatient chooses Patient-Root QR model.  P3.4, C.3.1
	QRLevelPatient QRLevel = iota

	// QRLevelStudy chooses Study-Root QR model.  P3.4, C.3.2
	QRLevelStudy

	// QRLevelSeries chooses Study-Root QR model, but using "SERIES" QueryRetrieveLevel.  P3.4, C.3.2
	QRLevelSeries

	qrOpCFind qrOpType = iota
	qrOpCGet
	qrOpCMove
)

// CFindResult is an object streamed by CFind method.
type CFindResult struct {
	// Exactly one of Err or Elements is set.
	Err      error
	Elements []*dicom.Element // Elements belonging to one dataset.
}

func encodeQRPayload(opType qrOpType, qrLevel QRLevel, filter []*dicom.Element, cm *contextManager) (contextManagerEntry, []byte, error) {
	var sopClassUID string
	var qrLevelString string
	switch qrLevel {
	case QRLevelPatient:
		switch opType {
		case qrOpCFind:
			sopClassUID = dicomuid.PatientRootQRFind
		case qrOpCGet:
			sopClassUID = dicomuid.PatientRootQRGet
		case qrOpCMove:
			sopClassUID = dicomuid.PatientRootQRMove
		}
		qrLevelString = "PATIENT"
	case QRLevelStudy, QRLevelSeries:
		switch opType {
		case qrOpCFind:
			sopClassUID = dicomuid.StudyRootQRFind
		case qrOpCGet:
			sopClassUID = dicomuid.StudyRootQRGet
		case qrOpCMove:
			sopClassUID = dicomuid.StudyRootQRMove
		}
		qrLevelString = "STUDY"
		if qrLevel == QRLevelSeries {
			qrLevelString = "SERIES"
		}
	default:
		return contextManagerEntry{}, nil, fmt.Errorf("Invalid C-FIND QR lever: %d", qrLevel)
	}

	// Translate qrLevel to the sopclass and QRLevel elem.
	// Encode the C-FIND DIMSE command.
	context, err := cm.lookupByAbstractSyntaxUID(sopClassUID)
	if err != nil {
		// This happens when the user passed a wrong sopclass list in
		// A-ASSOCIATE handshake.
		return context, nil, err
	}

	// Encode the data payload containing the filtering conditions.
	dataEncoder := dicomio.NewBytesEncoderWithTransferSyntax(context.transferSyntaxUID)
	foundQRLevel := false
	for _, elem := range filter {
		if elem.Tag == dicomtag.QueryRetrieveLevel {
			foundQRLevel = true
		}
		dicom.WriteElement(dataEncoder, elem, &dicom.WriteOptSet{})
		dicomlog.Vprintf(2, "dicom.serviceUser: Add QR payload: %v", elem)
	}
	if !foundQRLevel {
		elem := dicom.MustNewElement(dicomtag.QueryRetrieveLevel, qrLevelString)
		dicomlog.Vprintf(2, "dicom.serviceUser: Add QR payload: %v", elem)
		dicom.WriteElement(dataEncoder, elem, &dicom.WriteOptSet{})
	}
	if err := dataEncoder.Error(); err != nil {
		return context, nil, err
	}
	return context, dataEncoder.Bytes(), err
}

// CFind issues a C-FIND request. Returns a channel that streams sequence of
// either an error or a dataset found. The caller MUST read all responses from
// the channel before issuing any other DIMSE command (C-FIND, C-STORE, etc).
//
// The param sopClassUID is one of the UIDs defined in sopclass.QRFindClasses.
// filter is the list of elements to match and retrieve.
//
// REQUIRES: Connect() or SetConn has been called.
func (su *ServiceUser) CFind(qrLevel QRLevel, filter []*dicom.Element) chan CFindResult {
	ch := make(chan CFindResult, 128)
	err := su.waitUntilReady()
	if err != nil {
		ch <- CFindResult{Err: err}
		close(ch)
		return ch
	}
	context, payload, err := encodeQRPayload(qrOpCFind, qrLevel, filter, su.cm)
	if err != nil {
		ch <- CFindResult{Err: err}
		close(ch)
		return ch
	}
	cs, err := su.disp.newCommand(su.cm, context)
	if err != nil {
		ch <- CFindResult{Err: err}
		close(ch)
		return ch
	}
	go func() {
		defer close(ch)
		defer su.disp.deleteCommand(cs)
		cs.sendMessage(
			&dimse.CFindRq{
				AffectedSOPClassUID: context.abstractSyntaxUID,
				MessageID:           cs.messageID,
				CommandDataSetType:  dimse.CommandDataSetTypeNonNull,
			},
			payload)
	receiveLoop:
		for {
			event, ok := <-cs.upcallCh
			if !ok {
				su.status = serviceUserClosed
				ch <- CFindResult{Err: fmt.Errorf("connection closed while waiting for C-FIND response")}
				break
			}
			doassert(event.eventType == upcallEventData)
			doassert(event.command != nil)
			resp, ok := event.command.(*dimse.CFindRsp)
			if !ok {
				ch <- CFindResult{Err: fmt.Errorf("Found wrong response for C-FIND: %v", event.command)}
				break
			}
			var data []byte
			for bytes := range event.stream {
				data = append(data, bytes...)
			}
			elems, err := readElementsInBytes(data, context.transferSyntaxUID)
			if err != nil {
				dicomlog.Vprintf(0, "dicom.serviceUser: Failed to decode C-FIND response: %v %v", resp.String(), err)
				ch <- CFindResult{Err: err}
			}
			switch resp.Status.Status {
			case dimse.StatusPending:
				dicomlog.Vprintf(1, "dicom.serviceUser: C-FIND received pending response: %+v", elems)
				ch <- CFindResult{Elements: elems}
				continue receiveLoop
			case dimse.StatusSuccess:
				dicomlog.Vprintf(1, "dicom.serviceUser: C-FIND received response success: %+v", elems)
				ch <- CFindResult{Elements: elems}
				break receiveLoop
			default:
				e := fmt.Errorf("received C-MOVE error: %+v", resp)
				dicomlog.Vprintf(0, "dicom.serviceUser: C-FIND received response not success: %v", e)
				ch <- CFindResult{Err: e}
				break receiveLoop
			}
		}
	}()
	return ch
}

// CGet runs a C-GET command. It calls "cb" sequentially for every dataset
// received. "cb" should return dimse.Success iff the data was successfully and
// stably written. This function blocks until it receives all datasets from the
// server.
//
// The "data" arg to "cb" is the serialized dataset, encoded according to
// transferSyntaxUID.
//
// TODO(saito) We should parse the data into DataSet before passing to "cb".
func (su *ServiceUser) CGet(qrLevel QRLevel, filter []*dicom.Element,
	cb func(transferSyntaxUID, sopClassUID, sopInstanceUID string, data []byte) dimse.Status) error {
	err := su.waitUntilReady()
	if err != nil {
		return err
	}
	context, payload, err := encodeQRPayload(qrOpCGet, qrLevel, filter, su.cm)
	if err != nil {
		return err
	}
	cs, err := su.disp.newCommand(su.cm, context)
	if err != nil {
		return err
	}
	defer su.disp.deleteCommand(cs)

	handleCStore := func(msg dimse.Message, data []byte, cs *serviceCommandState) {
		c := msg.(*dimse.CStoreRq)
		status := cb(
			context.transferSyntaxUID,
			c.AffectedSOPClassUID,
			c.AffectedSOPInstanceUID,
			data)
		resp := &dimse.CStoreRsp{
			AffectedSOPClassUID:       c.AffectedSOPClassUID,
			MessageIDBeingRespondedTo: c.MessageID,
			CommandDataSetType:        dimse.CommandDataSetTypeNull,
			AffectedSOPInstanceUID:    c.AffectedSOPInstanceUID,
			Status:                    status,
		}
		cs.sendMessage(resp, nil)
	}
	su.disp.registerCallback(dimse.CommandFieldCStoreRq, handleCStore)
	defer su.disp.unregisterCallback(dimse.CommandFieldCStoreRq)
	cs.sendMessage(
		&dimse.CGetRq{
			AffectedSOPClassUID: context.abstractSyntaxUID,
			MessageID:           cs.messageID,
			CommandDataSetType:  dimse.CommandDataSetTypeNonNull,
		},
		payload)
receiveLoop:
	for {
		event, ok := <-cs.upcallCh
		if !ok {
			su.status = serviceUserClosed
			return fmt.Errorf("Connection closed while waiting for C-GET response")
		}
		doassert(event.eventType == upcallEventData)
		doassert(event.command != nil)
		resp, ok := event.command.(*dimse.CGetRsp)
		if !ok {
			return fmt.Errorf("found wrong response for C-GET: %v", event.command)
		}
		switch resp.Status.Status {
		case dimse.StatusPending:
			dicomlog.Vprintf(1, "dicom.serviceUser: C-GET received pending response")
			continue receiveLoop
		case dimse.StatusSuccess:
			dicomlog.Vprintf(1, "dicom.serviceUser: C-GET received success response")
			break receiveLoop
		default:
			e := fmt.Errorf("received C-GET error: %+v", resp)
			dicomlog.Vprintf(0, "dicom.serviceUser: C-GET: %v", e)
			return e
		}
	}
	return nil
}

func (su *ServiceUser) CMove(moveDestination string, qrLevel QRLevel, filter []*dicom.Element) error {
	err := su.waitUntilReady()
	if err != nil {
		return err
	}
	context, payload, err := encodeQRPayload(qrOpCMove, qrLevel, filter, su.cm)
	if err != nil {
		return err
	}
	cs, err := su.disp.newCommand(su.cm, context)
	if err != nil {
		return err
	}
	defer su.disp.deleteCommand(cs)

	defer su.disp.unregisterCallback(dimse.CommandFieldCStoreRq)
	cs.sendMessage(
		&dimse.CMoveRq{
			AffectedSOPClassUID: context.abstractSyntaxUID,
			MessageID:           cs.messageID,
			CommandDataSetType:  dimse.CommandDataSetTypeNonNull,
			MoveDestination:     moveDestination,
		},
		payload)
receiveLoop:
	for {
		event, ok := <-cs.upcallCh
		if !ok {
			su.status = serviceUserClosed
			return fmt.Errorf("connection closed while waiting for C-MOVE response")
		}
		doassert(event.eventType == upcallEventData)
		doassert(event.command != nil)
		resp, ok := event.command.(*dimse.CMoveRsp)
		if !ok {
			return fmt.Errorf("found wrong response for C-MOVE: %v", event.command)
		}
		switch resp.Status.Status {
		case dimse.StatusPending:
			dicomlog.Vprintf(1, "dicom.serviceUser: C-MOVE received pending response")
			continue receiveLoop
		case dimse.StatusSuccess:
			dicomlog.Vprintf(1, "dicom.serviceUser: C-MOVE received response success: %+v", resp)
			break receiveLoop
		default:
			e := fmt.Errorf("received C-MOVE error: %+v", resp)
			dicomlog.Vprintf(0, "dicom.serviceUser: C-MOVE received response not success: %v", e)
			return e
		}
	}
	return nil
}

// Release shuts down the connection. It must be called exactly once.  After
// Release(), no other operation can be performed on the ServiceUser object.
func (su *ServiceUser) Release() {
	su.disp.downcallCh <- stateEvent{event: evt11}
	su.mu.Lock()
	defer su.mu.Unlock()
	su.status = serviceUserClosed
	su.cond.Broadcast()
	su.disp.close()
}
