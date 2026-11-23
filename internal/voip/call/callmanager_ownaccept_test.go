package call

import (
	"context"
	"log/slog"
	"testing"

	"wacalls/internal/voip/core"

	"go.mau.fi/whatsmeow/types"
)

// ownSock is a signaling socket whose own account is known, so the manager can
// tell an accept from another device of the SAME account (the phone) apart from
// an accept by the remote party.
type ownSock struct {
	recordSock
	lid types.JID
	pn  types.JID
}

func (s *ownSock) OwnLID() types.JID { return s.lid }
func (s *ownSock) OwnPN() types.JID  { return s.pn }

func incomingRingingManager(t *testing.T, sock *ownSock) (*CallManager, *CallInfo, *[]*CallInfo) {
	t.Helper()
	cm := NewCallManager(sock, slog.Default())
	cm.relay = &fakeRelay{noConn: true}
	call := NewIncomingCall("CID", "194111097327706@lid", "194111097327706@lid", "", core.CallMediaTypeAudio)
	cm.currentCall = call
	var ended []*CallInfo
	cm.OnEnded = func(c *CallInfo) { ended = append(ended, c) }
	t.Cleanup(cm.cleanupMedia)
	return cm, call, &ended
}

// QA real (2026-10-02): o celular do próprio número atendeu a chamada recebida,
// o WaCalls tratou o <accept> do próprio LID como "o outro lado atendeu", entrou
// nos relays e ignorou o terminate do chamador — o CRM ficou tocando até o
// timeout de 60 s. Aceite de outro aparelho da conta = "atendida em outro lugar".
func TestAcceptFromOwnDeviceEndsIncomingCallAsAcceptedElsewhere(t *testing.T) {
	for _, tc := range []struct {
		name string
		from types.JID
	}{
		{"lid do celular", lidDevice("65777575153913", 0)},
		{"pn do celular", types.NewJID("556299321371", types.DefaultUserServer)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sock := &ownSock{
				lid: lidDevice("65777575153913", 40),
				pn:  types.JID{User: "556299321371", Server: types.DefaultUserServer, Device: 40},
			}
			cm, call, ended := incomingRingingManager(t, sock)

			cm.HandleCallAccept(context.Background(), acceptNode("CID", tc.from), tc.from)

			if !call.IsEnded() {
				t.Fatalf("chamada deveria terminar, estado %s", call.StateData.State)
			}
			if call.StateData.EndReason != core.EndCallReasonAcceptedElsewhere {
				t.Fatalf("razão = %q, quero accepted_elsewhere", call.StateData.EndReason)
			}
			if len(*ended) != 1 {
				t.Fatalf("OnEnded disparou %d vezes, quero 1", len(*ended))
			}
			cm.mu.Lock()
			acceptedBy := cm.acceptedByJid
			cm.mu.Unlock()
			if acceptedBy != "" {
				t.Fatalf("aceite do próprio aparelho não pode virar o atendente da chamada: %s", acceptedBy)
			}
		})
	}
}

// O aceite do contato numa chamada que NÓS fizemos continua sendo "o outro lado
// atendeu" (regressão do caminho de saída).
func TestAcceptFromRemoteStillAnswersOutgoingCall(t *testing.T) {
	sock := &elsewhereSock{devices: []types.JID{lidDevice("62440234549366", 0)}}
	cm := outgoingRingingManager(t, sock)
	peer := lidDevice("62440234549366", 0)

	cm.HandleCallAccept(context.Background(), acceptNode("CALL1", peer), peer)

	cm.mu.Lock()
	call := cm.currentCall
	cm.mu.Unlock()
	if call == nil || call.IsEnded() {
		t.Fatal("chamada de saída atendida pelo contato não pode terminar")
	}
}
