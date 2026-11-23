package app

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/pion/webrtc/v4"
)

func offerSDP(t *testing.T, api *webrtc.API) string {
	t.Helper()
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	defer func() { _ = pc.Close() }()
	if _, err := pc.CreateDataChannel("pcm", nil); err != nil {
		t.Fatalf("CreateDataChannel: %v", err)
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	done := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("SetLocalDescription: %v", err)
	}
	<-done
	return pc.LocalDescription().SDP
}

type fakeProbe struct {
	mu  sync.Mutex
	ip  string
	err error
}

func (f *fakeProbe) set(ip string, err error) {
	f.mu.Lock()
	f.ip, f.err = ip, err
	f.mu.Unlock()
}

func (f *fakeProbe) probe(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ip, f.err
}

// WACALLS_PUBLIC_IP=auto: internet residencial troca de IP (QA real, 2026-10-02:
// o WaCalls anunciava o IP antigo e quem estava fora da rede não atendia). O IP
// anunciado ao navegador vem do STUN e acompanha a troca sem reiniciar.
func TestAutoPublicIPFollowsSTUN(t *testing.T) {
	port := freeUDPPort(t)
	p := &fakeProbe{ip: "203.0.113.10"}

	b, err := newBrowserAPI(context.Background(), port, []string{"auto"}, p.probe, nil)
	if err != nil {
		t.Fatalf("newBrowserAPI: %v", err)
	}
	if sdp := offerSDP(t, b.Get()); !strings.Contains(sdp, "203.0.113.10") {
		t.Fatalf("SDP deveria anunciar o IP do STUN:\n%s", sdp)
	}

	p.set("203.0.113.20", nil)
	if changed, err := b.Refresh(context.Background()); err != nil || !changed {
		t.Fatalf("Refresh = (%v, %v), quero (true, nil)", changed, err)
	}
	sdp := offerSDP(t, b.Get())
	if !strings.Contains(sdp, "203.0.113.20") || strings.Contains(sdp, "203.0.113.10") {
		t.Fatalf("SDP deveria anunciar só o IP novo:\n%s", sdp)
	}

	// STUN fora do ar: mantém o último IP bom em vez de voltar a anunciar a LAN.
	p.set("", errors.New("stun down"))
	if _, err := b.Refresh(context.Background()); err == nil {
		t.Fatal("Refresh deveria devolver o erro do STUN")
	}
	if sdp := offerSDP(t, b.Get()); !strings.Contains(sdp, "203.0.113.20") {
		t.Fatalf("falha do STUN não pode apagar o IP bom:\n%s", sdp)
	}
}

// IP fixo continua fixo: nada de STUN.
func TestFixedPublicIPDoesNotProbe(t *testing.T) {
	port := freeUDPPort(t)
	called := false
	probe := func(context.Context) (string, error) { called = true; return "198.51.100.1", nil }

	b, err := newBrowserAPI(context.Background(), port, []string{"203.0.113.30"}, probe, nil)
	if err != nil {
		t.Fatalf("newBrowserAPI: %v", err)
	}
	if sdp := offerSDP(t, b.Get()); !strings.Contains(sdp, "203.0.113.30") {
		t.Fatalf("SDP deveria anunciar o IP fixo:\n%s", sdp)
	}
	if called {
		t.Fatal("IP fixo não deveria consultar o STUN")
	}
}
