package app

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"wacalls/internal/app/config"

	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
)

func buildBrowserAPI(udpPort int, externalIPs []string) (*webrtc.API, error) {
	if udpPort <= 0 {
		return webrtc.NewAPI(), nil
	}
	mux, err := newBrowserMux(udpPort)
	if err != nil {
		return nil, err
	}
	return apiWithMux(mux, externalIPs)
}

func newBrowserMux(udpPort int) (ice.UDPMux, error) {
	opts := []ice.UDPMuxFromPortOption{ice.UDPMuxFromPortWithNetworks(ice.NetworkTypeUDP4)}
	if iface := defaultRouteInterface(); iface != "" {
		opts = append(opts, ice.UDPMuxFromPortWithInterfaceFilter(func(name string) bool {
			return name == iface
		}))
	}
	return ice.NewMultiUDPMuxFromPort(udpPort, opts...)
}

func apiWithMux(mux ice.UDPMux, externalIPs []string) (*webrtc.API, error) {
	se := webrtc.SettingEngine{}
	se.SetICEUDPMux(mux)
	if len(externalIPs) > 0 {
		if err := se.SetICEAddressRewriteRules(webrtc.ICEAddressRewriteRule{
			External:        externalIPs,
			AsCandidateType: webrtc.ICECandidateTypeHost,
		}); err != nil {
			return nil, err
		}
	}
	return webrtc.NewAPI(webrtc.WithSettingEngine(se)), nil
}

// publicIPRefreshEvery: how often WACALLS_PUBLIC_IP=auto re-checks the public IP.
const publicIPRefreshEvery = 5 * time.Minute

// publicIPProbe returns the current public IPv4 (normally via STUN).
type publicIPProbe func(ctx context.Context) (string, error)

// browserAPI hands out the WebRTC API used for browser bridges. With
// WACALLS_PUBLIC_IP=auto the advertised address follows the public IP that STUN
// reports (home connections change IP); otherwise it is fixed at startup.
type browserAPI struct {
	mu    sync.RWMutex
	api   *webrtc.API
	ip    string
	mux   ice.UDPMux
	probe publicIPProbe
	log   *slog.Logger
}

func newBrowserAPI(ctx context.Context, udpPort int, externalIPs []string, probe publicIPProbe, log *slog.Logger) (*browserAPI, error) {
	if log == nil {
		log = slog.Default()
	}
	if !config.IsAutoPublicIP(externalIPs) || udpPort <= 0 {
		api, err := buildBrowserAPI(udpPort, externalIPs)
		if err != nil {
			return nil, err
		}
		return &browserAPI{api: api, log: log}, nil
	}
	mux, err := newBrowserMux(udpPort)
	if err != nil {
		return nil, err
	}
	api, err := apiWithMux(mux, nil)
	if err != nil {
		return nil, err
	}
	b := &browserAPI{api: api, mux: mux, probe: probe, log: log}
	if _, err := b.Refresh(ctx); err != nil {
		log.Warn("public ip auto-detection failed; advertising local addresses until it succeeds", "err", err)
	}
	return b, nil
}

// Get returns the API for a new bridge (safe for concurrent use).
func (b *browserAPI) Get() *webrtc.API {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.api
}

// Refresh re-probes the public IP and swaps the API when it changed. A failed
// probe keeps the last good address. No-op for a fixed configuration.
func (b *browserAPI) Refresh(ctx context.Context) (bool, error) {
	if b.probe == nil || b.mux == nil {
		return false, nil
	}
	ip, err := b.probe(ctx)
	if err != nil {
		return false, err
	}
	b.mu.RLock()
	same := ip == b.ip
	b.mu.RUnlock()
	if same {
		return false, nil
	}
	api, err := apiWithMux(b.mux, []string{ip})
	if err != nil {
		return false, err
	}
	b.mu.Lock()
	old := b.ip
	b.api, b.ip = api, ip
	b.mu.Unlock()
	b.log.Info("public ip for browser media", "ip", ip, "previous", old)
	return true, nil
}

// Watch refreshes the public IP every interval until ctx ends.
func (b *browserAPI) Watch(ctx context.Context, every time.Duration) {
	if b.probe == nil || b.mux == nil {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := b.Refresh(ctx); err != nil {
				b.log.Warn("public ip refresh failed; keeping the last address", "err", err)
			}
		}
	}
}

func defaultRouteInterface() string {
	data, err := os.ReadFile("/proc/net/route")
	if err != nil {
		return ""
	}
	return parseDefaultRoute(string(data))
}

func parseDefaultRoute(table string) string {
	for _, line := range strings.Split(table, "\n")[1:] {
		fields := strings.Fields(line)
		if len(fields) >= 4 && fields[1] == "00000000" && fields[0] != "" {
			return fields[0]
		}
	}
	return ""
}
