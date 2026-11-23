package session

import (
	"os"
	"sync"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"
	"google.golang.org/protobuf/proto"
)

// DeviceNameEnv sets the label shown in the phone's "Linked devices" list (default "Ticketz").
const DeviceNameEnv = "WACALLS_DEVICE_NAME"

var devicePropsMu sync.Mutex

// deviceLabel is the name shown in the phone's "Linked devices" list. The
// session name is an internal key (e.g. "ticketz-company12-whatsapp345") and
// is never shown to the phone owner.
func deviceLabel(_ string) string {
	if base := os.Getenv(DeviceNameEnv); base != "" {
		return base
	}
	return "Ticketz"
}

// connectWithDeviceProps sets the process-global store.DeviceProps for this pairing and
// connects while holding the lock, so concurrent pairings of different sessions cannot mix labels.
func connectWithDeviceProps(c *whatsmeow.Client, sessionName string) error {
	devicePropsMu.Lock()
	defer devicePropsMu.Unlock()
	store.DeviceProps.Os = proto.String(deviceLabel(sessionName))
	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_DESKTOP.Enum()
	return c.Connect()
}

// announcement lazily loads WACALLS_ANNOUNCE_WAV once.
func (m *Manager) announcement() []float32 {
	m.annOnce.Do(func() {
		p := os.Getenv(AnnounceEnv)
		if p == "" {
			return
		}
		pcm, err := LoadAnnouncement(p)
		if err != nil {
			m.log.Warn("announcement disabled", "path", p, "err", err)
			return
		}
		m.annPCM = pcm
	})
	return m.annPCM
}
