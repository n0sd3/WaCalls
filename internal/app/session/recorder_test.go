package session

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRecorderStereoAlignedWav(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.wav")
	r, err := StartRecorder(p)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ { // uplink only, 128-sample frames
		r.WriteUp(make([]float32, 128))
		time.Sleep(8 * time.Millisecond)
	}
	up := make([]float32, 320)
	for i := range up {
		up[i] = 0.5
	}
	r.WriteDown(up)
	time.Sleep(60 * time.Millisecond)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(p)
	if string(b[0:4]) != "RIFF" || binary.LittleEndian.Uint16(b[22:]) != 2 || binary.LittleEndian.Uint32(b[24:]) != 16000 {
		t.Fatal("bad header")
	}
	if int(binary.LittleEndian.Uint32(b[40:])) != len(b)-44 || (len(b)-44)%4 != 0 || len(b) < 44+4*1000 {
		t.Fatalf("bad sizes: data=%d file=%d", binary.LittleEndian.Uint32(b[40:]), len(b))
	}
	var rNonZero, lNonZero int
	for i := 44; i+4 <= len(b); i += 4 {
		if binary.LittleEndian.Uint16(b[i:]) != 0 {
			lNonZero++
		}
		if binary.LittleEndian.Uint16(b[i+2:]) != 0 {
			rNonZero++
		}
	}
	if lNonZero != 0 || rNonZero == 0 {
		t.Fatalf("channels: L=%d R=%d", lNonZero, rNonZero)
	}
}

func TestRecordingPathRejectsTraversal(t *testing.T) {
	if RecordingPath("../x") != "" || RecordingPath("ABC123") == "" {
		t.Fatal("callId validation")
	}
}

// O aviso de gravação é opt-in: gravar não implica tocar o aviso.
func TestAnnounceOnlyWhenRequested(t *testing.T) {
	s := &Session{recWant: map[string]bool{}, recAnnounce: map[string]bool{}, recorders: map[string]*Recorder{}, announcing: map[string]bool{}}
	s.SetRecord("silent", true, false)
	if s.wantsAnnounce("silent") {
		t.Fatal("gravação sem aviso pedido não pode tocar o aviso")
	}
	s.SetRecord("loud", true, true)
	if !s.wantsAnnounce("loud") {
		t.Fatal("aviso pedido deve tocar")
	}
	s.SetRecord("loud", false, true)
	if s.wantsAnnounce("loud") {
		t.Fatal("sem gravação não há aviso")
	}
}
