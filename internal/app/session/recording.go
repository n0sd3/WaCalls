package session

import (
	"time"
)

// AnnounceEnv names a mono 16 kHz PCM16 WAV played to the peer when a recorded call turns active.
const AnnounceEnv = "WACALLS_ANNOUNCE_WAV"

// SetRecord marks a call for recording; the recorder starts when the call becomes active.
// announce plays WACALLS_ANNOUNCE_WAV to the peer when recording starts (opt-in).
func (s *Session) SetRecord(callID string, on, announce bool) {
	s.recMu.Lock()
	defer s.recMu.Unlock()
	if on {
		s.recWant[callID] = true
	} else {
		delete(s.recWant, callID)
	}
	if on && announce {
		s.recAnnounce[callID] = true
	} else {
		delete(s.recAnnounce, callID)
	}
}

// wantsAnnounce reports whether the recording notice was requested for the call.
func (s *Session) wantsAnnounce(callID string) bool {
	s.recMu.Lock()
	defer s.recMu.Unlock()
	return s.recWant[callID] && s.recAnnounce[callID]
}

// IsRecording reports whether a recorder is still writing the call's file.
func (s *Session) IsRecording(callID string) bool {
	s.recMu.Lock()
	defer s.recMu.Unlock()
	return s.recorders[callID] != nil
}

func (s *Session) recorder(callID string) *Recorder {
	s.recMu.Lock()
	defer s.recMu.Unlock()
	return s.recorders[callID]
}

// maybeStartRecording runs on every state change; it is idempotent per call.
func (s *Session) maybeStartRecording(callID string) {
	s.recMu.Lock()
	if !s.recWant[callID] || s.recorders[callID] != nil {
		s.recMu.Unlock()
		return
	}
	path := RecordingPath(callID)
	if path == "" {
		s.recMu.Unlock()
		return
	}
	rec, err := StartRecorder(path)
	if err != nil {
		s.recMu.Unlock()
		s.log.Warn("recording could not start", "call_id", callID, "err", err)
		return
	}
	s.recorders[callID] = rec
	s.recMu.Unlock()
	s.log.Info("recording started", "call_id", callID, "path", path)
	if !s.wantsAnnounce(callID) {
		return
	}
	if pcm := s.mgr.announcement(); len(pcm) > 0 {
		go s.playAnnouncement(callID, pcm)
	}
}

func (s *Session) stopRecording(callID string) {
	s.recMu.Lock()
	rec := s.recorders[callID]
	delete(s.recorders, callID)
	delete(s.recWant, callID)
	delete(s.recAnnounce, callID)
	s.recMu.Unlock()
	if rec != nil {
		if err := rec.Close(); err != nil {
			s.log.Warn("recording close failed", "call_id", callID, "err", err)
		}
	}
}

// playAnnouncement feeds pcm into the uplink in 20 ms slices (real time), through the same
// CallManager.FeedCapturedPCM path as the browser mic. While it plays, browser mic frames are
// dropped (see uplink) so the announcement is not interleaved with speech.
func (s *Session) playAnnouncement(callID string, pcm []float32) {
	cm, ok := s.calls.Get(callID)
	if !ok {
		return
	}
	s.setAnnouncing(callID, true)
	defer s.setAnnouncing(callID, false)
	const slice = recSampleRate / 50 // 320 samples = 20 ms
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for off := 0; off < len(pcm); off += slice {
		<-t.C
		if _, live := s.calls.Get(callID); !live {
			return
		}
		end := off + slice
		if end > len(pcm) {
			end = len(pcm)
		}
		chunk := pcm[off:end]
		s.recorder(callID).WriteUp(chunk)
		cm.FeedCapturedPCM(chunk)
	}
}

func (s *Session) setAnnouncing(callID string, on bool) {
	s.recMu.Lock()
	defer s.recMu.Unlock()
	if on {
		s.announcing[callID] = true
	} else {
		delete(s.announcing, callID)
	}
}

func (s *Session) isAnnouncing(callID string) bool {
	s.recMu.Lock()
	defer s.recMu.Unlock()
	return s.announcing[callID]
}

// uplink is the browser-mic entry point: record it, then hand it to the call core.
func (s *Session) uplink(callID string, pcm []float32, feed func([]float32)) {
	if s.isAnnouncing(callID) {
		return
	}
	s.recorder(callID).WriteUp(pcm)
	feed(pcm)
}
