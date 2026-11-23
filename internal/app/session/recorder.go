package session

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

const (
	recSampleRate = 16000
	recTick       = 20 * time.Millisecond
	// recMaxLag bounds each direction's queue (500 ms); older samples are dropped so a
	// source running slightly fast never lets the two channels drift apart.
	recMaxLag = recSampleRate / 2
	// recHeaderEvery patches the WAV header every ~5 s so a crash leaves a playable file.
	recHeaderEvery = 250
)

var callIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// RecordingsDir is where <callId>.wav files live (WACALLS_RECORDINGS_DIR, default /data/recordings).
func RecordingsDir() string {
	if d := os.Getenv("WACALLS_RECORDINGS_DIR"); d != "" {
		return d
	}
	return "/data/recordings"
}

// RecordingPath returns the wav path for callID, or "" if the id is not a safe file name.
func RecordingPath(callID string) string {
	if !callIDRe.MatchString(callID) {
		return ""
	}
	return filepath.Join(RecordingsDir(), callID+".wav")
}

// Recorder streams a stereo PCM16 16 kHz WAV (L = operator/uplink, R = peer/downlink) to disk.
// Memory is bounded: two 500 ms queues and one 640-byte write buffer, regardless of call length.
type Recorder struct {
	f    *os.File
	mu   sync.Mutex
	up   []int16
	down []int16
	// frames written so far (samples per channel)
	written int64
	start   time.Time
	stop    chan struct{}
	done    chan struct{}
}

func StartRecorder(path string) (*Recorder, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}
	r := &Recorder{f: f, start: time.Now(), stop: make(chan struct{}), done: make(chan struct{})}
	if err := r.writeHeader(); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(44, 0); err != nil {
		f.Close()
		return nil, err
	}
	go r.loop()
	return r, nil
}

func f32ToI16(in []float32) []int16 {
	out := make([]int16, len(in))
	for i, v := range in {
		if v > 1 {
			v = 1
		} else if v < -1 {
			v = -1
		}
		out[i] = int16(v * 32767)
	}
	return out
}

func push(q []int16, s []int16) []int16 {
	q = append(q, s...)
	if len(q) > recMaxLag {
		q = append(q[:0], q[len(q)-recMaxLag:]...)
	}
	return q
}

// WriteUp queues operator (browser mic) samples.
func (r *Recorder) WriteUp(pcm []float32) {
	if r == nil || len(pcm) == 0 {
		return
	}
	s := f32ToI16(pcm)
	r.mu.Lock()
	r.up = push(r.up, s)
	r.mu.Unlock()
}

// WriteDown queues peer (WhatsApp contact) samples.
func (r *Recorder) WriteDown(pcm []float32) {
	if r == nil || len(pcm) == 0 {
		return
	}
	s := f32ToI16(pcm)
	r.mu.Lock()
	r.down = push(r.down, s)
	r.mu.Unlock()
}

func take(q []int16, n int) (out []int16, rest []int16) {
	out = make([]int16, n) // zero = silence when the side is short
	c := copy(out, q)
	return out, q[c:]
}

func (r *Recorder) loop() {
	defer close(r.done)
	t := time.NewTicker(recTick)
	defer t.Stop()
	ticks := 0
	for {
		select {
		case <-r.stop:
			r.flush()
			return
		case <-t.C:
			r.flush()
			if ticks++; ticks%recHeaderEvery == 0 {
				_ = r.patchHeader()
			}
		}
	}
}

// flush writes wall-clock-aligned frames: the number of samples that should exist by now
// minus what was already written; each channel is padded with silence if it has fewer.
func (r *Recorder) flush() {
	want := int64(time.Since(r.start).Seconds()*recSampleRate) - r.written
	if want <= 0 {
		return
	}
	r.mu.Lock()
	var u, d []int16
	u, r.up = take(r.up, int(want))
	d, r.down = take(r.down, int(want))
	r.mu.Unlock()
	buf := make([]byte, int(want)*4)
	for i := 0; i < int(want); i++ {
		binary.LittleEndian.PutUint16(buf[i*4:], uint16(u[i]))
		binary.LittleEndian.PutUint16(buf[i*4+2:], uint16(d[i]))
	}
	if _, err := r.f.Write(buf); err == nil {
		r.written += want
	}
}

func (r *Recorder) header() []byte {
	dataLen := uint32(r.written * 4)
	h := make([]byte, 44)
	copy(h[0:], "RIFF")
	binary.LittleEndian.PutUint32(h[4:], 36+dataLen)
	copy(h[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(h[16:], 16)
	binary.LittleEndian.PutUint16(h[20:], 1) // PCM
	binary.LittleEndian.PutUint16(h[22:], 2) // channels
	binary.LittleEndian.PutUint32(h[24:], recSampleRate)
	binary.LittleEndian.PutUint32(h[28:], recSampleRate*4)
	binary.LittleEndian.PutUint16(h[32:], 4)
	binary.LittleEndian.PutUint16(h[34:], 16)
	copy(h[36:], "data")
	binary.LittleEndian.PutUint32(h[40:], dataLen)
	return h
}

func (r *Recorder) writeHeader() error { _, err := r.f.WriteAt(r.header(), 0); return err }
func (r *Recorder) patchHeader() error { return r.writeHeader() }

// Close stops the loop, writes the tail and finalises the header. Safe to call once.
func (r *Recorder) Close() error {
	if r == nil {
		return nil
	}
	close(r.stop)
	<-r.done
	err := r.writeHeader()
	if cerr := r.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// LoadAnnouncement reads a mono 16 kHz PCM16 WAV into float32 samples.
func LoadAnnouncement(path string) ([]float32, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) < 12 || string(b[0:4]) != "RIFF" || string(b[8:12]) != "WAVE" {
		return nil, errors.New("not a RIFF/WAVE file")
	}
	var ok bool
	for p := 12; p+8 <= len(b); {
		id, sz := string(b[p:p+4]), int(binary.LittleEndian.Uint32(b[p+4:]))
		body := p + 8
		if id == "fmt " && body+16 <= len(b) {
			af := binary.LittleEndian.Uint16(b[body:])
			ch := binary.LittleEndian.Uint16(b[body+2:])
			sr := binary.LittleEndian.Uint32(b[body+4:])
			bits := binary.LittleEndian.Uint16(b[body+14:])
			if af != 1 || ch != 1 || sr != recSampleRate || bits != 16 {
				return nil, fmt.Errorf("announcement must be PCM16 mono 16000 Hz (got fmt=%d ch=%d sr=%d bits=%d)", af, ch, sr, bits)
			}
			ok = true
		}
		if id == "data" {
			if !ok {
				return nil, errors.New("data chunk before fmt chunk")
			}
			end := body + sz
			if end > len(b) {
				end = len(b)
			}
			n := (end - body) / 2
			out := make([]float32, n)
			for i := 0; i < n; i++ {
				out[i] = float32(int16(binary.LittleEndian.Uint16(b[body+i*2:]))) / 32768
			}
			return out, nil
		}
		p = body + sz + sz%2
	}
	return nil, errors.New("no data chunk")
}
