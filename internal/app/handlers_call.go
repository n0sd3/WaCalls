package app

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"wacalls/internal/app/events"
	"wacalls/internal/app/session"
	"wacalls/internal/voip/call"
	"wacalls/internal/voip/core"
)

func (s *Server) handleStartCall(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doStartCall(sess, w, r)
	}
}

func (s *Server) handleWebRTC(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doWebRTC(sess, w, r)
	}
}

func (s *Server) handleAccept(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doAccept(sess, w, r)
	}
}

func (s *Server) handleReject(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doReject(sess, w, r)
	}
}

func (s *Server) handleEndCall(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doEndCall(sess, w, r)
	}
}

func (s *Server) doStartCall(sess *session.Session, w http.ResponseWriter, r *http.Request) {
	if !sess.IsPaired() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not paired"})
		return
	}
	var body struct {
		Phone    string `json:"phone"`
		Record   bool   `json:"record"`
		Announce bool   `json:"announce"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.Phone) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "phone required"})
		return
	}
	phone := normalizePhone(body.Phone)
	if phone == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid phone"})
		return
	}
	owner := clientID(r)
	if other := s.broker.OwnerActiveCall(owner); other != "" {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "operator already on a call"})
		return
	}
	st, err := sess.StartCall(r.Context(), phone, body.Record || r.URL.Query().Get("record") == "1", body.Announce || r.URL.Query().Get("announce") == "1")
	if errors.Is(err, session.ErrTooManyCalls) {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "max concurrent calls"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.broker.UpsertCall(events.CallRecord{
		SessionID: sess.ID(), CallID: st.CallID, Owner: events.OwnerRef(owner), Direction: "outbound",
		Peer: st.Peer, PeerName: st.PeerName, PeerPhotoURL: st.PeerPhotoURL,
		StartedAt: time.Now().UnixMilli(), Status: events.StatusRinging,
	})
	sess.FetchCallPhoto(st.CallID, st.Peer)
	writeJSON(w, http.StatusOK, map[string]any{"call": map[string]string{"callId": st.CallID}})
}

func (s *Server) doWebRTC(sess *session.Session, w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("id")
	if !sess.HasCall(callID) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	var body struct {
		SDPOffer string `json:"sdp_offer"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.SDPOffer == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sdp_offer required"})
		return
	}
	answer, err := sess.AttachBrowser(callID, body.SDPOffer)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"sdp_answer": answer})
}

func (s *Server) doAccept(sess *session.Session, w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !sess.HasCall(id) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	owner := clientID(r)
	if other := s.broker.OwnerActiveCall(owner); other != "" && other != id {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "operator already on a call"})
		return
	}
	if !s.broker.SetOwner(sess.ID(), id, owner) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "claimed by another client"})
		return
	}
	s.broker.EmitIncomingClaimed(sess.ID(), id, owner)
	var body struct {
		Record   bool `json:"record"`
		Announce bool `json:"announce"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body) // body is optional
	if err := sess.AcceptCall(r.Context(), id, body.Record || r.URL.Query().Get("record") == "1", body.Announce || r.URL.Query().Get("announce") == "1"); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"call": map[string]string{"callId": id}})
}

func (s *Server) handleCallList(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"calls": s.broker.SessionCalls(sess.ID())})
}

func (s *Server) handleCallGet(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	rec, ok := s.broker.GetCall(sess.ID(), r.PathValue("id"))
	if !ok || rec.SessionID != sess.ID() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"call": rec})
}

func (s *Server) doReject(sess *session.Session, w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var invalid *call.InvalidTransition
	if err := sess.RejectCall(r.Context(), id); errors.As(err, &invalid) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	s.broker.EndCall(sess.ID(), id, string(core.EndCallReasonDeclined))
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleMute(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doMute(sess, w, r)
	}
}

func (s *Server) doMute(sess *session.Session, w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !sess.HasCall(id) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	var body struct {
		Muted *bool `json:"muted"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Muted == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "muted required"})
		return
	}
	var invalid *call.InvalidTransition
	if err := sess.SetMute(r.Context(), id, *body.Muted); errors.As(err, &invalid) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	} else if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) doEndCall(sess *session.Session, w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	_ = sess.EndCall(r.Context(), id)
	s.broker.EndCall(sess.ID(), id, string(core.EndCallReasonUserEnded))
	w.WriteHeader(http.StatusNoContent)
}

func normalizePhone(p string) string {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "+")
	var b strings.Builder
	for _, c := range p {
		if c >= '0' && c <= '9' {
			b.WriteRune(c)
		}
	}
	return b.String()
}

func (s *Server) handleRecordingGet(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	id := r.PathValue("id")
	path := session.RecordingPath(id)
	if path == "" || sess.IsRecording(id) {
		code := http.StatusNotFound
		if path != "" {
			code = http.StatusConflict
		}
		writeJSON(w, code, map[string]string{"error": "recording not available"})
		return
	}
	f, err := os.Open(path)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such recording"})
		return
	}
	defer f.Close()
	st, _ := f.Stat()
	w.Header().Set("Content-Type", "audio/wav")
	http.ServeContent(w, r, id+".wav", st.ModTime(), f)
}

func (s *Server) handleRecordingDelete(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	id := r.PathValue("id")
	path := session.RecordingPath(id)
	if path == "" || sess.IsRecording(id) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "recording not available"})
		return
	}
	if err := os.Remove(path); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such recording"})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
