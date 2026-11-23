package events

import (
	"context"
	"sort"
	"time"

	"wacalls/internal/voip/core"
)

const historyCap = 10000

type CallStatus string

const (
	StatusStarting     CallStatus = "starting"
	StatusRinging      CallStatus = "ringing"
	StatusConnected    CallStatus = "connected"
	StatusReconnecting CallStatus = "reconnecting"
	StatusEnded        CallStatus = "ended"
)

type CallRecord struct {
	SessionID    string     `json:"sessionId"`
	CallID       string     `json:"callId"`
	Owner        *string    `json:"owner"`
	Direction    string     `json:"direction"`
	Peer         string     `json:"peer"`
	PeerName     string     `json:"peerName,omitempty"`
	PeerPhotoURL string     `json:"peerPhotoUrl,omitempty"`
	StartedAt    int64      `json:"startedAt"`
	Status       CallStatus `json:"status"`
	EndedAt      *int64     `json:"endedAt,omitempty"`
	EndReason    string     `json:"endReason,omitempty"`
}

func OwnerRef(owner string) *string {
	if owner == "" {
		return nil
	}
	return &owner
}

// callKey scopes a call record to its session: when two sessions of the same
// WaCalls talk to each other, WhatsApp uses the same call-id on both ends.
func callKey(sessionID, callID string) string { return sessionID + "\x00" + callID }

func (b *Broker) UpsertCall(r CallRecord) {
	b.mu.Lock()
	var prev CallStatus
	if old, ok := b.calls[callKey(r.SessionID, r.CallID)]; ok {
		prev = old.Status
	}
	cp := r
	b.calls[callKey(r.SessionID, r.CallID)] = &cp
	b.mu.Unlock()
	if r.Status != prev {
		switch r.Status {
		case StatusRinging:
			b.webhooks.enqueue("call.ringing", r)
		case StatusConnected:
			b.webhooks.enqueue("call.active", r)
		}
	}
	b.broadcastCallList()
	b.broadcast(map[string]any{
		"type": "call-status", "sessionId": r.SessionID, "id": r.CallID, "owner": r.Owner,
		"status": r.Status, "peer": r.Peer, "startedAt": r.StartedAt,
		"peerName": r.PeerName, "peerPhotoUrl": r.PeerPhotoURL,
	})
}

func (b *Broker) SetCallPhoto(sessionID, callID, url string) {
	rec, ok := b.GetCall(sessionID, callID)
	if !ok || url == "" || rec.PeerPhotoURL == url {
		return
	}
	rec.PeerPhotoURL = url
	b.UpsertCall(*rec)
}

func (b *Broker) GetCall(sessionID, id string) (*CallRecord, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	c, ok := b.calls[callKey(sessionID, id)]
	if !ok {
		return nil, false
	}
	cp := *c
	return &cp, true
}

func (b *Broker) SetOwner(sessionID, id, owner string) bool {
	if owner == "" {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.calls[callKey(sessionID, id)]
	if !ok {
		return false
	}
	if c.Owner != nil && *c.Owner != owner {
		return false
	}
	c.Owner = &owner
	return true
}

func (b *Broker) OwnerActiveCall(owner string) string {
	if owner == "" {
		return ""
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, c := range b.calls {
		if c.Owner != nil && *c.Owner == owner && c.Status != StatusEnded {
			return c.CallID
		}
	}
	return ""
}

func (b *Broker) EndCall(sessionID, id, reason string) {
	b.mu.Lock()
	c, ok := b.calls[callKey(sessionID, id)]
	if !ok {
		b.mu.Unlock()
		return
	}
	now := time.Now().UnixMilli()
	c.Status = StatusEnded
	c.EndedAt = &now
	c.EndReason = reason
	ended := *c
	delete(b.calls, callKey(sessionID, id))
	owner := c.Owner
	b.mu.Unlock()

	b.webhooks.enqueue("call.ended", ended)
	b.broadcast(map[string]any{
		"type": "call-ended", "sessionId": sessionID, "id": id, "owner": owner, "reason": reason, "endedAt": now,
	})
	b.broadcastCallList()
	b.persist(ended)
}

func (b *Broker) persist(rec CallRecord) {
	if b.records == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var endedAt int64
	if rec.EndedAt != nil {
		endedAt = *rec.EndedAt
	}
	cr := core.CallRecord{
		CallID: rec.CallID, SessionID: rec.SessionID, Owner: rec.Owner,
		Direction: rec.Direction, Peer: rec.Peer,
		StartedAt: rec.StartedAt, EndedAt: endedAt, EndReason: rec.EndReason,
	}
	if err := b.records.Insert(ctx, cr); err != nil {
		b.log.Error("persist call record", "call_id", rec.CallID, "err", err)
		return
	}
	if err := b.records.Prune(ctx, historyCap); err != nil {
		b.log.Error("prune call records", "err", err)
	}
}

func (b *Broker) callList() []CallRecord {
	b.mu.RLock()
	defer b.mu.RUnlock()
	list := make([]CallRecord, 0, len(b.calls))
	for _, c := range b.calls {
		list = append(list, *c)
	}
	return list
}

func (b *Broker) SessionCalls(sid string) []CallRecord {
	b.mu.RLock()
	defer b.mu.RUnlock()
	list := []CallRecord{}
	for _, c := range b.calls {
		if c.SessionID == sid {
			list = append(list, *c)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].StartedAt != list[j].StartedAt {
			return list[i].StartedAt < list[j].StartedAt
		}
		return list[i].CallID < list[j].CallID
	})
	return list
}

func (b *Broker) broadcastCallList() {
	b.broadcast(map[string]any{"type": "call-list", "calls": b.callList()})
}

func (b *Broker) HistoryRows(ctx context.Context, sessionID string, limit int, before core.HistoryCursor) ([]CallRecord, core.HistoryCursor, error) {
	if b.records == nil {
		return []CallRecord{}, core.HistoryCursor{}, nil
	}
	recs, err := b.records.List(ctx, sessionID, limit+1, before)
	if err != nil {
		return nil, core.HistoryCursor{}, err
	}
	var next core.HistoryCursor
	if len(recs) > limit {
		recs = recs[:limit]
		last := recs[limit-1]
		next = core.HistoryCursor{EndedAt: last.EndedAt, CallID: last.CallID}
	}
	rows := make([]CallRecord, 0, len(recs))
	for _, r := range recs {
		endedAt := r.EndedAt
		rows = append(rows, CallRecord{
			SessionID: r.SessionID, CallID: r.CallID, Owner: r.Owner, Direction: r.Direction,
			Peer: r.Peer, StartedAt: r.StartedAt, Status: StatusEnded,
			EndedAt: &endedAt, EndReason: r.EndReason,
		})
	}
	return rows, next, nil
}
