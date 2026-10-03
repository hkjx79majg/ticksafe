package server

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func postMailbox(t *testing.T, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, mailboxAnalyzePath, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	Handler().ServeHTTP(rec, r)
	return rec
}

func mailboxOK(t *testing.T, body string) mailboxResponse {
	t.Helper()
	rec := postMailbox(t, body, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp mailboxResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return resp
}

func mbDecl(name string, cap int64) string {
	return `{"name":"` + name + `","capacity":` + strconv.FormatInt(cap, 10) + `}`
}

func mbTask(id string, prio, rel, dead int64, actions ...string) string {
	return `{"id":"` + id + `","priority":` + strconv.FormatInt(prio, 10) +
		`,"release":` + strconv.FormatInt(rel, 10) +
		`,"deadline":` + strconv.FormatInt(dead, 10) +
		`,"actions":[` + strings.Join(actions, ",") + `]}`
}

func mbRun(n int64) string { return `{"run":` + strconv.FormatInt(n, 10) + `}` }
func mbSend(m, msg string) string {
	return `{"send":{"mailbox":"` + m + `","message":"` + msg + `"}}`
}
func mbRecv(m string) string { return `{"receive":"` + m + `"}` }

func mbBody(mailboxes []string, tasks ...string) string {
	return `{"horizon":100,"mailboxes":[` + strings.Join(mailboxes, ",") +
		`],"tasks":[` + strings.Join(tasks, ",") + `]}`
}

func TestMailboxDirectDelivery(t *testing.T) {
	// R(p0, rel 2) receives and blocks; S(p2, rel 0) sends "hi" at t=5 after
	// running 5. Direct delivery at t=5: R gets [5,8), S finishes [8,9).
	body := mbBody([]string{mbDecl("M", 1)},
		mbTask("S", 2, 0, 50, mbRun(5), mbSend("M", "hi"), mbRun(1)),
		mbTask("R", 0, 2, 50, mbRecv("M"), mbRun(3)))
	resp := mailboxOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 9 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@9: %+v", resp.Status, resp.StoppedAt, resp.Timeline)
	}
	want := []mailboxTimelineInterval{
		{TaskID: "S", Start: 0, End: 5},
		{TaskID: "R", Start: 5, End: 8},
		{TaskID: "S", Start: 8, End: 9},
	}
	if len(resp.Timeline) != len(want) {
		t.Fatalf("timeline = %+v, want %+v", resp.Timeline, want)
	}
	for i := range want {
		if resp.Timeline[i] != want[i] {
			t.Fatalf("timeline[%d] = %+v, want %+v", i, resp.Timeline[i], want[i])
		}
	}
	r := resp.Results[1]
	if r.State != "completed" || r.Completion == nil || *r.Completion != 8 {
		t.Fatalf("R result = %+v", r)
	}
	if len(r.Received) != 1 || r.Received[0] != (receivedMessage{Mailbox: "M", Message: "hi", Time: 5}) {
		t.Fatalf("R received = %+v", r.Received)
	}
	s := resp.Results[0]
	if s.BlockedAction != nil || s.BlockedOn != nil || len(s.Received) != 0 {
		t.Fatalf("S result = %+v", s)
	}
}

func TestMailboxQueuedSend(t *testing.T) {
	// Capacity 2: two sends at t=0 enqueue without blocking, third blocks; the
	// receiver drains FIFO and refills the slot from the blocked sender.
	body := mbBody([]string{mbDecl("M", 2)},
		mbTask("S", 2, 0, 100,
			mbSend("M", "a"), mbSend("M", "b"), mbSend("M", "c"), mbRun(4)),
		mbTask("R", 1, 3, 100,
			mbRecv("M"), mbRun(1), mbRecv("M"), mbRun(1), mbRecv("M"), mbRun(1)))
	resp := mailboxOK(t, body)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed: %+v %+v", resp.Status, resp.Timeline, resp.Results)
	}
	r := resp.Results[1]
	wantMsgs := []receivedMessage{
		{Mailbox: "M", Message: "a", Time: 3},
		{Mailbox: "M", Message: "b", Time: 4},
		{Mailbox: "M", Message: "c", Time: 5},
	}
	if len(r.Received) != 3 {
		t.Fatalf("received = %+v, want %+v", r.Received, wantMsgs)
	}
	for i := range wantMsgs {
		if r.Received[i] != wantMsgs[i] {
			t.Fatalf("received[%d] = %+v, want %+v", i, r.Received[i], wantMsgs[i])
		}
	}
}

func TestMailboxBlockedSenderRefill(t *testing.T) {
	// Capacity 1. S(p2) enqueues "a" at t=0, then blocks sending "b".
	// R(p1) released at t=4 takes "a" at t=4; "b" refills immediately and the
	// second receive at t=5 yields "b", unblocking S. S(p2) only runs after R
	// blocks on the third receive at t=6: S runs [6,8), sends "c" directly and
	// completes; R finishes [8,9).
	body := mbBody([]string{mbDecl("M", 1)},
		mbTask("S", 2, 0, 100,
			mbSend("M", "a"), mbSend("M", "b"), mbRun(2), mbSend("M", "c")),
		mbTask("R", 1, 4, 100,
			mbRecv("M"), mbRun(1), mbRecv("M"), mbRun(1), mbRecv("M"), mbRun(1)))
	resp := mailboxOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 9 {
		t.Fatalf("status=%q stoppedAt=%d: %s", resp.Status, resp.StoppedAt, tlString(resp.Timeline))
	}
	want := []mailboxTimelineInterval{
		{TaskID: "R", Start: 4, End: 6},
		{TaskID: "S", Start: 6, End: 8},
		{TaskID: "R", Start: 8, End: 9},
	}
	if len(resp.Timeline) != len(want) {
		t.Fatalf("timeline = %s, want %+v", tlString(resp.Timeline), want)
	}
	for i := range want {
		if resp.Timeline[i] != want[i] {
			t.Fatalf("timeline[%d] = %+v, want %+v", i, resp.Timeline[i], want[i])
		}
	}
	s, r := resp.Results[0], resp.Results[1]
	if s.State != "completed" || s.Executed != 2 || s.Completion == nil || *s.Completion != 8 {
		t.Fatalf("S result = %+v", s)
	}
	wantMsgs := []receivedMessage{
		{Mailbox: "M", Message: "a", Time: 4},
		{Mailbox: "M", Message: "b", Time: 5},
		{Mailbox: "M", Message: "c", Time: 8},
	}
	if r.State != "completed" || len(r.Received) != 3 {
		t.Fatalf("R result = %+v, want %+v", r, wantMsgs)
	}
	for i := range wantMsgs {
		if r.Received[i] != wantMsgs[i] {
			t.Fatalf("R received[%d] = %+v, want %+v", i, r.Received[i], wantMsgs[i])
		}
	}
}

func TestMailboxEarliestBlockedReceiverAcrossTime(t *testing.T) {
	// R1(p2) blocks on M at t=0 (highest priority at release, so it executes
	// the receive first); R2(p1) preempts S at t=2 and blocks at t=2. S sends
	// at t=6: the earliest blocker R1 must receive even though R2 has the
	// better priority.
	body := mbBody([]string{mbDecl("M", 1)},
		mbTask("R1", 2, 0, 100, mbRecv("M"), mbRun(1)),
		mbTask("S", 3, 0, 100, mbRun(6), mbSend("M", "x"), mbRun(1)),
		mbTask("R2", 1, 2, 100, mbRecv("M"), mbRun(1)))
	resp := mailboxOK(t, body)
	if len(resp.Results[0].Received) != 1 || resp.Results[0].Received[0].Message != "x" {
		t.Fatalf("R1 should receive first (earliest block): %+v", resp.Results[0].Received)
	}
	if len(resp.Results[2].Received) != 0 {
		t.Fatalf("R2 must not receive: %+v", resp.Results[2].Received)
	}
}

func TestMailboxSameInstantReceiverInputOrder(t *testing.T) {
	// Both receivers block on M at t=0; the first in input order receives even
	// though it has the lower (numerically larger) priority.
	body := mbBody([]string{mbDecl("M", 1)},
		mbTask("winner", 1, 0, 50, mbRecv("M"), mbRun(1)),
		mbTask("loser", 0, 0, 50, mbRecv("M"), mbRun(1)),
		mbTask("S", 2, 0, 50, mbRun(2), mbSend("M", "x")))
	resp := mailboxOK(t, body)
	if got := resp.Results[0].Received; len(got) != 1 || got[0].Time != 2 || got[0].Message != "x" {
		t.Fatalf("winner should receive first: %+v", got)
	}
	if len(resp.Results[1].Received) != 0 {
		t.Fatalf("loser must not receive the first message: %+v", resp.Results[1].Received)
	}
}

func tlString(ivs []mailboxTimelineInterval) string {
	var b strings.Builder
	for _, iv := range ivs {
		b.WriteString(iv.TaskID)
		b.WriteString("[")
		b.WriteString(strconv.FormatInt(iv.Start, 10))
		b.WriteString(",")
		b.WriteString(strconv.FormatInt(iv.End, 10))
		b.WriteString(") ")
	}
	return b.String()
}

func TestMailboxUnblockPreemptsHigherPriority(t *testing.T) {
	// H(p0) blocks receiving on M at t=1; L(p2) runs and at t=4 its send
	// delivers directly to H, which preempts immediately at t=4.
	body := mbBody([]string{mbDecl("M", 1)},
		mbTask("L", 2, 0, 50, mbRun(4), mbSend("M", "x"), mbRun(4)),
		mbTask("H", 0, 1, 50, mbRecv("M"), mbRun(2)))
	resp := mailboxOK(t, body)
	if resp.Status != "completed" {
		t.Fatalf("status = %q: %+v", resp.Status, resp.Timeline)
	}
	want := []mailboxTimelineInterval{
		{TaskID: "L", Start: 0, End: 4},
		{TaskID: "H", Start: 4, End: 6},
		{TaskID: "L", Start: 6, End: 10},
	}
	if len(resp.Timeline) != len(want) {
		t.Fatalf("timeline = %s, want %+v", tlString(resp.Timeline), want)
	}
	for i := range want {
		if resp.Timeline[i] != want[i] {
			t.Fatalf("timeline[%d] = %+v, want %+v", i, resp.Timeline[i], want[i])
		}
	}
}

func TestMailboxEqualPriorityNoPreempt(t *testing.T) {
	// A and B both prio 1, released at 0. A runs first (input order); B blocks
	// on receive at t=0 after A's first run? Build: A runs [0,3) then sends to
	// blocked B; B was scheduled only after A blocks/completes at t=3... Use
	// releases at different times instead: B(rel 0) receive-blocks immediately;
	// A(rel 0, input first) runs 3, sends, then keeps running another 2: equal
	// priority receiver must NOT preempt the sender.
	body := mbBody([]string{mbDecl("M", 1)},
		mbTask("A", 1, 0, 50, mbRun(3), mbSend("M", "x"), mbRun(2)),
		mbTask("B", 1, 0, 50, mbRecv("M"), mbRun(1)))
	resp := mailboxOK(t, body)
	want := []mailboxTimelineInterval{
		{TaskID: "A", Start: 0, End: 5},
		{TaskID: "B", Start: 5, End: 6},
	}
	if len(resp.Timeline) != len(want) {
		t.Fatalf("timeline = %s, want %+v", tlString(resp.Timeline), want)
	}
	for i := range want {
		if resp.Timeline[i] != want[i] {
			t.Fatalf("timeline[%d] = %+v, want %+v", i, resp.Timeline[i], want[i])
		}
	}
}

func TestMailboxStalled(t *testing.T) {
	// A runs 2 then receives on the permanently-empty M and blocks; B is a
	// blocked receiver as well. No future releases -> stalled at t=2.
	body := mbBody([]string{mbDecl("M", 2)},
		mbTask("A", 1, 0, 50, mbRun(2), mbRecv("M")),
		mbTask("B", 2, 0, 50, mbRecv("M")))
	resp := mailboxOK(t, body)
	if resp.Status != "stalled" || resp.StoppedAt != 2 {
		t.Fatalf("status=%q stoppedAt=%d, want stalled@2", resp.Status, resp.StoppedAt)
	}
	a := resp.Results[0]
	if a.State != "blocked" || a.BlockedAction == nil || *a.BlockedAction != "receive" ||
		a.BlockedOn == nil || *a.BlockedOn != "M" || a.Executed != 2 {
		t.Fatalf("A result = %+v", a)
	}
	b := resp.Results[1]
	if b.State != "blocked" || b.BlockedAction == nil || *b.BlockedAction != "receive" {
		t.Fatalf("B result = %+v", b)
	}
}

func TestMailboxCompletedBeforeHorizon(t *testing.T) {
	body := mbBody([]string{mbDecl("M", 1)},
		mbTask("A", 0, 0, 50, mbRun(3)))
	resp := mailboxOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 3 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@3", resp.Status, resp.StoppedAt)
	}
}

func TestMailboxHorizonStatus(t *testing.T) {
	body := mbBody([]string{mbDecl("M", 1)},
		mbTask("A", 0, 0, 200, mbRun(200)))
	resp := mailboxOK(t, body)
	if resp.Status != "horizon" || resp.StoppedAt != 100 {
		t.Fatalf("status=%q stoppedAt=%d, want horizon@100", resp.Status, resp.StoppedAt)
	}
	if a := resp.Results[0]; a.State != "ready" || a.Completion != nil || a.Executed != 100 ||
		a.DeadlineStatus != "pending" {
		t.Fatalf("A result = %+v", a)
	}
}

func TestMailboxDeadlineMissed(t *testing.T) {
	// Completion after the deadline is observed as completed + missed.
	body := mbBody([]string{mbDecl("M", 1)},
		mbTask("A", 1, 0, 5, mbRun(20)))
	resp := mailboxOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 20 ||
		resp.Results[0].DeadlineStatus != "missed" {
		t.Fatalf("status=%q stoppedAt=%d result=%+v", resp.Status, resp.StoppedAt, resp.Results[0])
	}
}

func TestMailboxDeadlineMissedUnfinished(t *testing.T) {
	// Unfinished at the horizon with the deadline already passed -> missed.
	body := mbBody([]string{mbDecl("M", 1)},
		mbTask("A", 1, 0, 5, mbRun(200)))
	resp := mailboxOK(t, body)
	if resp.Status != "horizon" || resp.Results[0].DeadlineStatus != "missed" {
		t.Fatalf("status=%q result=%+v", resp.Status, resp.Results[0])
	}
}

func TestMailboxCompletionAtHorizonWithSend(t *testing.T) {
	// R(p0) preempts S at t=3 and blocks receiving on M. S resumes [3,5); its
	// run ends exactly at the horizon followed by a send, which delivers
	// directly to R at t=5. S completes at the boundary; R records delivery but
	// never runs, so the overall status is horizon.
	body := `{"horizon":5,"mailboxes":[` + mbDecl("M", 1) + `],"tasks":[` +
		mbTask("S", 1, 0, 50, mbRun(5), mbSend("M", "z")) + "," +
		mbTask("R", 0, 3, 50, mbRecv("M"), mbRun(1)) + `]}`
	resp := mailboxOK(t, body)
	if resp.Status != "horizon" || resp.StoppedAt != 5 {
		t.Fatalf("status=%q stoppedAt=%d, want horizon@5: %+v", resp.Status, resp.StoppedAt, resp.Timeline)
	}
	s := resp.Results[0]
	if s.State != "completed" || s.Completion == nil || *s.Completion != 5 {
		t.Fatalf("S result = %+v", s)
	}
	r := resp.Results[1]
	if len(r.Received) != 1 || r.Received[0].Time != 5 || r.Received[0].Message != "z" {
		t.Fatalf("R received = %+v, want z@5", r.Received)
	}
	if r.State != "ready" || r.Completion != nil || r.Executed != 0 {
		t.Fatalf("R must not run past the horizon: %+v", r)
	}
}

func TestMailboxBlockedSenderResultFields(t *testing.T) {
	body := mbBody([]string{mbDecl("M", 1)},
		mbTask("S", 0, 0, 50, mbSend("M", "a"), mbSend("M", "b")))
	resp := mailboxOK(t, body)
	if resp.Status != "stalled" || resp.StoppedAt != 0 {
		t.Fatalf("status=%q stoppedAt=%d, want stalled@0", resp.Status, resp.StoppedAt)
	}
	s := resp.Results[0]
	if s.State != "blocked" || s.BlockedAction == nil || *s.BlockedAction != "send" ||
		s.BlockedOn == nil || *s.BlockedOn != "M" {
		t.Fatalf("S result = %+v", s)
	}
}

func TestMailboxAdjacentRunsMerge(t *testing.T) {
	body := mbBody([]string{mbDecl("M", 1)},
		mbTask("A", 0, 0, 50, mbRun(2), mbRun(3)))
	resp := mailboxOK(t, body)
	if len(resp.Timeline) != 1 || resp.Timeline[0] != (mailboxTimelineInterval{TaskID: "A", Start: 0, End: 5}) {
		t.Fatalf("timeline = %+v", resp.Timeline)
	}
}

func TestMailboxUnicodeMessage(t *testing.T) {
	// "你好🎉" is 3 code points; valid. A receive must round-trip it.
	body := mbBody([]string{mbDecl("M", 1)},
		mbTask("S", 1, 0, 50, mbSend("M", "你好🎉")),
		mbTask("R", 0, 0, 50, mbRecv("M")))
	resp := mailboxOK(t, body)
	if resp.Status != "completed" {
		t.Fatalf("status = %q: %s", resp.Status, resp.Status)
	}
	if got := resp.Results[1].Received; len(got) != 1 || got[0].Message != "你好🎉" {
		t.Fatalf("received = %+v", got)
	}
}

func TestMailboxDeterministic(t *testing.T) {
	body := mbBody([]string{mbDecl("M", 2)},
		mbTask("A", 2, 0, 50, mbSend("M", "a"), mbRun(3), mbSend("M", "b"), mbRun(1)),
		mbTask("B", 0, 1, 50, mbRecv("M"), mbRun(2), mbRecv("M"), mbRun(2)))
	first := mailboxOK(t, body)
	second := mailboxOK(t, body)
	fj, _ := json.Marshal(first)
	sj, _ := json.Marshal(second)
	if !bytes.Equal(fj, sj) {
		t.Fatalf("non-deterministic response:\n%s\n%s", fj, sj)
	}
}

func TestMailboxValidationFailures(t *testing.T) {
	cases := map[string]string{
		"json null body":         `null`,
		"missing horizon":        `{"mailboxes":[{"name":"M","capacity":1}],"tasks":[]}`,
		"missing mailboxes":      `{"horizon":3,"tasks":[]}`,
		"empty mailboxes":        `{"horizon":3,"mailboxes":[],"tasks":[]}`,
		"too many mailboxes":     `{"horizon":3,"mailboxes":[` + strings.Repeat(mbDecl("M", 1)+",", 256) + mbDecl("Z", 1) + `],"tasks":[]}`,
		"duplicate mailbox":      `{"horizon":3,"mailboxes":[` + mbDecl("M", 1) + "," + mbDecl("M", 2) + `],"tasks":[]}`,
		"mailbox empty name":     `{"horizon":3,"mailboxes":[{"name":"","capacity":1}],"tasks":[]}`,
		"capacity zero":          `{"horizon":3,"mailboxes":[{"name":"M","capacity":0}],"tasks":[]}`,
		"capacity negative":      `{"horizon":3,"mailboxes":[{"name":"M","capacity":-1}],"tasks":[]}`,
		"capacity too large":     `{"horizon":3,"mailboxes":[{"name":"M","capacity":65536}],"tasks":[]}`,
		"capacity missing":       `{"horizon":3,"mailboxes":[{"name":"M"}],"tasks":[]}`,
		"capacity null":          `{"horizon":3,"mailboxes":[{"name":"M","capacity":null}],"tasks":[]}`,
		"missing tasks":          `{"horizon":3,"mailboxes":[` + mbDecl("M", 1) + `]}`,
		"empty tasks":            `{"horizon":3,"mailboxes":[` + mbDecl("M", 1) + `],"tasks":[]}`,
		"empty id":               mbBody([]string{mbDecl("M", 1)}, mbTask("", 0, 0, 1, mbRun(1))),
		"duplicate id":           mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, mbRun(1)), mbTask("a", 1, 0, 20, mbRun(1))),
		"priority negative":      mbBody([]string{mbDecl("M", 1)}, mbTask("a", -1, 0, 20, mbRun(1))),
		"release at horizon":     mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 100, 101, mbRun(1))),
		"deadline at release":    mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 0, mbRun(1))),
		"empty actions":          mbBody([]string{mbDecl("M", 1)}, `{"id":"a","priority":0,"release":0,"deadline":1,"actions":[]}`),
		"too many actions":       mbBody1025Actions(),
		"no verb":                mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, `{}`)),
		"two verbs":              mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, `{"run":1,"receive":"M"}`)),
		"send and receive verbs": mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, `{"send":{"mailbox":"M","message":"x"},"receive":"M"}`)),
		"run zero":               mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, mbRun(0))),
		"send missing mailbox":   mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, `{"send":{"message":"x"}}`)),
		"send missing message":   mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, `{"send":{"mailbox":"M"}}`)),
		"send null message":      mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, `{"send":{"mailbox":"M","message":null}}`)),
		"send empty message":     mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, mbSend("M", ""))),
		"send undeclared":        mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, mbSend("Q", "x"))),
		"receive undeclared":     mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, mbRecv("Q"))),
		"receive null":           mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, `{"receive":null}`)),
		"wrong run type":         mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, `{"run":"1"}`)),
		"run total overflow":     mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, mbRun(maxInt64), mbRun(1))),
		"global run overflow": mbBody([]string{mbDecl("M", 1)},
			mbTask("a", 0, 0, 20, mbRun(maxInt64)), mbTask("b", 0, 0, 20, mbRun(1))),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postMailbox(t, body, "application/json"),
				http.StatusUnprocessableEntity, "validation_failed")
		})
	}

	bad400 := map[string]string{
		"unknown top key":  `{"horizon":20,"mailboxes":[` + mbDecl("M", 1) + `],"bogus":1,"tasks":[]}`,
		"unknown action":   mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, `{"sleep":1}`)),
		"unknown send key": mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, `{"send":{"mailbox":"M","message":"x","bogus":1}}`)),
		"trailing token":   `{"horizon":3,"mailboxes":[],"tasks":[]} garbage`,
		"duplicate key":    `{"horizon":3,"horizon":4,"mailboxes":[],"tasks":[]}`,
		"syntax error":     `{`,
	}
	for name, body := range bad400 {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postMailbox(t, body, "application/json"),
				http.StatusBadRequest, "invalid_json")
		})
	}
}

func mbBody1025Actions() string {
	acts := make([]string, 1025)
	for i := range acts {
		acts[i] = mbRun(1)
	}
	return mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, acts...))
}

func TestMailboxCountsAndMessageLengthBounds(t *testing.T) {
	acts := make([]string, 1024)
	for i := range acts {
		acts[i] = mbRun(1)
	}
	ok := postMailbox(t, mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 2000, acts...)), "application/json")
	if ok.Code != http.StatusOK {
		t.Fatalf("1024 actions: %d %s", ok.Code, ok.Body.String())
	}

	// 256 mailboxes accepted, 257 rejected.
	mbs := make([]string, 256)
	for i := range mbs {
		mbs[i] = mbDecl("m"+strconv.Itoa(i), 1)
	}
	if rec := postMailbox(t, mbBody(mbs, mbTask("a", 0, 0, 20, mbRun(1))), "application/json"); rec.Code != http.StatusOK {
		t.Fatalf("256 mailboxes: %d %s", rec.Code, rec.Body.String())
	}
	mbs = append(mbs, mbDecl("extra", 1))
	assertErrorCode(t, postMailbox(t, mbBody(mbs, mbTask("a", 0, 0, 20, mbRun(1))), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")

	// A 257-code-point message is rejected; 256 accepted.
	long := strings.Repeat("x", 257)
	assertErrorCode(t, postMailbox(t, mbBody(mbs[:1], mbTask("a", 0, 0, 20, mbSend("m0", long))),
		"application/json"), http.StatusUnprocessableEntity, "validation_failed")
	exact := strings.Repeat("好", 256)
	if rec := postMailbox(t, mbBody(mbs[:1], mbTask("a", 0, 0, 20, mbSend("m0", exact))),
		"application/json"); rec.Code != http.StatusOK {
		t.Fatalf("256-rune message: %d %s", rec.Code, rec.Body.String())
	}
}

func TestMailboxMethodAndMediaType(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := httptest.NewRecorder()
		Handler().ServeHTTP(rec, httptest.NewRequest(method, mailboxAnalyzePath, nil))
		assertErrorCode(t, rec, http.StatusMethodNotAllowed, "method_not_allowed")
		if got := rec.Header().Get("Allow"); got != http.MethodPost {
			t.Fatalf("Allow = %q, want POST", got)
		}
	}
	assertErrorCode(t, postMailbox(t, `{"horizon":3,"mailboxes":[],"tasks":[]}`, ""),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	assertErrorCode(t, postMailbox(t, `{"horizon":3,"mailboxes":[],"tasks":[]}`, "text/plain"),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	rec := postMailbox(t, mbBody([]string{mbDecl("M", 1)}, mbTask("a", 0, 0, 20, mbRun(1))),
		"application/json; charset=utf-8")
	if rec.Code != http.StatusOK {
		t.Fatalf("charset parameter rejected: %d %s", rec.Code, rec.Body.String())
	}
}

// ---- Differential test against an independent tick-by-tick reference ------

type refMailbox struct {
	name     string
	capacity int64
}

type refMailTask struct {
	id              string
	prio, rel, dead int64
	acts            []mailboxAction
}

type refMailSim struct {
	boxes     []refMailbox
	tasks     []refMailTask
	n         int
	horizon   int64
	queue     map[string][]string
	fullWait  map[string][]int
	emptyWait map[string][]int
	blockedM  []string
	blockedK  []int
	blockTime []int64
	pending   []string
	pc        []int
	rem       []int64
	released  []bool
	done      []bool
	executed  []int64
	finish    []*int64
	received  [][]receivedMessage
	now       int64
	current   int
	segs      []mailboxSegment
	stalled   bool
}

func refMailRun(boxes []refMailbox, tasks []refMailTask, horizon int64) mailboxResponse {
	r := &refMailSim{
		boxes: boxes, tasks: tasks, n: len(tasks), horizon: horizon,
		queue: map[string][]string{}, fullWait: map[string][]int{}, emptyWait: map[string][]int{},
		current: -1,
	}
	r.blockedM = make([]string, r.n)
	r.blockedK = make([]int, r.n)
	r.blockTime = make([]int64, r.n)
	r.pending = make([]string, r.n)
	r.pc = make([]int, r.n)
	r.rem = make([]int64, r.n)
	r.released = make([]bool, r.n)
	r.done = make([]bool, r.n)
	r.executed = make([]int64, r.n)
	r.finish = make([]*int64, r.n)
	r.received = make([][]receivedMessage, r.n)

	r.now = 0
	for i := 0; i < r.n; i++ {
		if tasks[i].rel == 0 {
			r.released[i] = true
		}
	}
	for t := int64(0); t < horizon; t++ {
		if !r.settlePoint() {
			return r.build()
		}
		if r.current == -1 {
			allDone := true
			for i := 0; i < r.n; i++ {
				if !r.done[i] {
					allDone = false
				}
			}
			if allDone {
				return r.build()
			}
			r.now = t + 1
			for i := 0; i < r.n; i++ {
				if tasks[i].rel == t+1 {
					r.released[i] = true
				}
			}
			continue
		}
		cur := r.current
		r.rem[cur]--
		r.executed[cur]++
		r.segs = append(r.segs, mailboxSegment{index: cur, start: t, end: t + 1})
		if r.rem[cur] == 0 {
			r.pc[cur]++
		}
		r.now = t + 1
		for i := 0; i < r.n; i++ {
			if tasks[i].rel == t+1 {
				r.released[i] = true
			}
		}
	}

	// Boundary instant: drain the just-finished current task once.
	if r.current != -1 {
		i := r.current
		if !r.done[i] && r.blockedM[i] == "" && r.rem[i] == 0 {
			r.now = horizon
			r.settle(i)
		}
	}
	return r.build()
}

func (r *refMailSim) futureRelease() bool {
	for i := 0; i < r.n; i++ {
		if !r.released[i] {
			return true
		}
	}
	return false
}

func (r *refMailSim) settlePoint() bool {
	for {
		if r.current != -1 {
			r.settle(r.current)
			if r.done[r.current] || r.blockedM[r.current] != "" || r.rem[r.current] == 0 {
				r.current = -1
			}
		}
		best := r.pick()
		if r.current == -1 {
			if best == -1 {
				if !r.futureRelease() {
					allDone := true
					for i := 0; i < r.n; i++ {
						if !r.done[i] {
							allDone = false
						}
					}
					if !allDone {
						r.stalled = true
						return false
					}
				}
				return true
			}
			r.current = best
			r.settle(best)
			if r.done[best] || r.blockedM[best] != "" || r.rem[best] == 0 {
				r.current = -1
				continue
			}
		}
		resolved := false
		for !resolved {
			b := r.pick()
			if b != -1 && b != r.current && r.tasks[b].prio < r.tasks[r.current].prio {
				r.current = b
				r.settle(b)
				if r.done[b] || r.blockedM[b] != "" || r.rem[b] == 0 {
					r.current = -1
					break
				}
				continue
			}
			resolved = true
		}
		if r.current == -1 {
			continue
		}
		return true
	}
}

func (r *refMailSim) settle(i int) {
	for r.pc[i] < len(r.tasks[i].acts) {
		a := r.tasks[i].acts[r.pc[i]]
		switch a.kind {
		case mailActRun:
			if r.rem[i] == 0 {
				r.rem[i] = a.run
			}
			return
		case mailActSend:
			if ws := r.emptyWait[a.mailbox]; len(ws) > 0 {
				w := ws[0]
				for _, x := range ws[1:] {
					if r.blockTime[x] < r.blockTime[w] ||
						(r.blockTime[x] == r.blockTime[w] && x < w) {
						w = x
					}
				}
				r.emptyWait[a.mailbox] = removeTask(ws, w)
				r.blockedM[w] = ""
				r.received[w] = append(r.received[w], receivedMessage{
					Mailbox: a.mailbox, Message: a.message, Time: r.now,
				})
				r.pc[w]++
				r.pc[i]++
				continue
			}
			var cap int64
			for _, b := range r.boxes {
				if b.name == a.mailbox {
					cap = b.capacity
				}
			}
			q := r.queue[a.mailbox]
			if int64(len(q)) < cap {
				r.queue[a.mailbox] = append(q, a.message)
				r.pc[i]++
				continue
			}
			r.blockedM[i] = a.mailbox
			r.blockedK[i] = mailActSend
			r.blockTime[i] = r.now
			r.pending[i] = a.message
			r.fullWait[a.mailbox] = append(r.fullWait[a.mailbox], i)
			return
		case mailActReceive:
			q := r.queue[a.mailbox]
			if len(q) == 0 {
				r.blockedM[i] = a.mailbox
				r.blockedK[i] = mailActReceive
				r.blockTime[i] = r.now
				r.emptyWait[a.mailbox] = append(r.emptyWait[a.mailbox], i)
				return
			}
			msg := q[0]
			r.queue[a.mailbox] = q[1:]
			r.received[i] = append(r.received[i], receivedMessage{
				Mailbox: a.mailbox, Message: msg, Time: r.now,
			})
			if ss := r.fullWait[a.mailbox]; len(ss) > 0 {
				w := ss[0]
				for _, x := range ss[1:] {
					if r.blockTime[x] < r.blockTime[w] ||
						(r.blockTime[x] == r.blockTime[w] && x < w) {
						w = x
					}
				}
				r.fullWait[a.mailbox] = removeTask(ss, w)
				r.blockedM[w] = ""
				r.queue[a.mailbox] = append(r.queue[a.mailbox], r.pending[w])
				r.pending[w] = ""
				r.pc[w]++
			}
			r.pc[i]++
		}
	}
	r.done[i] = true
	at := r.now
	r.finish[i] = &at
}

func (r *refMailSim) pick() int {
	best := -1
	for i := 0; i < r.n; i++ {
		if !r.released[i] || r.done[i] || r.blockedM[i] != "" {
			continue
		}
		if best == -1 {
			best = i
			continue
		}
		a, bb := r.tasks[i], r.tasks[best]
		if a.prio < bb.prio ||
			(a.prio == bb.prio && a.rel < bb.rel) ||
			(a.prio == bb.prio && a.rel == bb.rel && i < best) {
			best = i
		}
	}
	return best
}

func (r *refMailSim) build() mailboxResponse {
	resp := mailboxResponse{Timeline: []mailboxTimelineInterval{}}
	for _, seg := range r.segs {
		last := len(resp.Timeline) - 1
		if last >= 0 &&
			resp.Timeline[last].TaskID == r.tasks[seg.index].id &&
			resp.Timeline[last].End == seg.start {
			resp.Timeline[last].End = seg.end
			continue
		}
		resp.Timeline = append(resp.Timeline, mailboxTimelineInterval{
			TaskID: r.tasks[seg.index].id, Start: seg.start, End: seg.end,
		})
	}
	resp.Results = make([]mailboxTaskResult, r.n)
	allDone := true
	for i, tk := range r.tasks {
		state := "ready"
		switch {
		case r.done[i]:
			state = "completed"
		case r.blockedM[i] != "":
			state = "blocked"
		case !r.released[i]:
			state = "unreleased"
		}
		if state != "completed" {
			allDone = false
		}
		var ba, bo *string
		if state == "blocked" {
			action := "send"
			if r.blockedK[i] == mailActReceive {
				action = "receive"
			}
			ba = &action
			m := r.blockedM[i]
			bo = &m
		}
		var ds string
		switch c := r.finish[i]; {
		case c != nil && *c <= tk.dead:
			ds = "met"
		case c != nil:
			ds = "missed"
		case tk.dead <= r.horizon:
			ds = "missed"
		default:
			ds = "pending"
		}
		recv := r.received[i]
		if recv == nil {
			recv = []receivedMessage{}
		}
		resp.Results[i] = mailboxTaskResult{
			Executed: r.executed[i], Completion: r.finish[i],
			State: state, DeadlineStatus: ds,
			BlockedAction: ba, BlockedOn: bo, Received: recv,
		}
	}
	switch {
	case r.stalled:
		resp.Status = "stalled"
	case allDone:
		resp.Status = "completed"
	default:
		resp.Status = "horizon"
	}
	resp.StoppedAt = r.now
	return resp
}

func TestMailboxDifferential(t *testing.T) {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	names := []string{"m0", "m1", "m2"}
	for iter := 0; iter < 3000; iter++ {
		n := 1 + rng.Intn(5)
		horizon := int64(2 + rng.Intn(20))
		gotBoxes := []mailboxDecl{{name: "m0", capacity: int64(1 + rng.Intn(3))}}
		refBoxes := []refMailbox{{name: "m0", capacity: gotBoxes[0].capacity}}
		for k := 1; k < 3; k++ {
			c := int64(1 + rng.Intn(3))
			gotBoxes = append(gotBoxes, mailboxDecl{name: names[k], capacity: c})
			refBoxes = append(refBoxes, refMailbox{name: names[k], capacity: c})
		}
		gotTasks := make([]mailboxTask, n)
		refTasks := make([]refMailTask, n)
		used := map[string]bool{}
		for i := 0; i < n; i++ {
			var id string
			for {
				id = "t" + strconv.Itoa(rng.Intn(40))
				if !used[id] {
					used[id] = true
					break
				}
			}
			prio := int64(rng.Intn(4))
			rel := int64(rng.Intn(int(horizon)))
			dead := rel + int64(1+rng.Intn(30))
			nacts := 1 + rng.Intn(9)
			acts := make([]mailboxAction, 0, nacts)
			for k := 0; k < nacts; k++ {
				switch rng.Intn(10) {
				case 0, 1, 2, 3:
					acts = append(acts, mailboxAction{kind: mailActRun, run: int64(1 + rng.Intn(5))})
				case 4, 5, 6:
					m := names[rng.Intn(len(names))]
					acts = append(acts, mailboxAction{kind: mailActSend, mailbox: m,
						message: "msg" + strconv.Itoa(rng.Intn(4))})
				default:
					m := names[rng.Intn(len(names))]
					acts = append(acts, mailboxAction{kind: mailActReceive, mailbox: m})
				}
			}
			gotTasks[i] = mailboxTask{id: id, priority: prio, release: rel, deadline: dead, actions: acts}
			refTasks[i] = refMailTask{id: id, prio: prio, rel: rel, dead: dead, acts: acts}
		}

		want := refMailRun(refBoxes, refTasks, horizon)
		got := simulateMailbox(gotBoxes, gotTasks, horizon)

		if got.Status != want.Status || got.StoppedAt != want.StoppedAt {
			t.Fatalf("iter %d seed %d: status %q@%d vs %q@%d\ngot %s\nref %s",
				iter, seed, got.Status, got.StoppedAt, want.Status, want.StoppedAt,
				tlString(got.Timeline), tlString(want.Timeline))
		}
		if len(got.Timeline) != len(want.Timeline) {
			t.Fatalf("iter %d seed %d: timeline len %d vs %d\ngot %s\nref %s",
				iter, seed, len(got.Timeline), len(want.Timeline),
				tlString(got.Timeline), tlString(want.Timeline))
		}
		for k := range want.Timeline {
			if got.Timeline[k] != want.Timeline[k] {
				t.Fatalf("iter %d seed %d: tl[%d] %+v vs %+v",
					iter, seed, k, got.Timeline[k], want.Timeline[k])
			}
		}
		for i := range want.Results {
			g, w := got.Results[i], want.Results[i]
			if g.Executed != w.Executed || g.State != w.State || g.DeadlineStatus != w.DeadlineStatus {
				t.Fatalf("iter %d seed %d: %s result %+v vs %+v", iter, seed, refTasks[i].id, g, w)
			}
			if (g.Completion == nil) != (w.Completion == nil) ||
				(g.Completion != nil && *g.Completion != *w.Completion) {
				t.Fatalf("iter %d seed %d: %s completion %v vs %v", iter, seed, refTasks[i].id, g.Completion, w.Completion)
			}
			if (g.BlockedAction == nil) != (w.BlockedAction == nil) ||
				(g.BlockedAction != nil && *g.BlockedAction != *w.BlockedAction) {
				t.Fatalf("iter %d seed %d: %s blockedAction %v vs %v", iter, seed, refTasks[i].id, g.BlockedAction, w.BlockedAction)
			}
			if (g.BlockedOn == nil) != (w.BlockedOn == nil) ||
				(g.BlockedOn != nil && *g.BlockedOn != *w.BlockedOn) {
				t.Fatalf("iter %d seed %d: %s blockedOn %v vs %v", iter, seed, refTasks[i].id, g.BlockedOn, w.BlockedOn)
			}
			if len(g.Received) != len(w.Received) {
				t.Fatalf("iter %d seed %d: %s received %+v vs %+v", iter, seed, refTasks[i].id, g.Received, w.Received)
			}
			for k := range w.Received {
				if g.Received[k] != w.Received[k] {
					t.Fatalf("iter %d seed %d: %s received[%d] %+v vs %+v",
						iter, seed, refTasks[i].id, k, g.Received[k], w.Received[k])
				}
			}
		}
	}
}
