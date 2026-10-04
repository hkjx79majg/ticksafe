package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func postStack(t *testing.T, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, stackAnalyzePath, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	Handler().ServeHTTP(rec, r)
	return rec
}

func stackOK(t *testing.T, body string) stackAnalyzeResponse {
	t.Helper()
	rec := postStack(t, body, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp stackAnalyzeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return resp
}

func sf(id string, frame int64, calls ...string) string {
	var callsJSON string
	if calls == nil {
		// Explicitly absent calls list.
		return `{"id":` + strconv.Quote(id) + `,"frameBytes":` + strconv.FormatInt(frame, 10) + `}`
	}
	callsJSON = `,"calls":[` + strings.Join(calls, ",") + `]`
	return `{"id":` + strconv.Quote(id) + `,"frameBytes":` + strconv.FormatInt(frame, 10) + callsJSON + `}`
}

func sc(callee string, overhead int64) string {
	return `{"callee":` + strconv.Quote(callee) +
		`,"overheadBytes":` + strconv.FormatInt(overhead, 10) + `}`
}

func se(entry string, limit int64) string {
	return `{"entry":` + strconv.Quote(entry) + `,"stackLimit":` + strconv.FormatInt(limit, 10) + `}`
}

func stackReq(funcs string, entries ...string) string {
	return `{"functions":[` + funcs + `],"entrypoints":[` + strings.Join(entries, ",") + `]}`
}

func requireStackErr(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, status, rec.Body.String())
	}
	var payload struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("error body is not JSON: %v", err)
	}
	if payload.Error.Code != code {
		t.Fatalf("error code = %q, want %q", payload.Error.Code, code)
	}
}

func TestStackSimpleChain(t *testing.T) {
	// A(10) --2--> B(3): required = 10 + 2 + 3 = 15.
	body := stackReq(
		sf("A", 10, sc("B", 2))+","+sf("B", 3),
		se("A", 15))
	resp := stackOK(t, body)
	if !resp.Analyzable || !resp.Safe {
		t.Fatalf("safe=%v analyzable=%v, want true/true", resp.Safe, resp.Analyzable)
	}
	r0 := resp.Results[0]
	if r0.Entry != "A" || r0.StackLimit != 15 || r0.RequiredBytes == nil || *r0.RequiredBytes != 15 ||
		r0.Status != "within_limit" {
		t.Fatalf("r0 = %+v", r0)
	}
	if got := strings.Join(r0.WorstPath, ","); got != "A,B" {
		t.Fatalf("worstPath = %v, want [A B]", r0.WorstPath)
	}
	if len(r0.Cycle) != 0 {
		t.Fatalf("cycle = %v, want empty", r0.Cycle)
	}

	// One byte less budget flips the status without changing requiredBytes.
	over := stackOK(t, stackReq(sf("A", 10, sc("B", 2))+","+sf("B", 3), se("A", 14))).Results[0]
	if over.Status != "exceeded" || over.RequiredBytes == nil || *over.RequiredBytes != 15 {
		t.Fatalf("over = %+v, want exceeded/15", over)
	}
}

func TestStackTieBreaksByCallsOrder(t *testing.T) {
	// A(1) -> B(2) and C(2) at equal overhead: the first call wins.
	body := stackReq(
		sf("A", 1, sc("B", 0), sc("C", 0))+","+
			sf("B", 2)+","+sf("C", 2),
		se("A", 100))
	resp := stackOK(t, body)
	if got := strings.Join(resp.Results[0].WorstPath, ","); got != "A,B" {
		t.Fatalf("worstPath = %v, want [A B]", resp.Results[0].WorstPath)
	}

	// Equal total through differing overhead/frame splits still favors call order.
	body = stackReq(
		sf("A", 1, sc("B", 1), sc("C", 0))+","+
			sf("B", 1)+","+sf("C", 2),
		se("A", 100))
	resp = stackOK(t, body)
	if got := strings.Join(resp.Results[0].WorstPath, ","); got != "A,B" {
		t.Fatalf("worstPath = %v, want [A B] on equal weight", resp.Results[0].WorstPath)
	}
}

func TestStackDiamondSharesDeepestPath(t *testing.T) {
	// A -> B -> D(40) and A -> C -> D; B and C frames 1, A 1.
	body := stackReq(
		sf("A", 1, sc("B", 0), sc("C", 0))+","+
			sf("B", 1, sc("D", 0))+","+
			sf("C", 1, sc("D", 0))+","+
			sf("D", 40),
		se("A", 100))
	resp := stackOK(t, body)
	r := resp.Results[0]
	if r.RequiredBytes == nil || *r.RequiredBytes != 42 {
		t.Fatalf("required = %v, want 42", *r.RequiredBytes)
	}
	if got := strings.Join(r.WorstPath, ","); got != "A,B,D" {
		t.Fatalf("worstPath = %v, want [A B D]", r.WorstPath)
	}
}

func TestStackZeroWeightContinuationTiesWithStop(t *testing.T) {
	// B has frame 0 and the edge adds 0, so extending past A ties with stopping;
	// the empty continuation sorts first and A alone is the worst path.
	body := stackReq(
		sf("A", 5, sc("B", 0))+","+sf("B", 0),
		se("A", 5))
	resp := stackOK(t, body)
	r := resp.Results[0]
	if r.RequiredBytes == nil || *r.RequiredBytes != 5 || r.Status != "within_limit" {
		t.Fatalf("r = %+v, want finite 5 within_limit", r)
	}
	if got := strings.Join(r.WorstPath, ","); got != "A" {
		t.Fatalf("worstPath = %v, want [A]", r.WorstPath)
	}
}

func TestStackMissingCallsAllowed(t *testing.T) {
	body := stackReq(sf("Lone", 7), se("Lone", 7))
	resp := stackOK(t, body)
	r := resp.Results[0]
	if r.RequiredBytes == nil || *r.RequiredBytes != 7 || r.Status != "within_limit" {
		t.Fatalf("r = %+v", r)
	}
	if got := strings.Join(r.WorstPath, ","); got != "Lone" {
		t.Fatalf("worstPath = %v, want [Lone]", r.WorstPath)
	}
}

func TestStackSelfLoop(t *testing.T) {
	body := stackReq(sf("A", 1, sc("A", 0)), se("A", 10))
	resp := stackOK(t, body)
	if resp.Safe || resp.Analyzable {
		t.Fatalf("safe=%v analyzable=%v, want false/false", resp.Safe, resp.Analyzable)
	}
	r := resp.Results[0]
	if r.Status != "unbounded" || r.RequiredBytes != nil {
		t.Fatalf("r = %+v, want unbounded/null required", r)
	}
	if got := strings.Join(r.Cycle, ","); got != "A" {
		t.Fatalf("cycle = %v, want [A]", r.Cycle)
	}
	if len(r.WorstPath) != 0 {
		t.Fatalf("worstPath = %v, want empty", r.WorstPath)
	}
}

func TestStackCycleFromBackEdgeTarget(t *testing.T) {
	// A -> B -> C -> B: back edge C -> B, cycle starts at target B.
	body := stackReq(
		sf("A", 1, sc("B", 0))+","+
			sf("B", 1, sc("C", 0))+","+
			sf("C", 1, sc("B", 0)),
		se("A", 10))
	resp := stackOK(t, body)
	r := resp.Results[0]
	if r.Status != "unbounded" {
		t.Fatalf("status = %q, want unbounded", r.Status)
	}
	if got := strings.Join(r.Cycle, ","); got != "B,C" {
		t.Fatalf("cycle = %v, want [B C]", r.Cycle)
	}
}

func TestStackCycleFollowsCallsOrderDFS(t *testing.T) {
	// Same cycle {B,C}, reached in opposite orders: the first back edge differs.
	bodyAB := stackReq(
		sf("E", 0, sc("A", 0), sc("X", 0))+","+
			sf("A", 0, sc("B", 0), sc("C", 0))+","+
			sf("B", 0, sc("C", 0))+","+
			sf("C", 0, sc("B", 0))+","+
			sf("X", 9),
		se("E", 10))
	if got := strings.Join(stackOK(t, bodyAB).Results[0].Cycle, ","); got != "B,C" {
		t.Fatalf("cycle = %v, want [B C] via back edge C->B", got)
	}

	bodyAC := stackReq(
		sf("E", 0, sc("A", 0))+","+
			sf("A", 0, sc("C", 0), sc("B", 0))+","+
			sf("B", 0, sc("C", 0))+","+
			sf("C", 0, sc("B", 0)),
		se("E", 10))
	if got := strings.Join(stackOK(t, bodyAC).Results[0].Cycle, ","); got != "C,B" {
		t.Fatalf("cycle = %v, want [C B] via back edge B->C", got)
	}
}

func TestStackUnreachableCycleIgnored(t *testing.T) {
	// Entry E reaches only the finite branch X; the B<->C loop is unreachable
	// and must not affect E's result or the top-level flags.
	body := stackReq(
		sf("E", 2, sc("X", 1))+","+sf("X", 3)+","+
			sf("B", 1, sc("C", 0))+","+sf("C", 1, sc("B", 0)),
		se("E", 100))
	resp := stackOK(t, body)
	if !resp.Safe || !resp.Analyzable {
		t.Fatalf("safe=%v analyzable=%v, want true/true", resp.Safe, resp.Analyzable)
	}
	r0 := resp.Results[0]
	if r0.Status != "within_limit" || r0.RequiredBytes == nil || *r0.RequiredBytes != 6 {
		t.Fatalf("r0 = %+v, want finite 6", r0)
	}
	if got := strings.Join(r0.WorstPath, ","); got != "E,X" {
		t.Fatalf("worstPath = %v, want [E X]", r0.WorstPath)
	}
	if len(r0.Cycle) != 0 {
		t.Fatalf("r0 cycle = %v, want empty", r0.Cycle)
	}

	// The same graph analyzed from B hits the reachable loop and is unbounded.
	body = stackReq(
		sf("E", 2, sc("X", 1))+","+sf("X", 3)+","+
			sf("B", 1, sc("C", 0))+","+sf("C", 1, sc("B", 0)),
		se("B", 100))
	if r := stackOK(t, body).Results[0]; r.Status != "unbounded" {
		t.Fatalf("B status = %q, want unbounded", r.Status)
	}
}

func TestStackFlagsAggregateAcrossEntries(t *testing.T) {
	// Entries: within, exceeded, unbounded -> safe false, analyzable false.
	body := stackReq(
		sf("Good", 1)+","+
			sf("Bad", 10)+","+
			sf("Rec", 1, sc("Rec", 0)),
		se("Good", 10), se("Bad", 5), se("Rec", 5))
	resp := stackOK(t, body)
	if resp.Safe || resp.Analyzable {
		t.Fatalf("safe=%v analyzable=%v, want false/false", resp.Safe, resp.Analyzable)
	}
	statuses := []string{resp.Results[0].Status, resp.Results[1].Status, resp.Results[2].Status}
	if got := strings.Join(statuses, ","); got != "within_limit,exceeded,unbounded" {
		t.Fatalf("statuses = %v", statuses)
	}

	// Only an exceeded entry: analyzable stays true.
	body = stackReq(sf("Bad", 10), se("Bad", 5))
	resp = stackOK(t, body)
	if resp.Safe || !resp.Analyzable {
		t.Fatalf("safe=%v analyzable=%v, want false/true", resp.Safe, resp.Analyzable)
	}
}

func TestStackBoundsAccepted(t *testing.T) {
	// frameBytes 0 and 1e12, overhead 1e12; limits 1 and 1e16.
	body := stackReq(
		sf("Z", 0)+","+
			sf("Max", maxFrameBytes, sc("Z", maxOverheadBytes)),
		se("Z", 1), se("Max", maxStackLimit))
	resp := stackOK(t, body)
	if resp.Results[0].Status != "within_limit" {
		t.Fatalf("zero-frame entry: %+v", resp.Results[0])
	}
	r1 := resp.Results[1]
	// Max(1e12) --1e12--> Z(0): required = 2e12.
	if r1.RequiredBytes == nil || *r1.RequiredBytes != 2*maxFrameBytes {
		t.Fatalf("max entry required = %v, want %d", *r1.RequiredBytes, 2*maxFrameBytes)
	}
	if r1.Status != "within_limit" {
		t.Fatalf("max entry status = %q, want within_limit", r1.Status)
	}
}

func TestStackErrors(t *testing.T) {
	base := stackReq(sf("A", 1), se("A", 1))
	cases := map[string]struct {
		body   string
		status int
		code   string
	}{
		"missing functions":     {`{"entrypoints":[{"entry":"A","stackLimit":1}]}`, 422, "validation_failed"},
		"empty functions":       {`{"functions":[],"entrypoints":[{"entry":"A","stackLimit":1}]}`, 422, "validation_failed"},
		"missing entrypoints":   {`{"functions":[{"id":"A","frameBytes":1}]}`, 422, "validation_failed"},
		"empty entrypoints":     {`{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[]}`, 422, "validation_failed"},
		"missing frameBytes":    {`{"functions":[{"id":"A"}],"entrypoints":[{"entry":"A","stackLimit":1}]}`, 422, "validation_failed"},
		"negative frameBytes":   {`{"functions":[{"id":"A","frameBytes":-1}],"entrypoints":[{"entry":"A","stackLimit":1}]}`, 422, "validation_failed"},
		"frameBytes too large":  {`{"functions":[{"id":"A","frameBytes":1000000000001}],"entrypoints":[{"entry":"A","stackLimit":1}]}`, 422, "validation_failed"},
		"frameBytes fraction":   {`{"functions":[{"id":"A","frameBytes":1.5}],"entrypoints":[{"entry":"A","stackLimit":1}]}`, 422, "validation_failed"},
		"negative overhead":     {`{"functions":[{"id":"A","frameBytes":1,"calls":[{"callee":"A","overheadBytes":-1}]}],"entrypoints":[{"entry":"A","stackLimit":1}]}`, 422, "validation_failed"},
		"overhead too large":    {`{"functions":[{"id":"A","frameBytes":1,"calls":[{"callee":"A","overheadBytes":1000000000001}]}],"entrypoints":[{"entry":"A","stackLimit":1}]}`, 422, "validation_failed"},
		"missing overhead":      {`{"functions":[{"id":"A","frameBytes":1,"calls":[{"callee":"A"}]}],"entrypoints":[{"entry":"A","stackLimit":1}]}`, 422, "validation_failed"},
		"unknown callee":        {`{"functions":[{"id":"A","frameBytes":1,"calls":[{"callee":"B","overheadBytes":0}]}],"entrypoints":[{"entry":"A","stackLimit":1}]}`, 422, "validation_failed"},
		"unknown entry":         {`{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[{"entry":"Z","stackLimit":1}]}`, 422, "validation_failed"},
		"duplicate function id": {`{"functions":[{"id":"A","frameBytes":1},{"id":"A","frameBytes":2}],"entrypoints":[{"entry":"A","stackLimit":1}]}`, 422, "validation_failed"},
		"duplicate entry":       {`{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[{"entry":"A","stackLimit":1},{"entry":"A","stackLimit":2}]}`, 422, "validation_failed"},
		"limit zero":            {`{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[{"entry":"A","stackLimit":0}]}`, 422, "validation_failed"},
		"limit negative":        {`{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[{"entry":"A","stackLimit":-1}]}`, 422, "validation_failed"},
		"limit too large":       {`{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[{"entry":"A","stackLimit":10000000000000001}]}`, 422, "validation_failed"},
		"whitespace id":         {`{"functions":[{"id":"A B","frameBytes":1}],"entrypoints":[{"entry":"A B","stackLimit":1}]}`, 422, "validation_failed"},
		"unknown top field":     {`{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[{"entry":"A","stackLimit":1}],"bogus":1}`, 400, "invalid_json"},
		"unknown call field":    {`{"functions":[{"id":"A","frameBytes":1,"calls":[{"callee":"A","overheadBytes":0,"x":1}]}],"entrypoints":[{"entry":"A","stackLimit":1}]}`, 400, "invalid_json"},
		"bad syntax":            {`{"functions":[}`, 400, "invalid_json"},
		"trailing content":      {base + " {}", 400, "invalid_json"},
		"duplicate key":         {`{"functions":[{"id":"A","frameBytes":1,"frameBytes":2}],"entrypoints":[{"entry":"A","stackLimit":1}]}`, 400, "invalid_json"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			requireStackErr(t, postStack(t, tc.body, "application/json"), tc.status, tc.code)
		})
	}

	// 4097 functions and 257 entrypoints exceed the quantity bounds.
	var sb strings.Builder
	sb.WriteString(`{"functions":[`)
	for i := 0; i < maxStackFunctions+1; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"id":"F` + strconv.Itoa(i) + `","frameBytes":0}`)
	}
	sb.WriteString(`],"entrypoints":[{"entry":"F0","stackLimit":1}]}`)
	requireStackErr(t, postStack(t, sb.String(), "application/json"), 422, "validation_failed")

	// 257 calls on one function.
	sb.Reset()
	sb.WriteString(`{"functions":[{"id":"A","frameBytes":0,"calls":[`)
	for i := 0; i < maxStackCalls+1; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"callee":"A","overheadBytes":0}`)
	}
	sb.WriteString(`]}],"entrypoints":[{"entry":"A","stackLimit":1}]}`)
	requireStackErr(t, postStack(t, sb.String(), "application/json"), 422, "validation_failed")
}

func TestStackMethodAndMediaType(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, stackAnalyzePath, nil))
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("GET: status = %d allow = %q", rec.Code, rec.Header().Get("Allow"))
	}
	requireStackErr(t, rec, 405, "method_not_allowed")

	requireStackErr(t, postStack(t, `{}`, "text/plain"), 415, "unsupported_media_type")
	requireStackErr(t, postStack(t, `{}`, ""), 415, "unsupported_media_type")
	// Media type parameters are allowed.
	rec = postStack(t, stackReq(sf("A", 1), se("A", 1)), "application/json; charset=utf-8")
	if rec.Code != http.StatusOK {
		t.Fatalf("json with charset: status = %d, want 200", rec.Code)
	}
}

func TestStackDeterministic(t *testing.T) {
	body := stackReq(
		sf("A", 3, sc("B", 1), sc("C", 2))+","+
			sf("B", 4, sc("D", 0))+","+
			sf("C", 1, sc("D", 1))+","+
			sf("D", 5),
		se("A", 100))
	first := postStack(t, body, "application/json")
	second := postStack(t, body, "application/json")
	if first.Body.String() != second.Body.String() {
		t.Fatalf("responses differ:\n%s\n%s", first.Body.String(), second.Body.String())
	}
	resp := stackOK(t, body)
	// Both paths: A-B-D = 3+1+4+0+5 = 13; A-C-D = 3+2+1+1+5 = 12 -> B branch.
	r := resp.Results[0]
	if r.RequiredBytes == nil || *r.RequiredBytes != 13 {
		t.Fatalf("required = %v, want 13", r.RequiredBytes)
	}
	if got := strings.Join(r.WorstPath, ","); got != "A,B,D" {
		t.Fatalf("worstPath = %v, want [A B D]", r.WorstPath)
	}
}
