package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func postStacks(t *testing.T, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, stackAnalyzePath, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	Handler().ServeHTTP(rec, r)
	return rec
}

func stacksOK(t *testing.T, body string) stackResponse {
	t.Helper()
	rec := postStacks(t, body, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp stackResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return resp
}

func stacksError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantCode string) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, wantStatus, rec.Body.String())
	}
	var body map[string]map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body is not JSON: %v", err)
	}
	if body["error"]["code"] != wantCode {
		t.Fatalf("error code = %q, want %q; body: %s", body["error"]["code"], wantCode, rec.Body.String())
	}
}

func TestStackSimpleChainWithinLimit(t *testing.T) {
	// A(10) -> B(20) overhead 3, B -> C(5) overhead 2: 10+3+20+2+5 = 40.
	resp := stacksOK(t, `{
		"functions": [
			{"id":"A","frameBytes":10},
			{"id":"B","frameBytes":20},
			{"id":"C","frameBytes":5}
		],
		"calls": [
			{"caller":"A","callee":"B","overheadBytes":3},
			{"caller":"B","callee":"C","overheadBytes":2}
		],
		"entrypoints": [{"entry":"A","stackLimit":100}]
	}`)
	if !resp.Safe || !resp.Analyzable {
		t.Fatalf("safe/analyzable = %v/%v, want true/true", resp.Safe, resp.Analyzable)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("results = %+v", resp.Results)
	}
	r := resp.Results[0]
	if r.Entry != "A" || r.StackLimit != 100 || r.Status != "within_limit" {
		t.Fatalf("result = %+v", r)
	}
	if r.RequiredBytes == nil || *r.RequiredBytes != 40 {
		t.Fatalf("requiredBytes = %v, want 40", r.RequiredBytes)
	}
	if got := strings.Join(r.WorstPath, ","); got != "A,B,C" {
		t.Fatalf("worstPath = %v, want [A B C]", r.WorstPath)
	}
	if len(r.Cycle) != 0 {
		t.Fatalf("cycle = %v, want empty", r.Cycle)
	}
}

func TestStackExceeded(t *testing.T) {
	resp := stacksOK(t, `{
		"functions": [{"id":"A","frameBytes":10},{"id":"B","frameBytes":20}],
		"calls": [{"caller":"A","callee":"B","overheadBytes":3}],
		"entrypoints": [{"entry":"A","stackLimit":32}]
	}`)
	if resp.Safe || !resp.Analyzable {
		t.Fatalf("safe/analyzable = %v/%v, want false/true", resp.Safe, resp.Analyzable)
	}
	r := resp.Results[0]
	if r.Status != "exceeded" || r.RequiredBytes == nil || *r.RequiredBytes != 33 {
		t.Fatalf("result = %+v, want exceeded/33", r)
	}
}

func TestStackWorstPathPicksMaxCost(t *testing.T) {
	// A -> B (cost 10+1+100) vs A -> C (cost 10+1+50): B branch wins.
	resp := stacksOK(t, `{
		"functions": [
			{"id":"A","frameBytes":10},
			{"id":"B","frameBytes":100},
			{"id":"C","frameBytes":50}
		],
		"calls": [
			{"caller":"A","callee":"C","overheadBytes":1},
			{"caller":"A","callee":"B","overheadBytes":1}
		],
		"entrypoints": [{"entry":"A","stackLimit":1000}]
	}`)
	r := resp.Results[0]
	if r.RequiredBytes == nil || *r.RequiredBytes != 111 {
		t.Fatalf("requiredBytes = %v, want 111", r.RequiredBytes)
	}
	if got := strings.Join(r.WorstPath, ","); got != "A,B" {
		t.Fatalf("worstPath = %v, want [A B]", r.WorstPath)
	}
}

func TestStackTieBreaksByCallsOrder(t *testing.T) {
	// Both branches cost 10+1+50; the earlier call (A->C) wins the tie.
	resp := stacksOK(t, `{
		"functions": [
			{"id":"A","frameBytes":10},
			{"id":"B","frameBytes":50},
			{"id":"C","frameBytes":50}
		],
		"calls": [
			{"caller":"A","callee":"C","overheadBytes":1},
			{"caller":"A","callee":"B","overheadBytes":1}
		],
		"entrypoints": [{"entry":"A","stackLimit":1000}]
	}`)
	r := resp.Results[0]
	if r.RequiredBytes == nil || *r.RequiredBytes != 61 {
		t.Fatalf("requiredBytes = %v, want 61", r.RequiredBytes)
	}
	if got := strings.Join(r.WorstPath, ","); got != "A,C" {
		t.Fatalf("worstPath = %v, want [A C]", r.WorstPath)
	}
}

func TestStackTieBreaksLevelByLevel(t *testing.T) {
	// A->B and A->C tie at the top level (both 10+0+20=30 via leaves), so the
	// earlier call A->B wins even though B's own subtree also has a tie that
	// resolves to the earlier B->D edge.
	resp := stacksOK(t, `{
		"functions": [
			{"id":"A","frameBytes":10},
			{"id":"B","frameBytes":5},
			{"id":"C","frameBytes":20},
			{"id":"D","frameBytes":15},
			{"id":"E","frameBytes":15}
		],
		"calls": [
			{"caller":"A","callee":"B","overheadBytes":0},
			{"caller":"A","callee":"C","overheadBytes":0},
			{"caller":"B","callee":"D","overheadBytes":0},
			{"caller":"B","callee":"E","overheadBytes":0}
		],
		"entrypoints": [{"entry":"A","stackLimit":1000}]
	}`)
	r := resp.Results[0]
	// A(10)+B(5)+D(15) = 30; A(10)+C(20) = 30: tie at level 1 -> A->B first.
	if r.RequiredBytes == nil || *r.RequiredBytes != 30 {
		t.Fatalf("requiredBytes = %v, want 30", r.RequiredBytes)
	}
	if got := strings.Join(r.WorstPath, ","); got != "A,B,D" {
		t.Fatalf("worstPath = %v, want [A B D]", r.WorstPath)
	}
}

func TestStackRecursionIsUnbounded(t *testing.T) {
	// A -> B -> C -> B: DFS from A finds back edge C->B, cycle [B C].
	resp := stacksOK(t, `{
		"functions": [
			{"id":"A","frameBytes":1},
			{"id":"B","frameBytes":1},
			{"id":"C","frameBytes":1}
		],
		"calls": [
			{"caller":"A","callee":"B","overheadBytes":0},
			{"caller":"B","callee":"C","overheadBytes":0},
			{"caller":"C","callee":"B","overheadBytes":0}
		],
		"entrypoints": [{"entry":"A","stackLimit":1000}]
	}`)
	if resp.Safe || resp.Analyzable {
		t.Fatalf("safe/analyzable = %v/%v, want false/false", resp.Safe, resp.Analyzable)
	}
	r := resp.Results[0]
	if r.Status != "unbounded" {
		t.Fatalf("status = %q, want unbounded", r.Status)
	}
	if r.RequiredBytes != nil {
		t.Fatalf("requiredBytes = %v, want null", *r.RequiredBytes)
	}
	if len(r.WorstPath) != 0 {
		t.Fatalf("worstPath = %v, want empty", r.WorstPath)
	}
	if got := strings.Join(r.Cycle, ","); got != "B,C" {
		t.Fatalf("cycle = %v, want [B C]", r.Cycle)
	}
}

func TestStackSelfLoopIsUnbounded(t *testing.T) {
	resp := stacksOK(t, `{
		"functions": [{"id":"A","frameBytes":1}],
		"calls": [{"caller":"A","callee":"A","overheadBytes":0}],
		"entrypoints": [{"entry":"A","stackLimit":10}]
	}`)
	r := resp.Results[0]
	if r.Status != "unbounded" || len(r.Cycle) != 1 || r.Cycle[0] != "A" {
		t.Fatalf("result = %+v, want unbounded with cycle [A]", r)
	}
}

func TestStackFirstBackEdgeInDFSOrder(t *testing.T) {
	// From A, calls order explores A->B first; B->D, D->B closes [B D] before
	// the A->C branch (which also closes a cycle) is ever explored.
	resp := stacksOK(t, `{
		"functions": [
			{"id":"A","frameBytes":1},
			{"id":"B","frameBytes":1},
			{"id":"C","frameBytes":1},
			{"id":"D","frameBytes":1}
		],
		"calls": [
			{"caller":"A","callee":"B","overheadBytes":0},
			{"caller":"A","callee":"C","overheadBytes":0},
			{"caller":"B","callee":"D","overheadBytes":0},
			{"caller":"D","callee":"B","overheadBytes":0},
			{"caller":"C","callee":"A","overheadBytes":0}
		],
		"entrypoints": [{"entry":"A","stackLimit":10}]
	}`)
	r := resp.Results[0]
	if r.Status != "unbounded" {
		t.Fatalf("status = %q, want unbounded", r.Status)
	}
	if got := strings.Join(r.Cycle, ","); got != "B,D" {
		t.Fatalf("cycle = %v, want [B D]", r.Cycle)
	}
}

func TestStackUnreachableCycleIgnored(t *testing.T) {
	// X<->Y cycle is unreachable from entry A, so A stays finite.
	resp := stacksOK(t, `{
		"functions": [
			{"id":"A","frameBytes":4},
			{"id":"B","frameBytes":6},
			{"id":"X","frameBytes":1},
			{"id":"Y","frameBytes":1}
		],
		"calls": [
			{"caller":"A","callee":"B","overheadBytes":2},
			{"caller":"X","callee":"Y","overheadBytes":0},
			{"caller":"Y","callee":"X","overheadBytes":0}
		],
		"entrypoints": [{"entry":"A","stackLimit":100}]
	}`)
	if !resp.Safe || !resp.Analyzable {
		t.Fatalf("safe/analyzable = %v/%v, want true/true", resp.Safe, resp.Analyzable)
	}
	r := resp.Results[0]
	if r.Status != "within_limit" || r.RequiredBytes == nil || *r.RequiredBytes != 12 {
		t.Fatalf("result = %+v, want within_limit/12", r)
	}
}

func TestStackDiamondCountsSharedCalleeOnce(t *testing.T) {
	// A -> B -> D and A -> C -> D: worst is max(10+1+20+1+5, 10+1+30+1+5).
	resp := stacksOK(t, `{
		"functions": [
			{"id":"A","frameBytes":10},
			{"id":"B","frameBytes":20},
			{"id":"C","frameBytes":30},
			{"id":"D","frameBytes":5}
		],
		"calls": [
			{"caller":"A","callee":"B","overheadBytes":1},
			{"caller":"A","callee":"C","overheadBytes":1},
			{"caller":"B","callee":"D","overheadBytes":1},
			{"caller":"C","callee":"D","overheadBytes":1}
		],
		"entrypoints": [{"entry":"A","stackLimit":1000}]
	}`)
	r := resp.Results[0]
	if r.RequiredBytes == nil || *r.RequiredBytes != 47 {
		t.Fatalf("requiredBytes = %v, want 47", r.RequiredBytes)
	}
	if got := strings.Join(r.WorstPath, ","); got != "A,C,D" {
		t.Fatalf("worstPath = %v, want [A C D]", r.WorstPath)
	}
}

func TestStackMultipleEntrypoints(t *testing.T) {
	// A is finite and fits; B is finite but exceeds; C recurses.
	resp := stacksOK(t, `{
		"functions": [
			{"id":"A","frameBytes":5},
			{"id":"B","frameBytes":50},
			{"id":"C","frameBytes":1}
		],
		"calls": [{"caller":"C","callee":"C","overheadBytes":0}],
		"entrypoints": [
			{"entry":"A","stackLimit":10},
			{"entry":"B","stackLimit":10},
			{"entry":"C","stackLimit":10}
		]
	}`)
	if resp.Safe || resp.Analyzable {
		t.Fatalf("safe/analyzable = %v/%v, want false/false", resp.Safe, resp.Analyzable)
	}
	if len(resp.Results) != 3 {
		t.Fatalf("results = %+v", resp.Results)
	}
	if resp.Results[0].Status != "within_limit" || *resp.Results[0].RequiredBytes != 5 {
		t.Fatalf("A result = %+v", resp.Results[0])
	}
	if resp.Results[1].Status != "exceeded" || *resp.Results[1].RequiredBytes != 50 {
		t.Fatalf("B result = %+v", resp.Results[1])
	}
	if resp.Results[2].Status != "unbounded" || resp.Results[2].RequiredBytes != nil {
		t.Fatalf("C result = %+v", resp.Results[2])
	}
}

func TestStackLeafFunctionOnly(t *testing.T) {
	// No calls at all: requiredBytes is the entry's own frame.
	resp := stacksOK(t, `{
		"functions": [{"id":"main","frameBytes":128}],
		"entrypoints": [{"entry":"main","stackLimit":128}]
	}`)
	r := resp.Results[0]
	if !resp.Safe || r.Status != "within_limit" || r.RequiredBytes == nil || *r.RequiredBytes != 128 {
		t.Fatalf("result = %+v, want within_limit/128", r)
	}
	if got := strings.Join(r.WorstPath, ","); got != "main" {
		t.Fatalf("worstPath = %v, want [main]", r.WorstPath)
	}
}

func TestStackDeterministicAcrossRequests(t *testing.T) {
	body := `{
		"functions": [
			{"id":"A","frameBytes":10},
			{"id":"B","frameBytes":20},
			{"id":"C","frameBytes":20}
		],
		"calls": [
			{"caller":"A","callee":"B","overheadBytes":1},
			{"caller":"A","callee":"C","overheadBytes":1}
		],
		"entrypoints": [{"entry":"A","stackLimit":100},{"entry":"B","stackLimit":10}]
	}`
	first := postStacks(t, body, "application/json").Body.String()
	for i := 0; i < 5; i++ {
		if got := postStacks(t, body, "application/json").Body.String(); got != first {
			t.Fatalf("response differs across requests:\n%s\n%s", first, got)
		}
	}
}

func TestStackMethodNotAllowed(t *testing.T) {
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, stackAnalyzePath, nil)
	Handler().ServeHTTP(rec, r)
	stacksError(t, rec, http.StatusMethodNotAllowed, "method_not_allowed")
	if allow := rec.Header().Get("Allow"); allow != http.MethodPost {
		t.Fatalf("Allow = %q, want POST", allow)
	}
}

func TestStackUnsupportedMediaType(t *testing.T) {
	stacksError(t, postStacks(t, `{}`, "text/plain"), http.StatusUnsupportedMediaType, "unsupported_media_type")
	stacksError(t, postStacks(t, `{}`, ""), http.StatusUnsupportedMediaType, "unsupported_media_type")
}

func TestStackInvalidJSON(t *testing.T) {
	valid := `{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[{"entry":"A","stackLimit":1}]}`
	cases := map[string]string{
		"syntax error":   `{"functions":`,
		"trailing":       valid + ` {}`,
		"duplicate key":  `{"functions":[{"id":"A","id":"B","frameBytes":1}],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
		"unknown field":  `{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[{"entry":"A","stackLimit":1}],"extra":1}`,
		"unknown nested": `{"functions":[{"id":"A","frameBytes":1,"x":1}],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
	}
	for name, body := range cases {
		stacksError(t, postStacks(t, body, "application/json"), http.StatusBadRequest, "invalid_json")
		_ = name
	}
	// Body over 1 MiB.
	big := `{"functions":[{"id":"` + strings.Repeat("a", 1<<20) + `","frameBytes":1}],"entrypoints":[]}`
	stacksError(t, postStacks(t, big, "application/json"), http.StatusBadRequest, "invalid_json")
}

func TestStackValidationFailed(t *testing.T) {
	cases := []string{
		// Missing or empty containers.
		`{}`,
		`{"entrypoints":[{"entry":"A","stackLimit":1}]}`,
		`{"functions":[],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
		`{"functions":[{"id":"A","frameBytes":1}]}`,
		`{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[]}`,
		// Function field violations.
		`{"functions":[{"id":"A"}],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
		`{"functions":[{"id":"A","frameBytes":-1}],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
		`{"functions":[{"id":"A","frameBytes":1000000000001}],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
		`{"functions":[{"id":"A","frameBytes":1.5}],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
		`{"functions":[{"id":"","frameBytes":1}],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
		`{"functions":[{"id":"A B","frameBytes":1}],"entrypoints":[{"entry":"A B","stackLimit":1}]}`,
		`{"functions":[{"id":"A","frameBytes":1},{"id":"A","frameBytes":2}],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
		// Call violations.
		`{"functions":[{"id":"A","frameBytes":1}],"calls":[{"caller":"A","callee":"B","overheadBytes":0}],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
		`{"functions":[{"id":"A","frameBytes":1}],"calls":[{"caller":"B","callee":"A","overheadBytes":0}],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
		`{"functions":[{"id":"A","frameBytes":1}],"calls":[{"caller":"A","callee":"A"}],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
		`{"functions":[{"id":"A","frameBytes":1}],"calls":[{"caller":"A","callee":"A","overheadBytes":-1}],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
		`{"functions":[{"id":"A","frameBytes":1}],"calls":[{"caller":"A","callee":"A","overheadBytes":1000000000001}],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
		// Entrypoint violations.
		`{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[{"entry":"B","stackLimit":1}]}`,
		`{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[{"entry":"A"}]}`,
		`{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[{"entry":"A","stackLimit":0}]}`,
		`{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[{"entry":"A","stackLimit":10000000000000001}]}`,
		`{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[{"entry":"A","stackLimit":1},{"entry":"A","stackLimit":2}]}`,
		// Wrong JSON types.
		`{"functions":"x","entrypoints":[{"entry":"A","stackLimit":1}]}`,
		`{"functions":[{"id":1,"frameBytes":1}],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
		`{"functions":[{"id":"A","frameBytes":"1"}],"entrypoints":[{"entry":"A","stackLimit":1}]}`,
	}
	for i, body := range cases {
		rec := postStacks(t, body, "application/json")
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("case %d: status = %d, want 422; body: %s", i, rec.Code, rec.Body.String())
		}
		var parsed map[string]map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil || parsed["error"]["code"] != "validation_failed" {
			t.Fatalf("case %d: body = %s, want validation_failed error", i, rec.Body.String())
		}
	}
}

func TestStackTooManyItems(t *testing.T) {
	// 4097 functions.
	var b strings.Builder
	b.WriteString(`{"functions":[`)
	for i := 0; i < 4097; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"id":"f`)
		b.WriteString(strconv.Itoa(i))
		b.WriteString(`","frameBytes":0}`)
	}
	b.WriteString(`],"entrypoints":[{"entry":"f0","stackLimit":1}]}`)
	stacksError(t, postStacks(t, b.String(), "application/json"), http.StatusUnprocessableEntity, "validation_failed")

	// 257 calls.
	b.Reset()
	b.WriteString(`{"functions":[{"id":"a","frameBytes":0},{"id":"b","frameBytes":0}],"calls":[`)
	for i := 0; i < 257; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"caller":"a","callee":"b","overheadBytes":0}`)
	}
	b.WriteString(`],"entrypoints":[{"entry":"a","stackLimit":1}]}`)
	stacksError(t, postStacks(t, b.String(), "application/json"), http.StatusUnprocessableEntity, "validation_failed")

	// 257 entrypoints.
	b.Reset()
	b.WriteString(`{"functions":[`)
	for i := 0; i < 257; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"id":"f`)
		b.WriteString(strconv.Itoa(i))
		b.WriteString(`","frameBytes":0}`)
	}
	b.WriteString(`],"entrypoints":[`)
	for i := 0; i < 257; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"entry":"f`)
		b.WriteString(strconv.Itoa(i))
		b.WriteString(`","stackLimit":1}`)
	}
	b.WriteString(`]}`)
	stacksError(t, postStacks(t, b.String(), "application/json"), http.StatusUnprocessableEntity, "validation_failed")
}

func TestStackBoundaryValuesAccepted(t *testing.T) {
	// frameBytes 0 and 1e12, overhead 1e12, stackLimit 1e16 are all in range.
	resp := stacksOK(t, `{
		"functions": [
			{"id":"A","frameBytes":0},
			{"id":"B","frameBytes":1000000000000}
		],
		"calls": [{"caller":"A","callee":"B","overheadBytes":1000000000000}],
		"entrypoints": [{"entry":"A","stackLimit":10000000000000000}]
	}`)
	r := resp.Results[0]
	if r.Status != "within_limit" || r.RequiredBytes == nil || *r.RequiredBytes != 2000000000000 {
		t.Fatalf("result = %+v, want within_limit/2e12", r)
	}
}

func TestStackErrorContainsNoPartialResults(t *testing.T) {
	rec := postStacks(t, `{"functions":[{"id":"A","frameBytes":1}],"entrypoints":[{"entry":"A","stackLimit":1},{"entry":"A","stackLimit":2}]}`, "application/json")
	if strings.Contains(rec.Body.String(), "results") {
		t.Fatalf("error response leaks partial results: %s", rec.Body.String())
	}
}
