package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func postAnalyze(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/schedules/analyze", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, req)
	return rec
}

func decodeResponse(t *testing.T, rec *httptest.ResponseRecorder) analyzeResponse {
	t.Helper()
	var resp analyzeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return resp
}

func TestAnalyzeSingleTask(t *testing.T) {
	rec := postAnalyze(t, `{"horizon":100,"tasks":[{"id":"a","priority":1,"release":0,"execution":10,"deadline":20}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	resp := decodeResponse(t, rec)
	if len(resp.Timeline) != 1 || resp.Timeline[0] != (interval{TaskID: "a", Start: 0, End: 10}) {
		t.Fatalf("unexpected timeline: %+v", resp.Timeline)
	}
	r := resp.Results[0]
	if r.Executed != 10 || r.Remaining != 0 || r.Completion == nil || *r.Completion != 10 || r.DeadlineStatus != "met" {
		t.Fatalf("unexpected result: %+v", r)
	}
}

func TestAnalyzePreemption(t *testing.T) {
	rec := postAnalyze(t, `{"horizon":100,"tasks":[
		{"id":"low","priority":5,"release":0,"execution":10,"deadline":100},
		{"id":"high","priority":1,"release":3,"execution":2,"deadline":100}
	]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	resp := decodeResponse(t, rec)
	want := []interval{
		{TaskID: "low", Start: 0, End: 3},
		{TaskID: "high", Start: 3, End: 5},
		{TaskID: "low", Start: 5, End: 12},
	}
	if len(resp.Timeline) != len(want) {
		t.Fatalf("timeline = %+v, want %+v", resp.Timeline, want)
	}
	for i := range want {
		if resp.Timeline[i] != want[i] {
			t.Fatalf("timeline = %+v, want %+v", resp.Timeline, want)
		}
	}
	if *resp.Results[0].Completion != 12 || *resp.Results[1].Completion != 5 {
		t.Fatalf("unexpected completions: %+v", resp.Results)
	}
}

func TestAnalyzeSamePriorityFIFO(t *testing.T) {
	rec := postAnalyze(t, `{"horizon":100,"tasks":[
		{"id":"first","priority":1,"release":0,"execution":5,"deadline":100},
		{"id":"second","priority":1,"release":1,"execution":5,"deadline":100}
	]}`)
	resp := decodeResponse(t, rec)
	want := []interval{
		{TaskID: "first", Start: 0, End: 5},
		{TaskID: "second", Start: 5, End: 10},
	}
	if len(resp.Timeline) != 2 || resp.Timeline[0] != want[0] || resp.Timeline[1] != want[1] {
		t.Fatalf("timeline = %+v, want %+v", resp.Timeline, want)
	}
}

func TestAnalyzeHorizonCutoffAndDeadlineStatus(t *testing.T) {
	rec := postAnalyze(t, `{"horizon":10,"tasks":[
		{"id":"unfinished","priority":1,"release":0,"execution":100,"deadline":1000},
		{"id":"missedTask","priority":2,"release":0,"execution":100,"deadline":10},
		{"id":"lateCompletion","priority":0,"release":0,"execution":4,"deadline":3}
	]}`)
	resp := decodeResponse(t, rec)
	if len(resp.Timeline) != 2 || resp.Timeline[0] != (interval{TaskID: "lateCompletion", Start: 0, End: 4}) ||
		resp.Timeline[1] != (interval{TaskID: "unfinished", Start: 4, End: 10}) {
		t.Fatalf("timeline = %+v", resp.Timeline)
	}
	unfinished := resp.Results[0]
	if unfinished.Executed != 6 || unfinished.Remaining != 94 || unfinished.Completion != nil || unfinished.DeadlineStatus != "pending" {
		t.Fatalf("unfinished = %+v", unfinished)
	}
	missed := resp.Results[1]
	if missed.Executed != 0 || missed.Remaining != 100 || missed.Completion != nil || missed.DeadlineStatus != "missed" {
		t.Fatalf("missed = %+v", missed)
	}
	late := resp.Results[2]
	if late.Completion == nil || *late.Completion != 4 || late.DeadlineStatus != "missed" {
		t.Fatalf("late = %+v", late)
	}
}

func TestAnalyzeIdleGapsOmitted(t *testing.T) {
	rec := postAnalyze(t, `{"horizon":50,"tasks":[{"id":"a","priority":1,"release":10,"execution":5,"deadline":20}]}`)
	resp := decodeResponse(t, rec)
	if len(resp.Timeline) != 1 || resp.Timeline[0] != (interval{TaskID: "a", Start: 10, End: 15}) {
		t.Fatalf("timeline = %+v", resp.Timeline)
	}
}

func TestAnalyzeMethodNotAllowed(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/schedules/analyze", nil)
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", rec.Code)
	}
	if rec.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("Allow = %q", rec.Header().Get("Allow"))
	}
	assertErrorCode(t, rec, "method_not_allowed")
}

func TestAnalyzeUnsupportedMediaType(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/schedules/analyze", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d", rec.Code)
	}
	assertErrorCode(t, rec, "unsupported_media_type")
}

func TestAnalyzeInvalidJSON(t *testing.T) {
	cases := map[string]string{
		"syntax":             `{"horizon":`,
		"trailing":           `{"horizon":10,"tasks":[{"id":"a","priority":1,"release":0,"execution":1,"deadline":2}]} extra`,
		"unknown field":      `{"horizon":10,"tasks":[{"id":"a","priority":1,"release":0,"execution":1,"deadline":2}],"bogus":1}`,
		"unknown task field": `{"horizon":10,"tasks":[{"id":"a","priority":1,"release":0,"execution":1,"deadline":2,"x":1}]}`,
		"wrong type":         `{"horizon":"10","tasks":[{"id":"a","priority":1,"release":0,"execution":1,"deadline":2}]}`,
		"empty":              ``,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			rec := postAnalyze(t, body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
			}
			assertErrorCode(t, rec, "invalid_json")
		})
	}
}

func TestAnalyzeValidationFailed(t *testing.T) {
	task := `"tasks":[{"id":"a","priority":1,"release":0,"execution":1,"deadline":2}]`
	cases := map[string]string{
		"horizon zero":               `{"horizon":0,` + task + `}`,
		"horizon too large":          `{"horizon":1000000000001,` + task + `}`,
		"horizon missing":            `{` + task + `}`,
		"no tasks":                   `{"horizon":10,"tasks":[]}`,
		"duplicate id":               `{"horizon":10,"tasks":[{"id":"a","priority":1,"release":0,"execution":1,"deadline":2},{"id":"a","priority":1,"release":0,"execution":1,"deadline":2}]}`,
		"id whitespace":              `{"horizon":10,"tasks":[{"id":"a b","priority":1,"release":0,"execution":1,"deadline":2}]}`,
		"id empty":                   `{"horizon":10,"tasks":[{"id":"","priority":1,"release":0,"execution":1,"deadline":2}]}`,
		"id too long":                `{"horizon":10,"tasks":[{"id":"` + strings.Repeat("x", 65) + `","priority":1,"release":0,"execution":1,"deadline":2}]}`,
		"priority negative":          `{"horizon":10,"tasks":[{"id":"a","priority":-1,"release":0,"execution":1,"deadline":2}]}`,
		"priority too large":         `{"horizon":10,"tasks":[{"id":"a","priority":256,"release":0,"execution":1,"deadline":2}]}`,
		"release negative":           `{"horizon":10,"tasks":[{"id":"a","priority":1,"release":-1,"execution":1,"deadline":2}]}`,
		"release at horizon":         `{"horizon":10,"tasks":[{"id":"a","priority":1,"release":10,"execution":1,"deadline":20}]}`,
		"execution zero":             `{"horizon":10,"tasks":[{"id":"a","priority":1,"release":0,"execution":0,"deadline":2}]}`,
		"deadline not after release": `{"horizon":10,"tasks":[{"id":"a","priority":1,"release":3,"execution":1,"deadline":3}]}`,
		"missing field":              `{"horizon":10,"tasks":[{"id":"a","priority":1,"release":0,"execution":1}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			rec := postAnalyze(t, body)
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
			}
			assertErrorCode(t, rec, "validation_failed")
		})
	}
}

func TestAnalyzeDeterministic(t *testing.T) {
	body := `{"horizon":20,"tasks":[
		{"id":"a","priority":2,"release":0,"execution":6,"deadline":20},
		{"id":"b","priority":1,"release":2,"execution":3,"deadline":20},
		{"id":"c","priority":1,"release":2,"execution":3,"deadline":20}
	]}`
	first := postAnalyze(t, body).Body.String()
	for i := 0; i < 5; i++ {
		if got := postAnalyze(t, body).Body.String(); got != first {
			t.Fatalf("non-deterministic response: %s vs %s", first, got)
		}
	}
}

func TestUnknownPathStill404(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d", rec.Code)
	}
}

func assertErrorCode(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()
	var payload struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("error body is not JSON: %v", err)
	}
	if payload.Error.Code != want {
		t.Fatalf("error code = %q, want %q (body %s)", payload.Error.Code, want, rec.Body)
	}
}
