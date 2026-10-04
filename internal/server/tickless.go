package server

import (
	"encoding/json"
	"net/http"
	"sort"
)

const (
	ticklessSimulatePath = "/v1/timers/tickless/simulate"
	// maxTicklessEvents bounds the number of nominal timer events expanded
	// within the horizon across all timers; maxTicklessWakeups bounds the
	// number of on-demand wakeups the simulation may perform.
	maxTicklessEvents  = 100000
	maxTicklessWakeups = 100000
)

type ticklessTimerIn struct {
	ID     string `json:"id"`
	First  *int64 `json:"first"`
	Period *int64 `json:"period"`
}

type ticklessSimulateRequest struct {
	Horizon        *int64             `json:"horizon"`
	CoalesceWindow *int64             `json:"coalesceWindow"`
	MaxSleep       *int64             `json:"maxSleep"`
	Timers         *[]ticklessTimerIn `json:"timers"`
}

type ticklessFiredEvent struct {
	ID          string `json:"id"`
	Event       int    `json:"event"`
	ScheduledAt int64  `json:"scheduledAt"`
	Lateness    int64  `json:"lateness"`
}

type ticklessWakeup struct {
	At    int64                `json:"at"`
	Fired []ticklessFiredEvent `json:"fired"`
}

type ticklessEventResult struct {
	Event       int   `json:"event"`
	ScheduledAt int64 `json:"scheduledAt"`
	FiredAt     int64 `json:"firedAt"`
	Lateness    int64 `json:"lateness"`
}

type ticklessTimerResult struct {
	ID     string                `json:"id"`
	Events []ticklessEventResult `json:"events"`
}

type ticklessSimulateResponse struct {
	WakeupCount int                   `json:"wakeupCount"`
	Wakeups     []ticklessWakeup      `json:"wakeups"`
	Results     []ticklessTimerResult `json:"results"`
}

// ticklessEvent is one nominal expiry first+k*period of a timer.
type ticklessEvent struct {
	timer     int
	event     int
	scheduled int64
}

// ticklessTimer carries the validated id together with its expanded events.
type ticklessTimer struct {
	id     string
	events []ticklessEvent
}

func handleTicklessSimulate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAnalyzeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req ticklessSimulateRequest
	if !decodeStrictJSON(w, r, &req) {
		return
	}
	timers, horizon, coalesceWindow, maxSleep, ok := validateTicklessSimulateRequest(&req)
	if !ok {
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	resp, ok := simulateTickless(timers, horizon, coalesceWindow, maxSleep)
	if !ok {
		// The expanded event count passed validation, but the coalescing
		// parameters produced more actual wakeups than the bound allows.
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func validateTicklessSimulateRequest(req *ticklessSimulateRequest) (timers []ticklessTimer, horizon, coalesceWindow, maxSleep int64, ok bool) {
	if req.Horizon == nil || *req.Horizon < 1 || *req.Horizon > maxHorizon {
		return nil, 0, 0, 0, false
	}
	horizon = *req.Horizon
	if req.CoalesceWindow == nil || *req.CoalesceWindow < 0 || *req.CoalesceWindow > maxHorizon {
		return nil, 0, 0, 0, false
	}
	coalesceWindow = *req.CoalesceWindow
	if req.MaxSleep == nil || *req.MaxSleep < 1 || *req.MaxSleep > maxHorizon {
		return nil, 0, 0, 0, false
	}
	maxSleep = *req.MaxSleep
	if req.Timers == nil || len(*req.Timers) == 0 || len(*req.Timers) > maxTasks {
		return nil, 0, 0, 0, false
	}
	timers = make([]ticklessTimer, 0, len(*req.Timers))
	ids := make(map[string]struct{}, len(*req.Timers))
	var totalEvents int64
	for i := range *req.Timers {
		in := (*req.Timers)[i]
		if !validTaskID(in.ID) {
			return nil, 0, 0, 0, false
		}
		if _, dup := ids[in.ID]; dup {
			return nil, 0, 0, 0, false
		}
		ids[in.ID] = struct{}{}
		if in.First == nil || *in.First < 0 || *in.First >= horizon {
			return nil, 0, 0, 0, false
		}
		if in.Period == nil || *in.Period < 0 {
			return nil, 0, 0, 0, false
		}
		first, period := *in.First, *in.Period
		// Events exist at first+k*period < horizon. A one-shot timer (period 0)
		// yields its single event; first < horizon guarantees at least one.
		var count int64
		if period == 0 {
			count = 1
		} else {
			count = (horizon-first-1)/period + 1
		}
		totalEvents += count
		if totalEvents > maxTicklessEvents {
			return nil, 0, 0, 0, false
		}
		events := make([]ticklessEvent, count)
		for k := range events {
			events[k] = ticklessEvent{timer: i, event: k, scheduled: first + int64(k)*period}
		}
		timers = append(timers, ticklessTimer{id: in.ID, events: events})
	}
	return timers, horizon, coalesceWindow, maxSleep, true
}

// simulateTickless runs a tickless on-demand wakeup simulation of the expanded
// timer events over [0,horizon). There is no periodic tick: at time now the
// next wakeup is scheduled at min(d+coalesceWindow, now+maxSleep, horizon),
// where d is the earliest not-yet-delivered nominal expiry. Every event whose
// nominal time is no later than the wakeup time (including events scheduled
// before the horizon that are delivered at the horizon) is delivered once,
// ordered by nominal time, timer input order, then event number. A maxSleep
// wakeup that delivers nothing is retained as an empty wakeup. The second
// result reports false when the wakeup count would exceed maxTicklessWakeups.
func simulateTickless(timers []ticklessTimer, horizon, coalesceWindow, maxSleep int64) (ticklessSimulateResponse, bool) {
	// Flatten the per-timer event lists into a single (scheduled, input
	// order, event) ordered queue. Periodic scheduledAt values are always
	// derived from first+k*period, so delayed wakes never introduce drift.
	total := 0
	for i := range timers {
		total += len(timers[i].events)
	}
	events := make([]ticklessEvent, 0, total)
	for i := range timers {
		events = append(events, timers[i].events...)
	}
	sort.Slice(events, func(a, b int) bool {
		ea, eb := events[a], events[b]
		if ea.scheduled != eb.scheduled {
			return ea.scheduled < eb.scheduled
		}
		if ea.timer != eb.timer {
			return ea.timer < eb.timer
		}
		return ea.event < eb.event
	})

	// firedAt[i][k] records the wake time of timer i's event k.
	firedAt := make([][]int64, len(timers))
	for i := range timers {
		firedAt[i] = make([]int64, len(timers[i].events))
	}

	wakeups := make([]ticklessWakeup, 0)
	next := 0
	var now int64
	for next < len(events) {
		d := events[next].scheduled
		wakeAt := d + coalesceWindow
		if s := now + maxSleep; s < wakeAt {
			wakeAt = s
		}
		if horizon < wakeAt {
			wakeAt = horizon
		}
		fired := make([]ticklessFiredEvent, 0)
		for next < len(events) && events[next].scheduled <= wakeAt {
			ev := events[next]
			fired = append(fired, ticklessFiredEvent{
				ID:          timers[ev.timer].id,
				Event:       ev.event,
				ScheduledAt: ev.scheduled,
				Lateness:    wakeAt - ev.scheduled,
			})
			firedAt[ev.timer][ev.event] = wakeAt
			next++
		}
		wakeups = append(wakeups, ticklessWakeup{At: wakeAt, Fired: fired})
		if len(wakeups) > maxTicklessWakeups {
			return ticklessSimulateResponse{}, false
		}
		now = wakeAt
	}

	// Results follow the input order: timer by input index, then event number.
	results := make([]ticklessTimerResult, len(timers))
	for i := range timers {
		eresults := make([]ticklessEventResult, len(timers[i].events))
		for k, e := range timers[i].events {
			at := firedAt[i][k]
			eresults[k] = ticklessEventResult{
				Event:       e.event,
				ScheduledAt: e.scheduled,
				FiredAt:     at,
				Lateness:    at - e.scheduled,
			}
		}
		results[i] = ticklessTimerResult{ID: timers[i].id, Events: eresults}
	}

	return ticklessSimulateResponse{
		WakeupCount: len(wakeups),
		Wakeups:     wakeups,
		Results:     results,
	}, true
}
