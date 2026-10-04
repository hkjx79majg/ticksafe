package server

import (
	"container/heap"
	"encoding/json"
	"net/http"
)

const (
	ticklessSimulatePath = "/v1/timers/tickless/simulate"
	// maxTicklessEvents bounds the total number of timer events generated
	// within the horizon across all timers; maxTicklessWakeups bounds the
	// actual number of wakeups the simulation performs.
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

type ticklessSimulateResponse struct {
	WakeupCount int                   `json:"wakeupCount"`
	Wakeups     []ticklessWakeup      `json:"wakeups"`
	Results     []ticklessEventResult `json:"results"`
}

// simTimer carries the validated timer parameters.
type simTimer struct {
	id     string
	first  int64
	period int64
}

// pendingEvent is one not-yet-delivered timer event.
type pendingEvent struct {
	scheduledAt int64
	timerIdx    int
	event       int
}

// pendingHeap orders pending events by nominal time, then timer input order,
// then event number, matching the required delivery order.
type pendingHeap []pendingEvent

func (h pendingHeap) Len() int { return len(h) }
func (h pendingHeap) Less(i, j int) bool {
	if h[i].scheduledAt != h[j].scheduledAt {
		return h[i].scheduledAt < h[j].scheduledAt
	}
	if h[i].timerIdx != h[j].timerIdx {
		return h[i].timerIdx < h[j].timerIdx
	}
	return h[i].event < h[j].event
}
func (h pendingHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *pendingHeap) Push(x any)   { *h = append(*h, x.(pendingEvent)) }
func (h *pendingHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
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
	timers, horizon, coalesceWindow, maxSleep, ok :=
		validateTicklessSimulateRequest(&req)
	if !ok {
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	resp := simulateTickless(timers, horizon, coalesceWindow, maxSleep)
	if resp == nil {
		// The expanded event set was within bounds, but the timer pattern
		// would force more than maxTicklessWakeups actual wakeups.
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func validateTicklessSimulateRequest(req *ticklessSimulateRequest) (
	timers []simTimer, horizon, coalesceWindow, maxSleep int64, ok bool,
) {
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
	timers = make([]simTimer, 0, len(*req.Timers))
	ids := make(map[string]struct{}, len(*req.Timers))
	totalEvents := 0
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
		// Events exist at first+k*period < horizon. period == 0 is a one-shot;
		// otherwise the count is at least 1 because first < horizon, and the
		// arithmetic stays well inside int64 since every operand is <= 1e12.
		if *in.Period == 0 {
			totalEvents++
		} else {
			totalEvents += int((horizon-*in.First-1)/(*in.Period) + 1)
		}
		if totalEvents > maxTicklessEvents {
			return nil, 0, 0, 0, false
		}
		timers = append(timers, simTimer{
			id:     in.ID,
			first:  *in.First,
			period: *in.Period,
		})
	}
	return timers, horizon, coalesceWindow, maxSleep, true
}

// ticklessEventCount returns the number of events a validated timer raises
// within [0,horizon): one for a one-shot, else first+k*period < horizon.
func ticklessEventCount(t simTimer, horizon int64) int {
	if t.period == 0 {
		return 1
	}
	return int((horizon-t.first-1)/t.period + 1)
}

// simulateTickless runs the on-demand ("tickless") wakeup simulation, returning
// nil when it would perform more than maxTicklessWakeups wakeups.
//
// No fixed-period tick is ever generated: each wakeup time is derived solely
// from d, the earliest undelivered nominal deadline, as
// min(d+coalesceWindow, now+maxSleep, horizon). At a wakeup every event with a
// nominal time not later than the current time is delivered, ordered by nominal
// time, timer input order and event number. Periodic events are always derived
// from first+k*period, so delivery latency never drifts their nominal times.
func simulateTickless(timers []simTimer, horizon, coalesceWindow, maxSleep int64) *ticklessSimulateResponse {
	n := len(timers)
	counts := make([]int, n)
	totalEvents := 0
	for i := range timers {
		counts[i] = ticklessEventCount(timers[i], horizon)
		totalEvents += counts[i]
	}

	// Expand every event up front; the request validator already bounded the
	// total by maxTicklessEvents. The heap top is therefore always the
	// earliest nominal deadline d among all undelivered events.
	pending := make(pendingHeap, 0, totalEvents)
	for i := range timers {
		for k := 0; k < counts[i]; k++ {
			at := timers[i].first
			if timers[i].period > 0 {
				at = timers[i].first + int64(k)*timers[i].period
			}
			pending = append(pending, pendingEvent{
				scheduledAt: at,
				timerIdx:    i,
				event:       k,
			})
		}
	}
	heap.Init(&pending)

	// Results are one flat entry per event, ordered by timer input order and
	// then event number; each timer's events occupy a contiguous range whose
	// start offsets are precomputed here. Entries are written by event number
	// because the nominal time first+k*period never depends on delivery
	// latency, and every event is delivered exactly once.
	results := make([]ticklessEventResult, totalEvents)
	offsets := make([]int, n)
	nextOffset := 0
	for i := range timers {
		offsets[i] = nextOffset
		nextOffset += counts[i]
	}

	wakeups := make([]ticklessWakeup, 0)
	delivered := 0
	var now int64
	for delivered < totalEvents {
		d := pending[0].scheduledAt
		wakeAt := d + coalesceWindow
		if cap := now + maxSleep; cap < wakeAt {
			wakeAt = cap
		}
		if horizon < wakeAt {
			wakeAt = horizon
		}
		now = wakeAt

		// The wakeup about to be recorded is the (len+1)th; more than the
		// allowed number of actual wakeups is a validation failure.
		if len(wakeups) >= maxTicklessWakeups {
			return nil
		}

		// Deliver all events whose nominal time is not later than now; the
		// heap yields them in (time, timer input order, event) order.
		fired := make([]ticklessFiredEvent, 0)
		for pending.Len() > 0 && pending[0].scheduledAt <= now {
			ev := heap.Pop(&pending).(pendingEvent)
			lateness := now - ev.scheduledAt
			fired = append(fired, ticklessFiredEvent{
				ID:          timers[ev.timerIdx].id,
				Event:       ev.event,
				ScheduledAt: ev.scheduledAt,
				Lateness:    lateness,
			})
			// Nominal time first+k*period never depends on delivery latency;
			// write into the event's fixed slot in the timer's range.
			results[offsets[ev.timerIdx]+ev.event] = ticklessEventResult{
				Event:       ev.event,
				ScheduledAt: ev.scheduledAt,
				FiredAt:     now,
				Lateness:    lateness,
			}
			delivered++
		}
		wakeups = append(wakeups, ticklessWakeup{At: now, Fired: fired})
	}

	return &ticklessSimulateResponse{
		WakeupCount: len(wakeups),
		Wakeups:     wakeups,
		Results:     results,
	}
}
