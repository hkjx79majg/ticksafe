package server

import (
	"encoding/json"
	"net/http"
	"sort"
)

const (
	interruptAnalyzePath = "/v1/schedules/interrupt/analyze"
	maxInterrupts        = 4096
	maxInterruptActions  = 1024
)

type interruptActionIn struct {
	Run      *int64 `json:"run"`
	Critical *int64 `json:"critical"`
}

type interruptTaskIn struct {
	ID       string               `json:"id"`
	Priority *int64               `json:"priority"`
	Release  *int64               `json:"release"`
	Deadline *int64               `json:"deadline"`
	Actions  *[]interruptActionIn `json:"actions"`
}

type interruptIn struct {
	ID        string `json:"id"`
	Priority  *int64 `json:"priority"`
	Arrival   *int64 `json:"arrival"`
	Execution *int64 `json:"execution"`
	Deadline  *int64 `json:"deadline"`
}

type interruptRequest struct {
	Horizon    *int64             `json:"horizon"`
	Tasks      *[]interruptTaskIn `json:"tasks"`
	Interrupts *[]interruptIn     `json:"interrupts"`
}

// intAction is one task action: a run or a non-preemptible critical section,
// always with a positive duration.
type intAction struct {
	critical bool
	duration int64
}

type intTask struct {
	id       string
	priority int64
	release  int64
	deadline int64
	actions  []intAction
}

type intIntr struct {
	id        string
	priority  int64
	arrival   int64
	execution int64
	deadline  int64
}

type interruptTimelineInterval struct {
	ActorType string `json:"actorType"`
	ActorID   string `json:"actorId"`
	Mode      string `json:"mode"`
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
}

type interruptTaskResult struct {
	ID             string `json:"id"`
	Executed       int64  `json:"executed"`
	Completion     *int64 `json:"completion"`
	DeadlineStatus string `json:"deadlineStatus"`
}

type interruptResult struct {
	ID             string `json:"id"`
	Executed       int64  `json:"executed"`
	Completion     *int64 `json:"completion"`
	DeadlineStatus string `json:"deadlineStatus"`
	Start          *int64 `json:"start"`
	Latency        *int64 `json:"latency"`
}

type criticalSectionResult struct {
	TaskID           string `json:"taskId"`
	ActionIndex      int    `json:"actionIndex"`
	Start            *int64 `json:"start"`
	End              *int64 `json:"end"`
	ObservedDuration int64  `json:"observedDuration"`
	Completed        bool   `json:"completed"`
}

type interruptResponse struct {
	Status           string                      `json:"status"`
	StoppedAt        int64                       `json:"stoppedAt"`
	Timeline         []interruptTimelineInterval `json:"timeline"`
	TaskResults      []interruptTaskResult       `json:"taskResults"`
	InterruptResults []interruptResult           `json:"interruptResults"`
	CriticalSections []criticalSectionResult     `json:"criticalSections"`
}

func handleInterruptAnalyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAnalyzeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req interruptRequest
	if !decodeStrictJSON(w, r, &req) {
		return
	}
	tasks, intrs, horizon, ok := validateInterruptRequest(&req)
	if !ok {
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(simulateInterrupt(tasks, intrs, horizon))
}

// validateInterruptRequest enforces the one-shot task constraints (without
// execution) plus the action-program and interrupt rules: 1..1024 actions per
// task, each exactly one of {"run":N} or {"critical":N} with a positive
// duration; 1..4096 interrupts with unique ids, 0..255 priorities, arrivals
// inside the horizon, positive executions and deadlines past their arrivals.
// The grand total of all action durations and interrupt executions must fit
// int64.
func validateInterruptRequest(req *interruptRequest) ([]intTask, []intIntr, int64, bool) {
	if req.Horizon == nil || *req.Horizon <= 0 || *req.Horizon > maxHorizon {
		return nil, nil, 0, false
	}
	horizon := *req.Horizon
	if req.Tasks == nil || len(*req.Tasks) == 0 || len(*req.Tasks) > maxTasks {
		return nil, nil, 0, false
	}
	if req.Interrupts == nil || len(*req.Interrupts) == 0 || len(*req.Interrupts) > maxInterrupts {
		return nil, nil, 0, false
	}
	var total int64
	tasks := make([]intTask, 0, len(*req.Tasks))
	ids := make(map[string]struct{}, len(*req.Tasks))
	for i := range *req.Tasks {
		in := (*req.Tasks)[i]
		if !validTaskID(in.ID) {
			return nil, nil, 0, false
		}
		if _, dup := ids[in.ID]; dup {
			return nil, nil, 0, false
		}
		ids[in.ID] = struct{}{}
		if in.Priority == nil || *in.Priority < 0 || *in.Priority > 255 {
			return nil, nil, 0, false
		}
		if in.Release == nil || *in.Release < 0 || *in.Release >= horizon {
			return nil, nil, 0, false
		}
		if in.Deadline == nil || *in.Deadline <= *in.Release {
			return nil, nil, 0, false
		}
		if in.Actions == nil || len(*in.Actions) == 0 || len(*in.Actions) > maxInterruptActions {
			return nil, nil, 0, false
		}
		actions := make([]intAction, 0, len(*in.Actions))
		var taskTotal int64
		for j := range *in.Actions {
			ai := (*in.Actions)[j]
			verbs := 0
			if ai.Run != nil {
				verbs++
			}
			if ai.Critical != nil {
				verbs++
			}
			if verbs != 1 {
				return nil, nil, 0, false
			}
			critical := ai.Critical != nil
			var duration int64
			if critical {
				duration = *ai.Critical
			} else {
				duration = *ai.Run
			}
			if duration <= 0 {
				return nil, nil, 0, false
			}
			var ok bool
			taskTotal, ok = saturatingAdd(taskTotal, duration)
			if !ok {
				return nil, nil, 0, false
			}
			actions = append(actions, intAction{critical: critical, duration: duration})
		}
		var ok bool
		total, ok = saturatingAdd(total, taskTotal)
		if !ok {
			return nil, nil, 0, false
		}
		tasks = append(tasks, intTask{
			id:       in.ID,
			priority: *in.Priority,
			release:  *in.Release,
			deadline: *in.Deadline,
			actions:  actions,
		})
	}
	intrs := make([]intIntr, 0, len(*req.Interrupts))
	iids := make(map[string]struct{}, len(*req.Interrupts))
	for i := range *req.Interrupts {
		in := (*req.Interrupts)[i]
		if !validTaskID(in.ID) {
			return nil, nil, 0, false
		}
		if _, dup := iids[in.ID]; dup {
			return nil, nil, 0, false
		}
		iids[in.ID] = struct{}{}
		if in.Priority == nil || *in.Priority < 0 || *in.Priority > 255 {
			return nil, nil, 0, false
		}
		if in.Arrival == nil || *in.Arrival < 0 || *in.Arrival >= horizon {
			return nil, nil, 0, false
		}
		if in.Execution == nil || *in.Execution <= 0 {
			return nil, nil, 0, false
		}
		if in.Deadline == nil || *in.Deadline <= *in.Arrival {
			return nil, nil, 0, false
		}
		var ok bool
		total, ok = saturatingAdd(total, *in.Execution)
		if !ok {
			return nil, nil, 0, false
		}
		intrs = append(intrs, intIntr{
			id:        in.ID,
			priority:  *in.Priority,
			arrival:   *in.Arrival,
			execution: *in.Execution,
			deadline:  *in.Deadline,
		})
	}
	return tasks, intrs, horizon, true
}

// intSegment is an unmerged execution interval recorded by the simulation.
type intSegment struct {
	interrupt  bool
	index      int
	critical   bool
	start, end int64
}

// critSection tracks one critical action of a task. A started critical
// section is non-preemptible, so it executes as a single contiguous interval:
// end stays nil only when the horizon truncates it.
type critSection struct {
	actionIndex int
	start       *int64
	end         *int64
	observed    int64
	completed   bool
}

// simulateInterrupt runs an event-driven single-core simulation over
// [0, horizon) where interrupts always outrank tasks. A task's critical
// action, once started, cannot be preempted by anything; its run actions are
// preemptible by higher-priority tasks and by any interrupt. Interrupts are
// preempted only by strictly-higher-priority interrupts and resume afterwards.
// At any instant all arrivals and releases are incorporated before selection;
// ties break by arrival/release, then input order.
func simulateInterrupt(tasks []intTask, intrs []intIntr, horizon int64) interruptResponse {
	n, m := len(tasks), len(intrs)

	pc := make([]int, n)
	runRem := make([]int64, n)
	released := make([]bool, n)
	completedT := make([]bool, n)
	executedT := make([]int64, n)
	completionT := make([]*int64, n)

	crits := make([][]critSection, n)
	critSlot := make([][]int, n)
	for i, t := range tasks {
		critSlot[i] = make([]int, len(t.actions))
		for j, a := range t.actions {
			if a.critical {
				critSlot[i][j] = len(crits[i])
				crits[i] = append(crits[i], critSection{actionIndex: j})
			} else {
				critSlot[i][j] = -1
			}
		}
	}

	remI := make([]int64, m)
	executedI := make([]int64, m)
	arrived := make([]bool, m)
	startI := make([]*int64, m)
	completionI := make([]*int64, m)
	for i, in := range intrs {
		remI[i] = in.execution
	}

	// Indices sorted by (release or arrival, input order) for event
	// bookkeeping and same-priority tie breaking.
	taskOrder := make([]int, n)
	for i := range taskOrder {
		taskOrder[i] = i
	}
	sort.Slice(taskOrder, func(a, b int) bool {
		ia, ib := taskOrder[a], taskOrder[b]
		if tasks[ia].release != tasks[ib].release {
			return tasks[ia].release < tasks[ib].release
		}
		return ia < ib
	})
	intrOrder := make([]int, m)
	for i := range intrOrder {
		intrOrder[i] = i
	}
	sort.Slice(intrOrder, func(a, b int) bool {
		ia, ib := intrOrder[a], intrOrder[b]
		if intrs[ia].arrival != intrs[ib].arrival {
			return intrs[ia].arrival < intrs[ib].arrival
		}
		return ia < ib
	})
	nextRel, nextArr := 0, 0

	const (
		kindNone = iota - 1
		kindTask
		kindIntr
	)
	var segs []intSegment
	curKind, curIdx := kindNone, 0
	var now int64
	doneT, doneI := 0, 0

	for now < horizon {
		for nextRel < n && tasks[taskOrder[nextRel]].release <= now {
			released[taskOrder[nextRel]] = true
			nextRel++
		}
		for nextArr < m && intrs[intrOrder[nextArr]].arrival <= now {
			arrived[intrOrder[nextArr]] = true
			nextArr++
		}
		if curKind == kindTask && completedT[curIdx] {
			curKind = kindNone
		}
		if curKind == kindIntr && remI[curIdx] == 0 {
			curKind = kindNone
		}

		// Best ready interrupt: smallest priority, then earliest arrival,
		// then earliest input index. Any ready interrupt outranks any task.
		bi := -1
		for i := 0; i < m; i++ {
			if !arrived[i] || remI[i] <= 0 {
				continue
			}
			if bi == -1 ||
				intrs[i].priority < intrs[bi].priority ||
				(intrs[i].priority == intrs[bi].priority && intrs[i].arrival < intrs[bi].arrival) ||
				(intrs[i].priority == intrs[bi].priority && intrs[i].arrival == intrs[bi].arrival && i < bi) {
				bi = i
			}
		}
		if bi != -1 {
			// A strictly higher priority interrupt preempts; equal never does.
			if curKind != kindIntr || (bi != curIdx && intrs[bi].priority < intrs[curIdx].priority) {
				curKind, curIdx = kindIntr, bi
			}
		} else {
			if curKind == kindIntr {
				curKind = kindNone
			}
			// Best runnable task: smallest priority, then earliest release,
			// then earliest input index.
			bt := -1
			for i := 0; i < n; i++ {
				if !released[i] || completedT[i] {
					continue
				}
				if bt == -1 ||
					tasks[i].priority < tasks[bt].priority ||
					(tasks[i].priority == tasks[bt].priority && tasks[i].release < tasks[bt].release) ||
					(tasks[i].priority == tasks[bt].priority && tasks[i].release == tasks[bt].release && i < bt) {
					bt = i
				}
			}
			if curKind == kindNone {
				if bt != -1 {
					curKind, curIdx = kindTask, bt
				}
			} else if bt != -1 && bt != curIdx && tasks[bt].priority < tasks[curIdx].priority {
				curIdx = bt
			}
		}

		if curKind == kindNone {
			// Idle: jump to the next release or arrival (or stop when every
			// task and interrupt is done; now is the completion time).
			next := int64(-1)
			if nextRel < n {
				next = tasks[taskOrder[nextRel]].release
			}
			if nextArr < m {
				if a := intrs[intrOrder[nextArr]].arrival; next == -1 || a < next {
					next = a
				}
			}
			if next == -1 {
				break
			}
			now = next
			continue
		}

		// Run until the action or interrupt completes, the horizon cuts it,
		// or a preempting event occurs. A critical section ignores events.
		critical := false
		runEnd := horizon
		if curKind == kindTask {
			act := tasks[curIdx].actions[pc[curIdx]]
			if runRem[curIdx] == 0 {
				runRem[curIdx] = act.duration
			}
			critical = act.critical
			if rem := runRem[curIdx]; rem < horizon-now {
				runEnd = now + rem
			}
			if !critical {
				// Any interrupt arrival preempts a running task.
				if nextArr < m {
					if a := intrs[intrOrder[nextArr]].arrival; a < runEnd {
						runEnd = a
					}
				}
				// The earliest strictly-higher-priority release preempts.
				for k := nextRel; k < n; k++ {
					j := taskOrder[k]
					if tasks[j].release >= runEnd {
						break
					}
					if tasks[j].priority < tasks[curIdx].priority {
						runEnd = tasks[j].release
						break
					}
				}
			}
		} else {
			if rem := remI[curIdx]; rem < horizon-now {
				runEnd = now + rem
			}
			// The earliest strictly-higher-priority arrival preempts.
			for k := nextArr; k < m; k++ {
				j := intrOrder[k]
				if intrs[j].arrival >= runEnd {
					break
				}
				if intrs[j].priority < intrs[curIdx].priority {
					runEnd = intrs[j].arrival
					break
				}
			}
		}

		segs = append(segs, intSegment{
			interrupt: curKind == kindIntr,
			index:     curIdx,
			critical:  critical,
			start:     now,
			end:       runEnd,
		})
		delta := runEnd - now
		if curKind == kindTask {
			executedT[curIdx] += delta
			runRem[curIdx] -= delta
			var rec *critSection
			if critical {
				rec = &crits[curIdx][critSlot[curIdx][pc[curIdx]]]
				s := now
				rec.start = &s
			}
			now = runEnd
			if runRem[curIdx] == 0 {
				if critical {
					e := now
					rec.end = &e
					rec.observed = e - *rec.start
					rec.completed = true
				}
				pc[curIdx]++
				if pc[curIdx] == len(tasks[curIdx].actions) {
					completedT[curIdx] = true
					c := now
					completionT[curIdx] = &c
					doneT++
				}
			} else if critical {
				// Truncated by the horizon: end stays null.
				rec.observed = now - *rec.start
			}
		} else {
			executedI[curIdx] += delta
			remI[curIdx] -= delta
			if startI[curIdx] == nil {
				s := now
				startI[curIdx] = &s
			}
			now = runEnd
			if remI[curIdx] == 0 {
				c := now
				completionI[curIdx] = &c
				doneI++
			}
		}
	}

	// Merge adjacent intervals of the same actor and mode; idle is omitted.
	timeline := make([]interruptTimelineInterval, 0, len(segs))
	for _, seg := range segs {
		actorType, actorID, mode := "task", "", "run"
		if seg.interrupt {
			actorType, actorID = "interrupt", intrs[seg.index].id
		} else {
			actorID = tasks[seg.index].id
		}
		if seg.critical {
			mode = "critical"
		}
		last := len(timeline) - 1
		if last >= 0 && timeline[last].ActorType == actorType &&
			timeline[last].ActorID == actorID && timeline[last].Mode == mode &&
			timeline[last].End == seg.start {
			timeline[last].End = seg.end
			continue
		}
		timeline = append(timeline, interruptTimelineInterval{
			ActorType: actorType,
			ActorID:   actorID,
			Mode:      mode,
			Start:     seg.start,
			End:       seg.end,
		})
	}

	deadlineStatus := func(completion *int64, deadline int64) string {
		switch {
		case completion != nil && *completion <= deadline:
			return "met"
		case completion != nil:
			return "missed"
		case deadline <= horizon:
			return "missed"
		default:
			return "pending"
		}
	}

	taskResults := make([]interruptTaskResult, n)
	for i, t := range tasks {
		taskResults[i] = interruptTaskResult{
			ID:             t.id,
			Executed:       executedT[i],
			Completion:     completionT[i],
			DeadlineStatus: deadlineStatus(completionT[i], t.deadline),
		}
	}
	interruptResults := make([]interruptResult, m)
	for i, in := range intrs {
		var latency *int64
		if startI[i] != nil {
			l := *startI[i] - in.arrival
			latency = &l
		}
		interruptResults[i] = interruptResult{
			ID:             in.id,
			Executed:       executedI[i],
			Completion:     completionI[i],
			DeadlineStatus: deadlineStatus(completionI[i], in.deadline),
			Start:          startI[i],
			Latency:        latency,
		}
	}
	var sections []criticalSectionResult
	for i, t := range tasks {
		for _, rec := range crits[i] {
			sections = append(sections, criticalSectionResult{
				TaskID:           t.id,
				ActionIndex:      rec.actionIndex,
				Start:            rec.start,
				End:              rec.end,
				ObservedDuration: rec.observed,
				Completed:        rec.completed,
			})
		}
	}
	if sections == nil {
		sections = []criticalSectionResult{}
	}

	status := "horizon"
	stoppedAt := horizon
	if doneT == n && doneI == m {
		status = "completed"
		stoppedAt = now
	}
	return interruptResponse{
		Status:           status,
		StoppedAt:        stoppedAt,
		Timeline:         timeline,
		TaskResults:      taskResults,
		InterruptResults: interruptResults,
		CriticalSections: sections,
	}
}
