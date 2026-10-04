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

// Interrupt task action discriminators.
const (
	intActRun int = iota
	intActCritical
)

// Timeline actor kinds and execution modes.
const (
	intActorTask      = "task"
	intActorInterrupt = "interrupt"
	intModeRun        = "run"
	intModeCritical   = "critical"
)

type intActionIn struct {
	Run      *int64 `json:"run"`
	Critical *int64 `json:"critical"`
}

type intTaskIn struct {
	ID       string         `json:"id"`
	Priority *int64         `json:"priority"`
	Release  *int64         `json:"release"`
	Deadline *int64         `json:"deadline"`
	Actions  *[]intActionIn `json:"actions"`
}

type intInterruptIn struct {
	ID        string `json:"id"`
	Priority  *int64 `json:"priority"`
	Arrival   *int64 `json:"arrival"`
	Execution *int64 `json:"execution"`
	Deadline  *int64 `json:"deadline"`
}

type interruptRequest struct {
	Horizon    *int64            `json:"horizon"`
	Tasks      *[]intTaskIn      `json:"tasks"`
	Interrupts *[]intInterruptIn `json:"interrupts"`
}

type intAction struct {
	kind int
	dur  int64
}

type intTask struct {
	id       string
	priority int64
	release  int64
	deadline int64
	actions  []intAction
}

type intInterrupt struct {
	id        string
	priority  int64
	arrival   int64
	execution int64
	deadline  int64
}

type intTimelineInterval struct {
	ActorType string `json:"actorType"`
	ActorID   string `json:"actorId"`
	Mode      string `json:"mode"`
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
}

type intTaskResult struct {
	ID             string `json:"id"`
	Executed       int64  `json:"executed"`
	Completion     *int64 `json:"completion"`
	DeadlineStatus string `json:"deadlineStatus"`
}

type intInterruptResult struct {
	ID             string `json:"id"`
	Executed       int64  `json:"executed"`
	Start          *int64 `json:"start"`
	Completion     *int64 `json:"completion"`
	Latency        *int64 `json:"latency"`
	DeadlineStatus string `json:"deadlineStatus"`
}

type intCriticalSection struct {
	TaskID           string `json:"taskId"`
	ActionIndex      int    `json:"actionIndex"`
	Start            *int64 `json:"start"`
	End              *int64 `json:"end"`
	ObservedDuration int64  `json:"observedDuration"`
	Completed        bool   `json:"completed"`
}

type interruptResponse struct {
	Status           string                `json:"status"`
	StoppedAt        int64                 `json:"stoppedAt"`
	Timeline         []intTimelineInterval `json:"timeline"`
	TaskResults      []intTaskResult       `json:"taskResults"`
	InterruptResults []intInterruptResult  `json:"interruptResults"`
	CriticalSections []intCriticalSection  `json:"criticalSections"`
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
	tasks, ints, ok := validateInterruptRequest(&req)
	if !ok {
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(simulateInterrupt(tasks, ints, *req.Horizon))
}

// validateInterruptRequest enforces the one-shot task constraints (without
// execution), the action-program rules (1..1024 actions, each exactly one of
// run/critical with positive microseconds) and the interrupt rules (1..4096
// interrupts with unique id, priority 0..255, arrival inside the horizon,
// positive execution and deadline after arrival). Per-task action totals and
// the global total of action durations plus interrupt executions must fit
// int64; any overflow fails validation.
func validateInterruptRequest(req *interruptRequest) ([]intTask, []intInterrupt, bool) {
	if req.Horizon == nil || *req.Horizon <= 0 || *req.Horizon > maxHorizon {
		return nil, nil, false
	}
	if req.Tasks == nil || len(*req.Tasks) == 0 || len(*req.Tasks) > maxTasks {
		return nil, nil, false
	}
	if req.Interrupts == nil || len(*req.Interrupts) == 0 || len(*req.Interrupts) > maxInterrupts {
		return nil, nil, false
	}
	horizon := *req.Horizon
	var total int64

	tasks := make([]intTask, 0, len(*req.Tasks))
	ids := make(map[string]struct{}, len(*req.Tasks))
	for i := range *req.Tasks {
		in := (*req.Tasks)[i]
		if !validTaskID(in.ID) {
			return nil, nil, false
		}
		if _, dup := ids[in.ID]; dup {
			return nil, nil, false
		}
		ids[in.ID] = struct{}{}
		if in.Priority == nil || *in.Priority < 0 || *in.Priority > 255 {
			return nil, nil, false
		}
		if in.Release == nil || *in.Release < 0 || *in.Release >= horizon {
			return nil, nil, false
		}
		if in.Deadline == nil || *in.Deadline <= *in.Release {
			return nil, nil, false
		}
		if in.Actions == nil || len(*in.Actions) == 0 || len(*in.Actions) > maxInterruptActions {
			return nil, nil, false
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
				return nil, nil, false
			}
			kind := intActRun
			dur := int64(0)
			if ai.Run != nil {
				dur = *ai.Run
			} else {
				kind = intActCritical
				dur = *ai.Critical
			}
			if dur <= 0 {
				return nil, nil, false
			}
			var ok bool
			taskTotal, ok = saturatingAdd(taskTotal, dur)
			if !ok {
				return nil, nil, false
			}
			actions = append(actions, intAction{kind: kind, dur: dur})
		}
		var ok bool
		total, ok = saturatingAdd(total, taskTotal)
		if !ok {
			return nil, nil, false
		}
		tasks = append(tasks, intTask{
			id:       in.ID,
			priority: *in.Priority,
			release:  *in.Release,
			deadline: *in.Deadline,
			actions:  actions,
		})
	}

	ints := make([]intInterrupt, 0, len(*req.Interrupts))
	iids := make(map[string]struct{}, len(*req.Interrupts))
	for i := range *req.Interrupts {
		in := (*req.Interrupts)[i]
		if !validTaskID(in.ID) {
			return nil, nil, false
		}
		if _, dup := iids[in.ID]; dup {
			return nil, nil, false
		}
		iids[in.ID] = struct{}{}
		if in.Priority == nil || *in.Priority < 0 || *in.Priority > 255 {
			return nil, nil, false
		}
		if in.Arrival == nil || *in.Arrival < 0 || *in.Arrival >= horizon {
			return nil, nil, false
		}
		if in.Execution == nil || *in.Execution <= 0 {
			return nil, nil, false
		}
		if in.Deadline == nil || *in.Deadline <= *in.Arrival {
			return nil, nil, false
		}
		var ok bool
		total, ok = saturatingAdd(total, *in.Execution)
		if !ok {
			return nil, nil, false
		}
		ints = append(ints, intInterrupt{
			id:        in.ID,
			priority:  *in.Priority,
			arrival:   *in.Arrival,
			execution: *in.Execution,
			deadline:  *in.Deadline,
		})
	}
	return tasks, ints, true
}

// intSegment is an unmerged execution interval recorded by the simulation.
type intSegment struct {
	actorType  string
	id         string
	mode       string
	start, end int64
}

// intSim is an event-driven single-core simulation with interrupts above
// tasks. Interrupts are fully preemptive among themselves by strict priority
// (a preempted interrupt suspends and resumes); a task's critical action,
// once started, runs to completion or the horizon without preemption. Only
// run and critical actions consume time.
type intSim struct {
	tasks   []intTask
	ints    []intInterrupt
	horizon int64

	tReleased   []bool
	tDone       []bool
	pc          []int   // next action to execute
	runRem      []int64 // remainder of the run action at pc
	critRem     []int64 // remainder of the critical action at pc
	tExecuted   []int64
	tCompletion []*int64

	iArrived    []bool
	iDone       []bool
	iRem        []int64
	iExecuted   []int64
	iStart      []*int64
	iCompletion []*int64

	crits []map[int]*intCriticalSection // task -> actionIndex -> record

	tOrder []int // tasks sorted by (release, input index)
	iOrder []int // interrupts sorted by (arrival, input index)
	tNext  int
	iNext  int

	inCritical int // task whose critical section is executing, -1 otherwise
	now        int64
	segs       []intSegment
	doneTasks  int
	doneInts   int
}

func simulateInterrupt(tasks []intTask, ints []intInterrupt, horizon int64) interruptResponse {
	n, m := len(tasks), len(ints)
	s := &intSim{
		tasks:       tasks,
		ints:        ints,
		horizon:     horizon,
		tReleased:   make([]bool, n),
		tDone:       make([]bool, n),
		pc:          make([]int, n),
		runRem:      make([]int64, n),
		critRem:     make([]int64, n),
		tExecuted:   make([]int64, n),
		tCompletion: make([]*int64, n),
		iArrived:    make([]bool, m),
		iDone:       make([]bool, m),
		iRem:        make([]int64, m),
		iExecuted:   make([]int64, m),
		iStart:      make([]*int64, m),
		iCompletion: make([]*int64, m),
		crits:       make([]map[int]*intCriticalSection, n),
		inCritical:  -1,
	}
	for i, in := range ints {
		s.iRem[i] = in.execution
	}
	for t, task := range tasks {
		s.crits[t] = make(map[int]*intCriticalSection)
		for ai, a := range task.actions {
			if a.kind == intActCritical {
				s.crits[t][ai] = &intCriticalSection{TaskID: task.id, ActionIndex: ai}
			}
		}
	}
	s.tOrder = make([]int, n)
	for i := range s.tOrder {
		s.tOrder[i] = i
	}
	sort.Slice(s.tOrder, func(a, b int) bool {
		ia, ib := s.tOrder[a], s.tOrder[b]
		if tasks[ia].release != tasks[ib].release {
			return tasks[ia].release < tasks[ib].release
		}
		return ia < ib
	})
	s.iOrder = make([]int, m)
	for i := range s.iOrder {
		s.iOrder[i] = i
	}
	sort.Slice(s.iOrder, func(a, b int) bool {
		ia, ib := s.iOrder[a], s.iOrder[b]
		if ints[ia].arrival != ints[ib].arrival {
			return ints[ia].arrival < ints[ib].arrival
		}
		return ia < ib
	})

	for s.now < horizon {
		// Incorporate every arrival and release at the current instant before
		// making any scheduling decision.
		s.releaseUpTo()
		s.arriveUpTo()

		if s.inCritical != -1 {
			s.runCritical()
		} else if bi := s.bestInterrupt(); bi != -1 {
			s.runInterrupt(bi)
		} else if bt := s.bestTask(); bt != -1 {
			a := s.tasks[bt].actions[s.pc[bt]]
			if a.kind == intActCritical {
				if s.critRem[bt] == 0 {
					s.critRem[bt] = a.dur
				}
				s.beginCritical(bt)
				s.runCritical()
			} else {
				if s.runRem[bt] == 0 {
					s.runRem[bt] = a.dur
				}
				s.runTask(bt)
			}
		} else {
			// Idle: jump to the next release or arrival (or the horizon).
			next := horizon
			if s.tNext < n && tasks[s.tOrder[s.tNext]].release < next {
				next = tasks[s.tOrder[s.tNext]].release
			}
			if s.iNext < m && ints[s.iOrder[s.iNext]].arrival < next {
				next = ints[s.iOrder[s.iNext]].arrival
			}
			s.now = next
			continue
		}
		if s.doneTasks == n && s.doneInts == m {
			break
		}
	}
	return s.buildResponse()
}

func (s *intSim) releaseUpTo() {
	for s.tNext < len(s.tasks) && s.tasks[s.tOrder[s.tNext]].release <= s.now {
		s.tReleased[s.tOrder[s.tNext]] = true
		s.tNext++
	}
}

func (s *intSim) arriveUpTo() {
	for s.iNext < len(s.ints) && s.ints[s.iOrder[s.iNext]].arrival <= s.now {
		s.iArrived[s.iOrder[s.iNext]] = true
		s.iNext++
	}
}

// bestInterrupt selects the arrived, unfinished interrupt with the highest
// priority, breaking ties by arrival then input index. Arrivals are
// monotonic, so the running interrupt always keeps the earliest arrival among
// equal-priority contenders and is never displaced by them.
func (s *intSim) bestInterrupt() int {
	best := -1
	for i := range s.ints {
		if !s.iArrived[i] || s.iDone[i] {
			continue
		}
		if best == -1 {
			best = i
			continue
		}
		a, b := s.ints[i], s.ints[best]
		if a.priority < b.priority ||
			(a.priority == b.priority && a.arrival < b.arrival) ||
			(a.priority == b.priority && a.arrival == b.arrival && i < best) {
			best = i
		}
	}
	return best
}

// bestTask selects the released, unfinished task with the highest priority,
// breaking ties by release then input index.
func (s *intSim) bestTask() int {
	best := -1
	for i := range s.tasks {
		if !s.tReleased[i] || s.tDone[i] {
			continue
		}
		if best == -1 {
			best = i
			continue
		}
		a, b := s.tasks[i], s.tasks[best]
		if a.priority < b.priority ||
			(a.priority == b.priority && a.release < b.release) ||
			(a.priority == b.priority && a.release == b.release && i < best) {
			best = i
		}
	}
	return best
}

// beginCritical opens the critical section of the action at pc: from this
// instant until it ends (or the horizon) nothing preempts the task.
func (s *intSim) beginCritical(t int) {
	now := s.now
	s.crits[t][s.pc[t]].Start = &now
	s.inCritical = t
}

// runCritical advances the open critical section to its end or the horizon;
// no event can cut it.
func (s *intSim) runCritical() {
	t := s.inCritical
	end := s.now + s.critRem[t]
	if end > s.horizon {
		end = s.horizon
	}
	s.segs = append(s.segs, intSegment{actorType: intActorTask, id: s.tasks[t].id, mode: intModeCritical, start: s.now, end: end})
	delta := end - s.now
	s.tExecuted[t] += delta
	s.critRem[t] -= delta
	s.now = end
	cs := s.crits[t][s.pc[t]]
	cs.ObservedDuration += delta
	if s.critRem[t] == 0 {
		done := s.now
		cs.End = &done
		cs.Completed = true
		s.inCritical = -1
		s.advanceTask(t)
	}
}

// runInterrupt advances interrupt i until it completes, the horizon cuts it,
// or a strictly higher priority interrupt arrives. Equal-priority arrivals
// never cut.
func (s *intSim) runInterrupt(i int) {
	end := s.now + s.iRem[i]
	if end > s.horizon {
		end = s.horizon
	}
	for k := s.iNext; k < len(s.ints); k++ {
		j := s.iOrder[k]
		if arr := s.ints[j].arrival; arr < end {
			if s.ints[j].priority < s.ints[i].priority {
				end = arr
				break
			}
		} else {
			break
		}
	}
	if s.iStart[i] == nil {
		now := s.now
		s.iStart[i] = &now
	}
	s.segs = append(s.segs, intSegment{actorType: intActorInterrupt, id: s.ints[i].id, mode: intModeRun, start: s.now, end: end})
	delta := end - s.now
	s.iExecuted[i] += delta
	s.iRem[i] -= delta
	s.now = end
	if s.iRem[i] == 0 {
		s.iDone[i] = true
		done := s.now
		s.iCompletion[i] = &done
		s.doneInts++
	}
}

// runTask advances the run action of task t until the action completes, the
// horizon cuts it, any interrupt arrives, or a strictly higher priority task
// releases.
func (s *intSim) runTask(t int) {
	end := s.now + s.runRem[t]
	if end > s.horizon {
		end = s.horizon
	}
	// Any interrupt arrival preempts a task's run action.
	if s.iNext < len(s.ints) {
		if arr := s.ints[s.iOrder[s.iNext]].arrival; arr < end {
			end = arr
		}
	}
	// A strictly higher priority task release preempts; equal never does.
	for k := s.tNext; k < len(s.tasks); k++ {
		j := s.tOrder[k]
		if rel := s.tasks[j].release; rel < end {
			if s.tasks[j].priority < s.tasks[t].priority {
				end = rel
				break
			}
		} else {
			break
		}
	}
	s.segs = append(s.segs, intSegment{actorType: intActorTask, id: s.tasks[t].id, mode: intModeRun, start: s.now, end: end})
	delta := end - s.now
	s.tExecuted[t] += delta
	s.runRem[t] -= delta
	s.now = end
	if s.runRem[t] == 0 {
		s.advanceTask(t)
	}
}

func (s *intSim) advanceTask(t int) {
	s.pc[t]++
	if s.pc[t] >= len(s.tasks[t].actions) {
		s.tDone[t] = true
		done := s.now
		s.tCompletion[t] = &done
		s.doneTasks++
	}
}

func intDeadlineStatus(completion *int64, deadline, horizon int64) string {
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

func (s *intSim) buildResponse() interruptResponse {
	timeline := make([]intTimelineInterval, 0, len(s.segs))
	for _, seg := range s.segs {
		last := len(timeline) - 1
		if last >= 0 &&
			timeline[last].ActorType == seg.actorType &&
			timeline[last].ActorID == seg.id &&
			timeline[last].Mode == seg.mode &&
			timeline[last].End == seg.start {
			timeline[last].End = seg.end
			continue
		}
		timeline = append(timeline, intTimelineInterval{
			ActorType: seg.actorType,
			ActorID:   seg.id,
			Mode:      seg.mode,
			Start:     seg.start,
			End:       seg.end,
		})
	}

	taskResults := make([]intTaskResult, len(s.tasks))
	for i, t := range s.tasks {
		taskResults[i] = intTaskResult{
			ID:             t.id,
			Executed:       s.tExecuted[i],
			Completion:     s.tCompletion[i],
			DeadlineStatus: intDeadlineStatus(s.tCompletion[i], t.deadline, s.horizon),
		}
	}

	intResults := make([]intInterruptResult, len(s.ints))
	for i, in := range s.ints {
		var latency *int64
		if s.iStart[i] != nil {
			l := *s.iStart[i] - in.arrival
			latency = &l
		}
		intResults[i] = intInterruptResult{
			ID:             in.id,
			Executed:       s.iExecuted[i],
			Start:          s.iStart[i],
			Completion:     s.iCompletion[i],
			Latency:        latency,
			DeadlineStatus: intDeadlineStatus(s.iCompletion[i], in.deadline, s.horizon),
		}
	}

	crits := make([]intCriticalSection, 0)
	for t := range s.tasks {
		for ai, a := range s.tasks[t].actions {
			if a.kind == intActCritical {
				crits = append(crits, *s.crits[t][ai])
			}
		}
	}

	status := "horizon"
	stoppedAt := s.horizon
	if s.doneTasks == len(s.tasks) && s.doneInts == len(s.ints) {
		status = "completed"
		stoppedAt = s.now
	}

	return interruptResponse{
		Status:           status,
		StoppedAt:        stoppedAt,
		Timeline:         timeline,
		TaskResults:      taskResults,
		InterruptResults: intResults,
		CriticalSections: crits,
	}
}
