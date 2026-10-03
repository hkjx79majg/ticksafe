package server

import (
	"encoding/json"
	"net/http"
	"sort"
	"unicode/utf8"
)

const (
	mailboxAnalyzePath = "/v1/schedules/mailbox/analyze"
	maxMailboxes       = 256
	maxMailCapacity    = 65535
	maxMessageRunes    = 256
	maxMailboxActions  = 1024
)

// Mailbox action discriminators.
const (
	mailActRun int = iota
	mailActSend
	mailActReceive
)

type mailboxDeclIn struct {
	Name     string `json:"name"`
	Capacity *int64 `json:"capacity"`
}

type mailboxSendIn struct {
	Mailbox *string `json:"mailbox"`
	Message *string `json:"message"`
}

type mailboxActionIn struct {
	Run     *int64         `json:"run"`
	Send    *mailboxSendIn `json:"send"`
	Receive *string        `json:"receive"`
}

type mailboxTaskIn struct {
	ID       string             `json:"id"`
	Priority *int64             `json:"priority"`
	Release  *int64             `json:"release"`
	Deadline *int64             `json:"deadline"`
	Actions  *[]mailboxActionIn `json:"actions"`
}

type mailboxRequest struct {
	Horizon   *int64           `json:"horizon"`
	Mailboxes *[]mailboxDeclIn `json:"mailboxes"`
	Tasks     *[]mailboxTaskIn `json:"tasks"`
}

type mailboxAction struct {
	kind    int
	run     int64
	mailbox string
	message string
}

type mailboxTask struct {
	id       string
	priority int64
	release  int64
	deadline int64
	actions  []mailboxAction
}

type mailboxDecl struct {
	name     string
	capacity int64
}

type mailboxTimelineInterval struct {
	TaskID string `json:"taskId"`
	Start  int64  `json:"start"`
	End    int64  `json:"end"`
}

type receivedMessage struct {
	Mailbox string `json:"mailbox"`
	Message string `json:"message"`
	Time    int64  `json:"time"`
}

type mailboxTaskResult struct {
	Executed       int64             `json:"executed"`
	Completion     *int64            `json:"completion"`
	State          string            `json:"state"`
	DeadlineStatus string            `json:"deadlineStatus"`
	BlockedAction  *string           `json:"blockedAction"`
	BlockedOn      *string           `json:"blockedOn"`
	Received       []receivedMessage `json:"received"`
}

type mailboxResponse struct {
	Status    string                    `json:"status"`
	StoppedAt int64                     `json:"stoppedAt"`
	Timeline  []mailboxTimelineInterval `json:"timeline"`
	Results   []mailboxTaskResult       `json:"results"`
}

func handleMailboxAnalyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAnalyzeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req mailboxRequest
	if !decodeStrictJSON(w, r, &req) {
		return
	}
	boxes, tasks, ok := validateMailboxRequest(&req)
	if !ok {
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(simulateMailbox(boxes, tasks, *req.Horizon))
}

// validMessage reports whether s holds 1..256 valid Unicode code points.
func validMessage(s string) bool {
	if s == "" {
		return false
	}
	count := 0
	for len(s) > 0 {
		r, size := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError && size == 1 {
			return false
		}
		count++
		s = s[size:]
	}
	return count <= maxMessageRunes
}

// validateMailboxRequest enforces the one-shot task constraints plus the
// mailbox declaration and action-program rules: exactly one verb per action,
// positive run microseconds, 1..256-code-point messages, references to
// declared mailboxes only, and an int64-wide per-task and global run total.
func validateMailboxRequest(req *mailboxRequest) ([]mailboxDecl, []mailboxTask, bool) {
	if req.Horizon == nil || *req.Horizon <= 0 || *req.Horizon > maxHorizon {
		return nil, nil, false
	}
	if req.Mailboxes == nil || len(*req.Mailboxes) == 0 || len(*req.Mailboxes) > maxMailboxes {
		return nil, nil, false
	}
	if req.Tasks == nil || len(*req.Tasks) == 0 || len(*req.Tasks) > maxTasks {
		return nil, nil, false
	}
	boxes := make([]mailboxDecl, 0, len(*req.Mailboxes))
	declared := make(map[string]struct{}, len(*req.Mailboxes))
	for i := range *req.Mailboxes {
		in := (*req.Mailboxes)[i]
		if !validTaskID(in.Name) {
			return nil, nil, false
		}
		if _, dup := declared[in.Name]; dup {
			return nil, nil, false
		}
		declared[in.Name] = struct{}{}
		if in.Capacity == nil || *in.Capacity <= 0 || *in.Capacity > maxMailCapacity {
			return nil, nil, false
		}
		boxes = append(boxes, mailboxDecl{name: in.Name, capacity: *in.Capacity})
	}

	horizon := *req.Horizon
	tasks := make([]mailboxTask, 0, len(*req.Tasks))
	ids := make(map[string]struct{}, len(*req.Tasks))
	var totalRun int64
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
		if in.Actions == nil || len(*in.Actions) == 0 || len(*in.Actions) > maxMailboxActions {
			return nil, nil, false
		}

		actions := make([]mailboxAction, 0, len(*in.Actions))
		var taskRun int64
		for j := range *in.Actions {
			ai := (*in.Actions)[j]
			verbs := 0
			if ai.Run != nil {
				verbs++
			}
			if ai.Send != nil {
				verbs++
			}
			if ai.Receive != nil {
				verbs++
			}
			if verbs != 1 {
				return nil, nil, false
			}
			switch {
			case ai.Run != nil:
				if *ai.Run <= 0 {
					return nil, nil, false
				}
				var ok bool
				taskRun, ok = saturatingAdd(taskRun, *ai.Run)
				if !ok {
					return nil, nil, false
				}
				actions = append(actions, mailboxAction{kind: mailActRun, run: *ai.Run})
			case ai.Send != nil:
				if ai.Send.Mailbox == nil || ai.Send.Message == nil {
					return nil, nil, false
				}
				m := *ai.Send.Mailbox
				if _, ok := declared[m]; !ok {
					return nil, nil, false
				}
				if !validMessage(*ai.Send.Message) {
					return nil, nil, false
				}
				actions = append(actions, mailboxAction{
					kind:    mailActSend,
					mailbox: m,
					message: *ai.Send.Message,
				})
			default:
				m := *ai.Receive
				if _, ok := declared[m]; !ok {
					return nil, nil, false
				}
				actions = append(actions, mailboxAction{kind: mailActReceive, mailbox: m})
			}
		}
		var ok bool
		totalRun, ok = saturatingAdd(totalRun, taskRun)
		if !ok {
			return nil, nil, false
		}
		tasks = append(tasks, mailboxTask{
			id:       in.ID,
			priority: *in.Priority,
			release:  *in.Release,
			deadline: *in.Deadline,
			actions:  actions,
		})
	}
	return boxes, tasks, true
}

// mailboxSegment is an unmerged run interval recorded by the simulation.
type mailboxSegment struct {
	index      int
	start, end int64
}

// mailboxSim is an event-driven single-core fixed-priority simulation with
// bounded FIFO mailboxes. Time advances only through run actions; sends,
// receives, blocking, wakeups and completion happen at a single instant.
//
// Per mailbox, blocked senders exist only while the queue is full and blocked
// receivers only while it is empty, so the two lists can never both be
// nonempty: a send is either handed directly to a blocked receiver, enqueued
// in a free slot, or blocks carrying its message.
type mailboxSim struct {
	boxes []mailboxDecl
	tasks []mailboxTask
	n     int

	queue     map[string][]string // mailbox -> queued messages (FIFO)
	fullWait  map[string][]int    // mailbox -> blocked senders
	emptyWait map[string][]int    // mailbox -> blocked receivers

	blockedM    []string // "" when not blocked
	blockedKind []int    // mailActSend / mailActReceive when blocked
	blockTime   []int64
	pendingMsg  []string // message carried by a blocked sender
	pc          []int    // next action to interpret
	runRem      []int64  // remainder of the run action at pc

	released   []bool
	completed  []bool
	executed   []int64
	completion []*int64
	received   [][]receivedMessage

	order   []int // tasks sorted by (release, input index)
	nextRel int
	segs    []mailboxSegment
	current int
	now     int64
}

func simulateMailbox(boxes []mailboxDecl, tasks []mailboxTask, horizon int64) mailboxResponse {
	n := len(tasks)
	s := &mailboxSim{
		boxes:       boxes,
		tasks:       tasks,
		n:           n,
		queue:       make(map[string][]string),
		fullWait:    make(map[string][]int),
		emptyWait:   make(map[string][]int),
		blockedM:    make([]string, n),
		blockedKind: make([]int, n),
		blockTime:   make([]int64, n),
		pendingMsg:  make([]string, n),
		pc:          make([]int, n),
		runRem:      make([]int64, n),
		released:    make([]bool, n),
		completed:   make([]bool, n),
		executed:    make([]int64, n),
		completion:  make([]*int64, n),
		received:    make([][]receivedMessage, n),
		current:     -1,
	}
	s.order = make([]int, n)
	for i := range s.order {
		s.order[i] = i
	}
	sort.Slice(s.order, func(a, b int) bool {
		ia, ib := s.order[a], s.order[b]
		if tasks[ia].release != tasks[ib].release {
			return tasks[ia].release < tasks[ib].release
		}
		return ia < ib
	})

	stopped := false
loop:
	for {
		s.releaseUpTo()

		// After any state change, first drain the current task's consecutive
		// zero-duration actions, then schedule.
		if s.current != -1 {
			s.settle(s.current)
			if !s.runnable(s.current) {
				s.current = -1
			}
		}

		if s.current == -1 {
			best := s.pickBest()
			if best == -1 {
				if s.nextRel >= n {
					// All tasks have been released. If none is runnable the
					// unfinished ones are all blocked: the system has stalled;
					// if everything already completed, the analysis completed.
					if s.allCompleted() {
						break
					}
					stopped = true
					break loop
				}
				s.now = tasks[s.order[s.nextRel]].release
				continue
			}
			s.current = best
			s.settle(best)
			if !s.runnable(best) {
				s.current = -1
				continue
			}
		}

		// A freshly selected task's zero-duration chain (or a delivery made by
		// the current task) can unblock a strictly higher-priority task at the
		// same instant; re-resolve until stable. Equal priority never preempts.
		for {
			best := s.pickBest()
			if best != -1 && best != s.current &&
				tasks[best].priority < tasks[s.current].priority {
				s.current = best
				s.settle(best)
				if !s.runnable(best) {
					s.current = -1
					continue loop
				}
				continue
			}
			break
		}

		if s.now >= horizon {
			break
		}
		cur := s.current
		// Advance until the run completes, the horizon cuts it, or a strictly
		// higher-priority task releases. Mailbox state changes only at
		// zero-duration instants, never mid-run.
		rem := s.runRem[cur]
		length := rem
		if avail := horizon - s.now; length > avail {
			length = avail
		}
		end := s.now + length
		for k := s.nextRel; k < n; k++ {
			j := s.order[k]
			if tasks[j].priority < tasks[cur].priority &&
				tasks[j].release > s.now && tasks[j].release < end {
				end = tasks[j].release
			}
		}
		s.segs = append(s.segs, mailboxSegment{index: cur, start: s.now, end: end})
		delta := end - s.now
		s.executed[cur] += delta
		s.now = end
		if rem -= delta; rem > 0 {
			s.runRem[cur] = rem
			continue
		}
		s.runRem[cur] = 0
		s.pc[cur]++
		if s.now >= horizon {
			// A run ending exactly at the horizon: matching the mutex analysis,
			// drain only this task's immediately following zero-duration actions
			// once. Nothing else runs at or past the horizon.
			s.releaseUpTo()
			s.settle(cur)
			break
		}
	}

	return s.buildResponse(stopped, horizon)
}

// settle drains the task's consecutive zero-duration actions. It stops when
// the task faces a run action (arming its remainder), blocks, or completes.
func (s *mailboxSim) settle(i int) {
	for {
		t := s.tasks[i]
		if s.pc[i] >= len(t.actions) {
			s.completed[i] = true
			now := s.now
			s.completion[i] = &now
			return
		}
		a := t.actions[s.pc[i]]
		switch a.kind {
		case mailActRun:
			if s.runRem[i] == 0 {
				s.runRem[i] = a.run
			}
			return
		case mailActSend:
			m := a.message
			if ws := s.emptyWait[a.mailbox]; len(ws) > 0 {
				// Direct delivery to the earliest blocked receiver; same-instant
				// ties break by input order. The receiver is unblocked but its
				// own zero-duration chain runs only once the scheduler picks it:
				// the current (sending) task drains its chain first.
				w := s.pickEarliest(ws)
				s.emptyWait[a.mailbox] = removeTask(ws, w)
				s.blockedM[w] = ""
				s.deliver(w, a.mailbox, m)
				s.pc[w]++
				s.pc[i]++
				continue
			}
			box := s.findBox(a.mailbox)
			q := s.queue[a.mailbox]
			if int64(len(q)) < box.capacity {
				s.queue[a.mailbox] = append(q, m)
				s.pc[i]++
				continue
			}
			// Queue full: the sender blocks carrying its message.
			s.blockedM[i] = a.mailbox
			s.blockedKind[i] = mailActSend
			s.blockTime[i] = s.now
			s.pendingMsg[i] = m
			s.fullWait[a.mailbox] = append(s.fullWait[a.mailbox], i)
			return
		case mailActReceive:
			name := a.mailbox
			q := s.queue[name]
			if len(q) == 0 {
				// Empty mailbox: the receiver blocks.
				s.blockedM[i] = name
				s.blockedKind[i] = mailActReceive
				s.blockTime[i] = s.now
				s.emptyWait[name] = append(s.emptyWait[name], i)
				return
			}
			msg := q[0]
			s.queue[name] = q[1:]
			s.deliver(i, name, msg)
			// If the mailbox was full before the dequeue, the earliest blocked
			// sender's message fills the slot at the tail immediately,
			// completing its send.
			if ss := s.fullWait[name]; len(ss) > 0 {
				w := s.pickEarliest(ss)
				s.fullWait[name] = removeTask(ss, w)
				s.blockedM[w] = ""
				s.queue[name] = append(s.queue[name], s.pendingMsg[w])
				s.pendingMsg[w] = ""
				s.pc[w]++
			}
			s.pc[i]++
		}
	}
}

// deliver records a message arriving at task i at the current instant.
func (s *mailboxSim) deliver(i int, mailbox, message string) {
	s.received[i] = append(s.received[i], receivedMessage{
		Mailbox: mailbox,
		Message: message,
		Time:    s.now,
	})
}

// pickEarliest returns the member of ids with the smallest block time,
// breaking ties by input order.
func (s *mailboxSim) pickEarliest(ids []int) int {
	best := ids[0]
	for _, x := range ids[1:] {
		if s.blockTime[x] < s.blockTime[best] ||
			(s.blockTime[x] == s.blockTime[best] && x < best) {
			best = x
		}
	}
	return best
}

// removeTask returns ids without x (ids contains x exactly once).
func removeTask(ids []int, x int) []int {
	out := ids[:0]
	for _, v := range ids {
		if v != x {
			out = append(out, v)
		}
	}
	return out
}

func (s *mailboxSim) findBox(name string) mailboxDecl {
	for _, b := range s.boxes {
		if b.name == name {
			return b
		}
	}
	return mailboxDecl{}
}

// pickBest selects the runnable task with the highest priority, breaking ties
// by release time then input index.
func (s *mailboxSim) pickBest() int {
	best := -1
	for i := 0; i < s.n; i++ {
		if !s.released[i] || s.completed[i] || s.blockedM[i] != "" {
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

func (s *mailboxSim) allCompleted() bool {
	for i := 0; i < s.n; i++ {
		if !s.completed[i] {
			return false
		}
	}
	return true
}

func (s *mailboxSim) runnable(i int) bool {
	return s.released[i] && !s.completed[i] && s.blockedM[i] == "" && s.runRem[i] > 0
}

func (s *mailboxSim) releaseUpTo() {
	for s.nextRel < s.n && s.tasks[s.order[s.nextRel]].release <= s.now {
		s.released[s.order[s.nextRel]] = true
		s.nextRel++
	}
}

func (s *mailboxSim) buildResponse(stalled bool, horizon int64) mailboxResponse {
	timeline := make([]mailboxTimelineInterval, 0, len(s.segs))
	for _, seg := range s.segs {
		last := len(timeline) - 1
		if last >= 0 &&
			timeline[last].TaskID == s.tasks[seg.index].id &&
			timeline[last].End == seg.start {
			timeline[last].End = seg.end
			continue
		}
		timeline = append(timeline, mailboxTimelineInterval{
			TaskID: s.tasks[seg.index].id,
			Start:  seg.start,
			End:    seg.end,
		})
	}

	results := make([]mailboxTaskResult, s.n)
	allCompleted := true
	for i, t := range s.tasks {
		state := "ready"
		switch {
		case s.completed[i]:
			state = "completed"
		case s.blockedM[i] != "":
			state = "blocked"
		case !s.released[i]:
			state = "unreleased"
		}
		if state != "completed" {
			allCompleted = false
		}
		var blockedAction, blockedOn *string
		if state == "blocked" {
			action := "send"
			if s.blockedKind[i] == mailActReceive {
				action = "receive"
			}
			blockedAction = &action
			m := s.blockedM[i]
			blockedOn = &m
		}
		var deadlineStatus string
		switch c := s.completion[i]; {
		case c != nil && *c <= t.deadline:
			deadlineStatus = "met"
		case c != nil:
			deadlineStatus = "missed"
		case t.deadline <= horizon:
			deadlineStatus = "missed"
		default:
			deadlineStatus = "pending"
		}
		received := s.received[i]
		if received == nil {
			received = []receivedMessage{}
		}
		results[i] = mailboxTaskResult{
			Executed:       s.executed[i],
			Completion:     s.completion[i],
			State:          state,
			DeadlineStatus: deadlineStatus,
			BlockedAction:  blockedAction,
			BlockedOn:      blockedOn,
			Received:       received,
		}
	}

	status := "horizon"
	switch {
	case stalled:
		status = "stalled"
	case allCompleted:
		status = "completed"
	}
	return mailboxResponse{
		Status:    status,
		StoppedAt: s.now,
		Timeline:  timeline,
		Results:   results,
	}
}
