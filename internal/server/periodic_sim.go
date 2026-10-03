package server

import (
	"container/heap"
	"encoding/json"
	"net/http"
)

const (
	periodicSimPath = "/v1/schedules/periodic/simulate"
	// maxPeriodicJobs bounds the jobs expanded over all tasks; a window that
	// would release more jobs is rejected before any simulation work.
	maxPeriodicJobs = 100_000
)

type periodicSimTaskIn struct {
	ID        string `json:"id"`
	Priority  *int64 `json:"priority"`
	Execution *int64 `json:"execution"`
	Period    *int64 `json:"period"`
	Deadline  *int64 `json:"deadline"`
	// Offset is captured raw so that an omitted field defaults to zero while
	// an explicit null or a non-integer value is a 422 type error.
	Offset json.RawMessage `json:"offset"`
}

type periodicSimRequest struct {
	Horizon *int64               `json:"horizon"`
	Tasks   *[]periodicSimTaskIn `json:"tasks"`
}

type periodicTimelineInterval struct {
	TaskID string `json:"taskId"`
	Job    int64  `json:"job"`
	Start  int64  `json:"start"`
	End    int64  `json:"end"`
}

type periodicJobResult struct {
	Job              int64  `json:"job"`
	Release          int64  `json:"release"`
	AbsoluteDeadline int64  `json:"absoluteDeadline"`
	Executed         int64  `json:"executed"`
	Remaining        int64  `json:"remaining"`
	Completion       *int64 `json:"completion"`
	DeadlineStatus   string `json:"deadlineStatus"`
}

type periodicTaskSimResult struct {
	ID   string              `json:"id"`
	Jobs []periodicJobResult `json:"jobs"`
}

type periodicSimResponse struct {
	Status   string                     `json:"status"`
	Timeline []periodicTimelineInterval `json:"timeline"`
	Results  []periodicTaskSimResult    `json:"results"`
}

func handlePeriodicSimulate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAnalyzeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req periodicSimRequest
	if !decodeStrictJSON(w, r, &req) {
		return
	}
	tasks, ok := validatePeriodicSimRequest(&req)
	if !ok {
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	resp := simulatePeriodic(tasks, *req.Horizon)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// validatePeriodicSimRequest reuses the periodic-analysis field rules and adds
// horizon and offset checks. Fields are rejected in the same 422 cases as the
// WCRT entry; offsets default to zero.
func validatePeriodicSimRequest(req *periodicSimRequest) ([]periodicTask, bool) {
	if req.Horizon == nil || *req.Horizon <= 0 || *req.Horizon > maxHorizon {
		return nil, false
	}
	if req.Tasks == nil || len(*req.Tasks) == 0 || len(*req.Tasks) > maxTasks {
		return nil, false
	}
	horizon := *req.Horizon
	tasks := make([]periodicTask, 0, len(*req.Tasks))
	ids := make(map[string]struct{}, len(*req.Tasks))
	priorities := make(map[int64]struct{}, len(*req.Tasks))
	var jobCount int64
	for i := range *req.Tasks {
		in := (*req.Tasks)[i]
		if !validTaskID(in.ID) {
			return nil, false
		}
		if _, dup := ids[in.ID]; dup {
			return nil, false
		}
		ids[in.ID] = struct{}{}
		if in.Priority == nil || *in.Priority < 0 || *in.Priority > 255 {
			return nil, false
		}
		if _, dup := priorities[*in.Priority]; dup {
			return nil, false
		}
		priorities[*in.Priority] = struct{}{}
		if in.Execution == nil || *in.Execution <= 0 || *in.Execution > maxPeriodicValue {
			return nil, false
		}
		if in.Period == nil || *in.Period <= 0 || *in.Period > maxPeriodicValue {
			return nil, false
		}
		if in.Deadline == nil || *in.Deadline <= 0 || *in.Deadline > maxPeriodicValue ||
			*in.Deadline > *in.Period {
			return nil, false
		}
		offset := int64(0)
		if len(in.Offset) > 0 {
			// Present: distinguish null/non-integer (422) from an integer.
			if string(in.Offset) == "null" {
				return nil, false
			}
			if err := json.Unmarshal(in.Offset, &offset); err != nil {
				return nil, false
			}
		}
		if offset < 0 || offset >= *in.Period || offset >= horizon {
			return nil, false
		}
		// Jobs release at offset+k*period while strictly below horizon, so the
		// count is ceil((horizon-offset)/period); every operand is at most
		// 1e12 and offset < horizon guarantees at least one job.
		count := (horizon - offset + *in.Period - 1) / *in.Period
		if count > maxPeriodicJobs-jobCount {
			return nil, false
		}
		jobCount += count
		tasks = append(tasks, periodicTask{
			id:        in.ID,
			priority:  *in.Priority,
			execution: *in.Execution,
			period:    *in.Period,
			deadline:  *in.Deadline,
			offset:    offset,
		})
	}
	return tasks, true
}

// periodicJob is one expanded release; jobs are numbered from zero per task.
type periodicJob struct {
	task       int
	job        int64
	release    int64
	executed   int64
	remaining  int64
	completion int64 // -1 while unfinished
}

// readyItem orders ready jobs by (priority, release, task, job number). The
// task and job tie breaks make a task's jobs run in number order and keep the
// schedule deterministic across otherwise-equal releases.
type readyItem struct {
	jobIdx  int
	prio    int64
	release int64
	task    int
	job     int64
}

type readyHeap []*readyItem

func (h readyHeap) Len() int { return len(h) }
func (h readyHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if a.prio != b.prio {
		return a.prio < b.prio
	}
	if a.release != b.release {
		return a.release < b.release
	}
	if a.task != b.task {
		return a.task < b.task
	}
	return a.job < b.job
}
func (h readyHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *readyHeap) Push(x any)   { *h = append(*h, x.(*readyItem)) }
func (h *readyHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

type periodicSeg struct {
	task  int
	job   int64
	start int64
	end   int64
}

// simulatePeriodic runs an event-driven single-core fixed-priority fully
// preemptive simulation over [0, horizon). Each task releases a job at
// offset+k*period for every release strictly below horizon; every job has the
// same execution demand and relative deadline. Overdue jobs keep executing.
// The processor runs the ready job with the numerically smallest priority;
// strictly higher-priority releases preempt immediately, equal-priority
// releases never do, and a task's jobs run in number order. Idle intervals
// advance to the next release; a job finishing exactly at the horizon is
// complete.
func simulatePeriodic(tasks []periodicTask, horizon int64) periodicSimResponse {
	nt := len(tasks)

	// Per-task cursors over the release sequence offset+k*period; jobs are
	// expanded lazily as releases become due.
	jobCounts := make([]int, nt)
	totalJobs := 0
	for i, t := range tasks {
		jobCounts[i] = int((horizon - t.offset + t.period - 1) / t.period)
		totalJobs += jobCounts[i]
	}
	nextJob := make([]int64, nt)
	releaseAt := func(i int, k int64) int64 {
		return tasks[i].offset + k*tasks[i].period
	}
	remainingReleases := func(i int) int {
		return jobCounts[i] - int(nextJob[i])
	}

	jobs := make([]periodicJob, 0, totalJobs)
	ready := &readyHeap{}
	heap.Init(ready)

	expandDue := func(upto int64) {
		for i := 0; i < nt; i++ {
			for remainingReleases(i) > 0 {
				rt := releaseAt(i, nextJob[i])
				if rt > upto {
					break
				}
				idx := len(jobs)
				jobs = append(jobs, periodicJob{
					task:       i,
					job:        nextJob[i],
					release:    rt,
					remaining:  tasks[i].execution,
					completion: -1,
				})
				nextJob[i]++
				heap.Push(ready, &readyItem{
					jobIdx:  idx,
					prio:    tasks[i].priority,
					release: rt,
					task:    i,
					job:     jobs[idx].job,
				})
			}
		}
	}

	// nextReleaseTime reports the earliest future release across all tasks.
	nextReleaseTime := func() (int64, bool) {
		best := int64(0)
		found := false
		for i := 0; i < nt; i++ {
			if remainingReleases(i) == 0 {
				continue
			}
			rt := releaseAt(i, nextJob[i])
			if !found || rt < best {
				best, found = rt, true
			}
		}
		return best, found
	}

	// popReady drops entries of finished jobs and returns the best ready job.
	popReady := func() int {
		for ready.Len() > 0 {
			it := heap.Pop(ready).(*readyItem)
			if jobs[it.jobIdx].remaining > 0 {
				return it.jobIdx
			}
		}
		return -1
	}
	// peekReady is the non-consuming variant used to test preemption.
	peekReady := func() int {
		for ready.Len() > 0 {
			it := (*ready)[0]
			if jobs[it.jobIdx].remaining > 0 {
				return it.jobIdx
			}
			heap.Pop(ready)
		}
		return -1
	}

	var segments []periodicSeg
	current := -1
	var now int64
	for now < horizon {
		expandDue(now)
		if current >= 0 && jobs[current].remaining <= 0 {
			current = -1
		}

		// Reschedule: take the best ready job when idle, or switch only when a
		// strictly higher-priority job is ready (equal priority never preempts).
		best := peekReady()
		switch {
		case current == -1:
			current = popReady()
		case best != -1 && tasks[jobs[best].task].priority < tasks[jobs[current].task].priority:
			heap.Push(ready, &readyItem{
				jobIdx:  current,
				prio:    tasks[jobs[current].task].priority,
				release: jobs[current].release,
				task:    jobs[current].task,
				job:     jobs[current].job,
			})
			current = popReady()
		}

		if current == -1 {
			// Idle: advance directly to the next release or to the horizon.
			rt, ok := nextReleaseTime()
			if !ok {
				now = horizon
				break
			}
			now = rt
			continue
		}

		// Run until the job finishes, the horizon, or the next strictly
		// higher-priority release.
		runEnd := horizon
		if rem := jobs[current].remaining; rem < horizon-now {
			runEnd = now + rem
		}
		curPrio := tasks[jobs[current].task].priority
		for i := 0; i < nt; i++ {
			if remainingReleases(i) == 0 || tasks[i].priority >= curPrio {
				continue
			}
			// Due releases have been expanded, so the cursor release is > now.
			if rt := releaseAt(i, nextJob[i]); rt < runEnd {
				runEnd = rt
			}
		}

		segments = append(segments, periodicSeg{
			task:  jobs[current].task,
			job:   jobs[current].job,
			start: now,
			end:   runEnd,
		})
		delta := runEnd - now
		jobs[current].executed += delta
		jobs[current].remaining -= delta
		now = runEnd
		if jobs[current].remaining <= 0 {
			done := now
			jobs[current].completion = done
			current = -1
		}
	}
	// Releases strictly below horizon that never reached a scheduling point
	// (e.g. lower-priority jobs during a run to the horizon) still exist and
	// must appear in the results. A release exactly at the horizon is not a
	// release within [0, horizon).
	expandDue(horizon - 1)

	// Timeline: omit idle gaps and merge only adjacent intervals of one job.
	timeline := make([]periodicTimelineInterval, 0, len(segments))
	for _, seg := range segments {
		if last := len(timeline) - 1; last >= 0 &&
			timeline[last].TaskID == tasks[seg.task].id &&
			timeline[last].Job == seg.job &&
			timeline[last].End == seg.start {
			timeline[last].End = seg.end
			continue
		}
		timeline = append(timeline, periodicTimelineInterval{
			TaskID: tasks[seg.task].id,
			Job:    seg.job,
			Start:  seg.start,
			End:    seg.end,
		})
	}

	// Jobs expand per task in number order; regrouping preserves that order.
	taskJobs := make([][]periodicJob, nt)
	for i := range taskJobs {
		taskJobs[i] = make([]periodicJob, 0, jobCounts[i])
	}
	for _, j := range jobs {
		taskJobs[j.task] = append(taskJobs[j.task], j)
	}

	results := make([]periodicTaskSimResult, nt)
	allDone := true
	for i, t := range tasks {
		jrs := make([]periodicJobResult, 0, len(taskJobs[i]))
		for _, j := range taskJobs[i] {
			deadline := j.release + t.deadline
			var completion *int64
			var status string
			if j.completion >= 0 {
				c := j.completion
				completion = &c
				if c <= deadline {
					status = "met"
				} else {
					status = "missed"
				}
			} else {
				allDone = false
				if deadline <= horizon {
					status = "missed"
				} else {
					status = "pending"
				}
			}
			jrs = append(jrs, periodicJobResult{
				Job:              j.job,
				Release:          j.release,
				AbsoluteDeadline: deadline,
				Executed:         j.executed,
				Remaining:        j.remaining,
				Completion:       completion,
				DeadlineStatus:   status,
			})
		}
		results[i] = periodicTaskSimResult{ID: t.id, Jobs: jrs}
	}

	status := "horizon"
	if allDone {
		status = "completed"
	}
	return periodicSimResponse{Status: status, Timeline: timeline, Results: results}
}
