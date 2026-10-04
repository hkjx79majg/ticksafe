package server

import (
	"encoding/json"
	"net/http"
)

const (
	stackAnalyzePath    = "/v1/stacks/analyze"
	maxStackFunctions   = 4096
	maxStackCalls       = 256
	maxStackEntrypoints = 256
	maxFrameBytes       = 1_000_000_000_000
	maxOverheadBytes    = 1_000_000_000_000
	maxStackLimit       = 10_000_000_000_000_000
)

type stackFunctionIn struct {
	ID         string `json:"id"`
	FrameBytes *int64 `json:"frameBytes"`
}

type stackCallIn struct {
	Caller        string `json:"caller"`
	Callee        string `json:"callee"`
	OverheadBytes *int64 `json:"overheadBytes"`
}

type stackEntrypointIn struct {
	Entry      string `json:"entry"`
	StackLimit *int64 `json:"stackLimit"`
}

type stackRequest struct {
	Functions   *[]stackFunctionIn   `json:"functions"`
	Calls       *[]stackCallIn       `json:"calls"`
	Entrypoints *[]stackEntrypointIn `json:"entrypoints"`
}

type stackResult struct {
	Entry         string   `json:"entry"`
	StackLimit    int64    `json:"stackLimit"`
	RequiredBytes *int64   `json:"requiredBytes"`
	Status        string   `json:"status"`
	WorstPath     []string `json:"worstPath"`
	Cycle         []string `json:"cycle"`
}

type stackResponse struct {
	Safe       bool          `json:"safe"`
	Analyzable bool          `json:"analyzable"`
	Results    []stackResult `json:"results"`
}

// stackEdge is one declared call: caller -> callee with a per-call overhead.
type stackEdge struct {
	to       string
	overhead int64
}

// stackGraph is the validated call graph: per-function frame costs plus
// adjacency lists that preserve the input order of calls.
type stackGraph struct {
	frames map[string]int64
	adj    map[string][]stackEdge
}

type stackEntrypoint struct {
	entry      string
	stackLimit int64
}

func handleStackAnalyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAnalyzeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req stackRequest
	if !decodeStrictJSON(w, r, &req) {
		return
	}
	graph, entries, ok := validateStackRequest(&req)
	if !ok {
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(analyzeStacks(graph, entries))
}

// validateStackRequest enforces the stack-analysis schema: 1..4096 functions
// with unique ids and frameBytes in [0, 1e12]; at most 256 calls whose caller
// and callee are both declared and whose overheadBytes lie in [0, 1e12]; and
// 1..256 entrypoints with distinct, declared entries and stackLimit in
// [1, 1e16].
func validateStackRequest(req *stackRequest) (stackGraph, []stackEntrypoint, bool) {
	if req.Functions == nil || len(*req.Functions) == 0 || len(*req.Functions) > maxStackFunctions {
		return stackGraph{}, nil, false
	}
	graph := stackGraph{
		frames: make(map[string]int64, len(*req.Functions)),
		adj:    make(map[string][]stackEdge, len(*req.Functions)),
	}
	for i := range *req.Functions {
		in := (*req.Functions)[i]
		if !validTaskID(in.ID) {
			return stackGraph{}, nil, false
		}
		if _, dup := graph.frames[in.ID]; dup {
			return stackGraph{}, nil, false
		}
		if in.FrameBytes == nil || *in.FrameBytes < 0 || *in.FrameBytes > maxFrameBytes {
			return stackGraph{}, nil, false
		}
		graph.frames[in.ID] = *in.FrameBytes
	}
	if req.Calls != nil {
		if len(*req.Calls) > maxStackCalls {
			return stackGraph{}, nil, false
		}
		for i := range *req.Calls {
			in := (*req.Calls)[i]
			if _, ok := graph.frames[in.Caller]; !ok {
				return stackGraph{}, nil, false
			}
			if _, ok := graph.frames[in.Callee]; !ok {
				return stackGraph{}, nil, false
			}
			if in.OverheadBytes == nil || *in.OverheadBytes < 0 || *in.OverheadBytes > maxOverheadBytes {
				return stackGraph{}, nil, false
			}
			graph.adj[in.Caller] = append(graph.adj[in.Caller], stackEdge{to: in.Callee, overhead: *in.OverheadBytes})
		}
	}
	if req.Entrypoints == nil || len(*req.Entrypoints) == 0 || len(*req.Entrypoints) > maxStackEntrypoints {
		return stackGraph{}, nil, false
	}
	entries := make([]stackEntrypoint, 0, len(*req.Entrypoints))
	seen := make(map[string]struct{}, len(*req.Entrypoints))
	for i := range *req.Entrypoints {
		in := (*req.Entrypoints)[i]
		if _, ok := graph.frames[in.Entry]; !ok {
			return stackGraph{}, nil, false
		}
		if _, dup := seen[in.Entry]; dup {
			return stackGraph{}, nil, false
		}
		seen[in.Entry] = struct{}{}
		if in.StackLimit == nil || *in.StackLimit < 1 || *in.StackLimit > maxStackLimit {
			return stackGraph{}, nil, false
		}
		entries = append(entries, stackEntrypoint{entry: in.Entry, stackLimit: *in.StackLimit})
	}
	return graph, entries, true
}

// findStackCycle runs a depth-first traversal from entry, exploring outgoing
// calls in input order, and returns the cycle closed by the first back edge:
// the current DFS path from the back-edge target down to the node holding the
// edge. It returns nil when no cycle is reachable from entry.
func findStackCycle(entry string, adj map[string][]stackEdge) []string {
	const (
		white = iota // unvisited
		gray         // on the current DFS path
		black        // fully explored
	)
	color := make(map[string]int, len(adj))
	var path []string
	var cycle []string
	var visit func(u string) bool
	visit = func(u string) bool {
		color[u] = gray
		path = append(path, u)
		for _, e := range adj[u] {
			switch color[e.to] {
			case gray:
				start := 0
				for i, id := range path {
					if id == e.to {
						start = i
						break
					}
				}
				cycle = append([]string(nil), path[start:]...)
				return true
			case white:
				if visit(e.to) {
					return true
				}
			}
		}
		path = path[:len(path)-1]
		color[u] = black
		return false
	}
	if visit(entry) {
		return cycle
	}
	return nil
}

// analyzeStacks evaluates every entrypoint in input order. An entry whose
// reachable graph contains a cycle is unbounded; otherwise the worst finite
// path cost (frames plus call overheads) decides within_limit vs exceeded.
// Ties between equal-cost paths resolve level by level toward the call that
// appears first in the calls array.
func analyzeStacks(graph stackGraph, entries []stackEntrypoint) stackResponse {
	// best[u] is the worst finite cost of any path starting at u; pick[u] is
	// the adjacency index of the first call achieving it (-1 for leaves).
	// Both are shared across entries: costs are entry-independent, and solve
	// only runs on nodes whose reachable subgraph is acyclic.
	best := make(map[string]int64, len(graph.frames))
	pick := make(map[string]int, len(graph.frames))
	var solve func(u string) int64
	solve = func(u string) int64 {
		if c, ok := best[u]; ok {
			return c
		}
		// The reachable subgraph is acyclic here, so a path visits each
		// function at most once: the total is bounded by
		// 4096*1e12 frames + 4095*1e12 overheads, far below int64 overflow.
		total := graph.frames[u]
		chosen := -1
		for i, e := range graph.adj[u] {
			cand := graph.frames[u] + e.overhead + solve(e.to)
			if chosen == -1 || cand > total {
				total = cand
				chosen = i
			}
		}
		best[u] = total
		pick[u] = chosen
		return total
	}

	resp := stackResponse{Safe: true, Analyzable: true, Results: make([]stackResult, 0, len(entries))}
	for _, ep := range entries {
		res := stackResult{
			Entry:      ep.entry,
			StackLimit: ep.stackLimit,
			WorstPath:  []string{},
			Cycle:      []string{},
		}
		if cycle := findStackCycle(ep.entry, graph.adj); cycle != nil {
			res.Status = "unbounded"
			res.Cycle = cycle
			resp.Safe = false
			resp.Analyzable = false
		} else {
			required := solve(ep.entry)
			path := []string{}
			u := ep.entry
			for {
				path = append(path, u)
				c := pick[u]
				if c == -1 {
					break
				}
				u = graph.adj[u][c].to
			}
			res.RequiredBytes = &required
			res.WorstPath = path
			if required <= ep.stackLimit {
				res.Status = "within_limit"
			} else {
				res.Status = "exceeded"
				resp.Safe = false
			}
		}
		resp.Results = append(resp.Results, res)
	}
	return resp
}
