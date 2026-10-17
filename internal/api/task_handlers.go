package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/pkg/models"
	"github.com/yourorg/sandboxing/task/queue"
	"github.com/yourorg/sandboxing/task/registry"
	"github.com/yourorg/sandboxing/task/scheduler"
)

// TaskHandler handles all task-layer REST endpoints.
type TaskHandler struct {
	store     *store.DB
	registry  *registry.Registry
	queue     *queue.Queue
	scheduler *scheduler.Scheduler
}

func NewTaskHandler(
	store *store.DB,
	reg *registry.Registry,
	q *queue.Queue,
	sched *scheduler.Scheduler,
) *TaskHandler {
	return &TaskHandler{
		store:     store,
		registry:  reg,
		queue:     q,
		scheduler: sched,
	}
}

// RegisterTaskRoutes mounts all task-layer routes onto the router.
func (h *TaskHandler) RegisterTaskRoutes(r *gin.Engine) {
	v1 := r.Group("/v1")

	// ── Task definitions ──────────────────────────────────────────────────────
	v1.POST("/tasks", h.DeployTask)
	v1.GET("/tasks", h.ListTasks)
	v1.GET("/tasks/:id", h.GetTask)
	v1.GET("/tasks/:id/versions", h.ListTaskVersions)
	v1.DELETE("/tasks/:id", h.DeleteTask)

	// ── Triggering ────────────────────────────────────────────────────────────
	v1.POST("/tasks/:id/trigger", h.TriggerTask)
	v1.POST("/tasks/:id/trigger-and-wait", h.TriggerAndWait)
	v1.POST("/tasks/:id/batch", h.BatchTrigger)

	// ── Runs ──────────────────────────────────────────────────────────────────
	v1.GET("/runs", h.ListRuns)
	v1.GET("/runs/:id", h.GetRun)
	v1.GET("/runs/:id/stream", h.StreamRun) // SSE
	v1.POST("/runs/:id/cancel", h.CancelRun)
	v1.POST("/runs/:id/replay", h.ReplayRun)
	v1.POST("/runs/:id/resume", h.ResumeRun) // human-in-the-loop
	v1.POST("/runs/bulk-cancel", h.BulkCancelRuns)
	v1.POST("/runs/bulk-replay", h.BulkReplayRuns)

	// ── Queues ────────────────────────────────────────────────────────────────
	v1.GET("/queues", h.ListQueues)
	v1.GET("/queues/:name", h.GetQueue)
	v1.POST("/queues/:name/pause", h.PauseQueue)
	v1.POST("/queues/:name/resume", h.ResumeQueue)

	// ── Schedules ─────────────────────────────────────────────────────────────
	v1.POST("/schedules", h.CreateSchedule)
	v1.GET("/schedules", h.ListSchedules)
	v1.PUT("/schedules/:id", h.UpdateSchedule)
	v1.DELETE("/schedules/:id", h.DeleteSchedule)
	v1.POST("/schedules/:id/disable", h.DisableSchedule)
	v1.POST("/schedules/:id/enable", h.EnableSchedule)
}

// ── Task definitions ──────────────────────────────────────────────────────────

func (h *TaskHandler) DeployTask(c *gin.Context) {
	var spec registry.DeploySpec
	if err := c.ShouldBindJSON(&spec); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	def, err := h.registry.Deploy(c.Request.Context(), &spec)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, def)
}

func (h *TaskHandler) ListTasks(c *gin.Context) {
	rows, err := h.store.Raw().QueryContext(c.Request.Context(), `
		SELECT DISTINCT ON (name) id, name, version, template, memory, isolation, created_at
		FROM task_definitions
		ORDER BY name, version DESC`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	var tasks []map[string]interface{}
	for rows.Next() {
		var id, name, template, memory, isolation string
		var version int
		var createdAt time.Time
		rows.Scan(&id, &name, &version, &template, &memory, &isolation, &createdAt)
		tasks = append(tasks, map[string]interface{}{
			"id": id, "name": name, "version": version,
			"template": template, "memory": memory,
			"isolation": isolation, "created_at": createdAt,
		})
	}
	c.JSON(http.StatusOK, gin.H{"tasks": tasks})
}

func (h *TaskHandler) GetTask(c *gin.Context) {
	def, err := h.registry.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
		return
	}
	c.JSON(http.StatusOK, def)
}

func (h *TaskHandler) ListTaskVersions(c *gin.Context) {
	rows, err := h.store.Raw().QueryContext(c.Request.Context(), `
		SELECT id, name, version, created_at FROM task_definitions
		WHERE name=$1 ORDER BY version DESC`, c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	var versions []map[string]interface{}
	for rows.Next() {
		var id, name string
		var version int
		var createdAt time.Time
		rows.Scan(&id, &name, &version, &createdAt)
		versions = append(versions, map[string]interface{}{
			"id": id, "name": name, "version": version, "created_at": createdAt,
		})
	}
	c.JSON(http.StatusOK, gin.H{"versions": versions})
}

func (h *TaskHandler) DeleteTask(c *gin.Context) {
	h.store.Raw().ExecContext(c.Request.Context(),
		`DELETE FROM task_definitions WHERE name=$1`, c.Param("id"))
	c.Status(http.StatusNoContent)
}

// ── Triggering ────────────────────────────────────────────────────────────────

type triggerRequest struct {
	Payload        interface{} `json:"payload"`
	IdempotencyKey string      `json:"idempotency_key"`
	Priority       int         `json:"priority"`
	Delay          string      `json:"delay"`
	TTL            string      `json:"ttl"`
	Tags           []string    `json:"tags"`
}

func (h *TaskHandler) TriggerTask(c *gin.Context) {
	var req triggerRequest
	c.ShouldBindJSON(&req)

	run, err := h.queue.Trigger(c.Request.Context(), c.Param("id"), req.Payload, queue.TriggerOptions{
		IdempotencyKey: req.IdempotencyKey,
		Priority:       req.Priority,
		Delay:          req.Delay,
		Tags:           req.Tags,
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, run)
}

func (h *TaskHandler) TriggerAndWait(c *gin.Context) {
	var req triggerRequest
	c.ShouldBindJSON(&req)

	// use a long context for blocking trigger
	ctx := c.Request.Context()
	run, err := h.queue.TriggerAndWait(ctx, c.Param("id"), req.Payload, queue.TriggerOptions{
		IdempotencyKey: req.IdempotencyKey,
		Priority:       req.Priority,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, run)
}

func (h *TaskHandler) BatchTrigger(c *gin.Context) {
	var req struct {
		Payloads []interface{} `json:"payloads"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	runs, err := h.queue.BatchTrigger(c.Request.Context(), c.Param("id"), req.Payloads, queue.TriggerOptions{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"runs": runs, "count": len(runs)})
}

// ── Runs ──────────────────────────────────────────────────────────────────────

func (h *TaskHandler) ListRuns(c *gin.Context) {
	taskName := c.Query("task")
	status := c.Query("status")

	query := `SELECT id, task_id, status, attempt, created_at, started_at, completed_at
	          FROM task_runs WHERE 1=1`
	args := []interface{}{}
	n := 1

	if taskName != "" {
		query += fmt.Sprintf(` AND task_id IN (SELECT id FROM task_definitions WHERE name=$%d)`, n)
		args = append(args, taskName)
		n++
	}
	if status != "" {
		query += fmt.Sprintf(` AND status=$%d`, n)
		args = append(args, status)
		n++
	}
	query += ` ORDER BY created_at DESC LIMIT 50`

	rows, err := h.store.Raw().QueryContext(c.Request.Context(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	var runs []map[string]interface{}
	for rows.Next() {
		var id, taskID, status string
		var attempt int
		var createdAt time.Time
		var startedAt, completedAt *time.Time
		rows.Scan(&id, &taskID, &status, &attempt, &createdAt, &startedAt, &completedAt)
		runs = append(runs, map[string]interface{}{
			"id": id, "task_id": taskID, "status": status,
			"attempt": attempt, "created_at": createdAt,
			"started_at": startedAt, "completed_at": completedAt,
		})
	}
	c.JSON(http.StatusOK, gin.H{"runs": runs})
}

func (h *TaskHandler) GetRun(c *gin.Context) {
	run, err := h.store.GetTaskRun(c.Request.Context(), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	c.JSON(http.StatusOK, run)
}

// StreamRun streams run status changes as SSE.
// The client receives an event each time the run's status changes.
func (h *TaskHandler) StreamRun(c *gin.Context) {
	runID := c.Param("id")

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "streaming not supported"})
		return
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var lastStatus string
	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-ticker.C:
			run, err := h.store.GetTaskRun(c.Request.Context(), runID)
			if err != nil {
				return
			}

			status := string(run.Status)
			if status != lastStatus {
				lastStatus = status
				data, _ := json.Marshal(run)
				fmt.Fprintf(c.Writer, "data: %s\n\n", data)
				flusher.Flush()
			}

			// stop streaming on terminal states
			switch run.Status {
			case models.TaskRunCompleted, models.TaskRunFailed,
				models.TaskRunCancelled, models.TaskRunTimedOut:
				return
			}
		}
	}
}

func (h *TaskHandler) CancelRun(c *gin.Context) {
	if err := h.queue.Cancel(c.Request.Context(), c.Param("id")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "cancelled"})
}

func (h *TaskHandler) ReplayRun(c *gin.Context) {
	run, err := h.queue.Replay(c.Request.Context(), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, run)
}

// ResumeRun resumes a run that is waiting for a human decision token.
// The human calls this endpoint with their decision payload.
func (h *TaskHandler) ResumeRun(c *gin.Context) {
	runID := c.Param("id")

	var body map[string]interface{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// get the run and its wait token
	run, err := h.store.GetTaskRun(c.Request.Context(), runID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	if run.Status != models.TaskRunWaitToken {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("run is not waiting for a token (status: %s)", run.Status),
		})
		return
	}

	// store the human's decision in the wait token
	data, _ := json.Marshal(body)
	if err := h.store.ResolveWaitToken(c.Request.Context(), run.WaitTokenID, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// mark run as ready to resume — the resume loop will pick it up
	h.store.Raw().ExecContext(c.Request.Context(),
		`UPDATE task_runs SET status='queued', resume_at=NOW() WHERE id=$1`, runID)

	c.JSON(http.StatusOK, gin.H{"status": "resuming", "run_id": runID})
}

func (h *TaskHandler) BulkCancelRuns(c *gin.Context) {
	var req struct {
		Task   string `json:"task"`
		Status string `json:"status"`
	}
	c.ShouldBindJSON(&req)

	h.store.Raw().ExecContext(c.Request.Context(), `
		UPDATE task_runs SET status='cancelled', completed_at=NOW()
		WHERE task_id IN (SELECT id FROM task_definitions WHERE name=$1)
		  AND status=$2`, req.Task, req.Status)

	c.JSON(http.StatusOK, gin.H{"cancelled": true})
}

func (h *TaskHandler) BulkReplayRuns(c *gin.Context) {
	var req struct {
		Task   string `json:"task"`
		Status string `json:"status"`
	}
	c.ShouldBindJSON(&req)

	// get all matching runs and replay each
	rows, _ := h.store.Raw().QueryContext(c.Request.Context(), `
		SELECT id FROM task_runs
		WHERE task_id IN (SELECT id FROM task_definitions WHERE name=$1)
		  AND status=$2 LIMIT 100`, req.Task, req.Status)
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id string
		rows.Scan(&id)
		h.queue.Replay(c.Request.Context(), id)
		count++
	}

	c.JSON(http.StatusOK, gin.H{"replayed": count})
}

// ── Queues ────────────────────────────────────────────────────────────────────

func (h *TaskHandler) ListQueues(c *gin.Context) {
	rows, _ := h.store.Raw().QueryContext(c.Request.Context(),
		`SELECT name, concurrency_limit, paused FROM queues`)
	defer rows.Close()

	var queues []map[string]interface{}
	for rows.Next() {
		var name string
		var limit int
		var paused bool
		rows.Scan(&name, &limit, &paused)
		queues = append(queues, map[string]interface{}{
			"name": name, "concurrency_limit": limit, "paused": paused,
		})
	}
	c.JSON(http.StatusOK, gin.H{"queues": queues})
}

func (h *TaskHandler) GetQueue(c *gin.Context) {
	q, err := h.queue.GetQueue(c.Request.Context(), c.Param("name"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, q)
}

func (h *TaskHandler) PauseQueue(c *gin.Context) {
	h.queue.PauseQueue(c.Request.Context(), c.Param("name"))
	c.JSON(http.StatusOK, gin.H{"paused": true})
}

func (h *TaskHandler) ResumeQueue(c *gin.Context) {
	h.queue.ResumeQueue(c.Request.Context(), c.Param("name"))
	c.JSON(http.StatusOK, gin.H{"paused": false})
}

// ── Schedules ─────────────────────────────────────────────────────────────────

func (h *TaskHandler) CreateSchedule(c *gin.Context) {
	var sched models.TaskSchedule
	if err := c.ShouldBindJSON(&sched); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	sched.ID = fmt.Sprintf("sched-%d", time.Now().UnixNano())
	sched.Enabled = true
	sched.NextRunAt = time.Now().Add(time.Minute) // fire on next minute

	if err := h.store.InsertSchedule(c.Request.Context(), &sched); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, sched)
}

func (h *TaskHandler) ListSchedules(c *gin.Context) {
	rows, _ := h.store.Raw().QueryContext(c.Request.Context(),
		`SELECT id, task_name, cron, timezone, enabled, next_run_at FROM task_schedules`)
	defer rows.Close()

	var schedules []map[string]interface{}
	for rows.Next() {
		var id, taskName, cron, timezone string
		var enabled bool
		var nextRun time.Time
		rows.Scan(&id, &taskName, &cron, &timezone, &enabled, &nextRun)
		schedules = append(schedules, map[string]interface{}{
			"id": id, "task_name": taskName, "cron": cron,
			"timezone": timezone, "enabled": enabled, "next_run_at": nextRun,
		})
	}
	c.JSON(http.StatusOK, gin.H{"schedules": schedules})
}

func (h *TaskHandler) UpdateSchedule(c *gin.Context) {
	var req struct {
		Cron     string `json:"cron"`
		Timezone string `json:"timezone"`
		Enabled  *bool  `json:"enabled"`
	}
	c.ShouldBindJSON(&req)
	h.store.Raw().ExecContext(c.Request.Context(), `
		UPDATE task_schedules SET cron=$1, timezone=$2, enabled=$3 WHERE id=$4`,
		req.Cron, req.Timezone, req.Enabled, c.Param("id"))
	c.JSON(http.StatusOK, gin.H{"updated": true})
}

func (h *TaskHandler) DeleteSchedule(c *gin.Context) {
	h.store.Raw().ExecContext(c.Request.Context(),
		`DELETE FROM task_schedules WHERE id=$1`, c.Param("id"))
	c.Status(http.StatusNoContent)
}

func (h *TaskHandler) DisableSchedule(c *gin.Context) {
	h.store.Raw().ExecContext(c.Request.Context(),
		`UPDATE task_schedules SET enabled=false WHERE id=$1`, c.Param("id"))
	c.JSON(http.StatusOK, gin.H{"enabled": false})
}

func (h *TaskHandler) EnableSchedule(c *gin.Context) {
	h.store.Raw().ExecContext(c.Request.Context(),
		`UPDATE task_schedules SET enabled=true WHERE id=$1`, c.Param("id"))
	c.JSON(http.StatusOK, gin.H{"enabled": true})
}
