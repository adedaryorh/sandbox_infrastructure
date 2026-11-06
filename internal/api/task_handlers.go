package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/pkg/models"
	"github.com/yourorg/sandboxing/task/queue"
	"github.com/yourorg/sandboxing/task/registry"
	"github.com/yourorg/sandboxing/task/runner"
	"github.com/yourorg/sandboxing/task/scheduler"
	"time"
)

type TaskHandler struct {
	runner    *runner.Runner
	store     *store.DB
	registry  *registry.Registry
	queue     *queue.Queue
	scheduler *scheduler.Scheduler
}

func NewTaskHandler(s *store.DB, r *registry.Registry, q *queue.Queue, sc *scheduler.Scheduler) *TaskHandler {
	return &TaskHandler{store: s, registry: r, queue: q, scheduler: sc}
}
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
	v1.POST("/runs/:id/resume", h.ResumeRun)
	v1.POST("/runs/:id/wait", h.WaitRun) // human-in-the-loop
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

func (h *TaskHandler) DeployTask(c *gin.Context) {
	var spec registry.DeploySpec
	if c.ShouldBindJSON(&spec) != nil || spec.Name == "" || len(spec.Name) > 120 || len(spec.Handler) == 0 || len(spec.Handler) > 1<<20 {
		c.JSON(400, gin.H{"error": "name and handler (up to 1 MiB) are required"})
		return
	}
	def, err := h.registry.Deploy(c.Request.Context(), &spec)
	if err != nil {
		apiError(c, err)
		return
	}
	c.JSON(201, def)
}
func (h *TaskHandler) ListTasks(c *gin.Context) {
	ctx, t := tenantContext(c)
	rows, err := h.store.Raw().QueryContext(ctx, `select distinct on(name) name from task_definitions where tenant_id=$1 order by name,version desc`, t)
	if err != nil {
		apiError(c, err)
		return
	}
	var names []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			rows.Close()
			apiError(c, err)
			return
		}
		names = append(names, name)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		apiError(c, err)
		return
	}
	items := []*models.TaskDefinition{}
	for _, name := range names {
		v, e := h.registry.Get(ctx, name)
		if e != nil {
			apiError(c, e)
			return
		}
		items = append(items, v)
	}
	c.JSON(200, gin.H{"tasks": items})
}
func (h *TaskHandler) GetTask(c *gin.Context) {
	v, err := h.registry.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		apiError(c, err)
		return
	}
	c.JSON(200, v)
}
func (h *TaskHandler) ListTaskVersions(c *gin.Context) {
	ctx, t := tenantContext(c)
	rows, err := h.store.Raw().QueryContext(ctx, `select id,name,version,created_at from task_definitions where name=$1 and tenant_id=$2 order by version desc`, c.Param("id"), t)
	if err != nil {
		apiError(c, err)
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var id, name string
		var version int
		var at time.Time
		if err = rows.Scan(&id, &name, &version, &at); err != nil {
			apiError(c, err)
			return
		}
		items = append(items, gin.H{"id": id, "name": name, "version": version, "created_at": at})
	}
	if rows.Err() != nil {
		apiError(c, rows.Err())
		return
	}
	c.JSON(200, gin.H{"versions": items})
}
func (h *TaskHandler) DeleteTask(c *gin.Context) {
	ctx, t := tenantContext(c)
	if _, e := h.registry.Get(ctx, c.Param("id")); e != nil {
		apiError(c, e)
		return
	}
	result, err := h.store.Raw().ExecContext(ctx, `delete from task_definitions d where d.name=$1 and d.tenant_id=$2 and not exists(select 1 from task_runs r where r.task_id=d.id)`, c.Param("id"), t)
	if err != nil {
		apiError(c, err)
		return
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		c.JSON(409, gin.H{"error": "task versions with run history cannot be deleted"})
		return
	}
	c.Status(204)
}

type triggerRequest struct {
	Payload        any      `json:"payload"`
	IdempotencyKey string   `json:"idempotency_key"`
	Priority       int      `json:"priority"`
	Delay          string   `json:"delay"`
	Tags           []string `json:"tags"`
}

func (h *TaskHandler) trigger(c *gin.Context, wait bool) {
	var req triggerRequest
	if c.ShouldBindJSON(&req) != nil {
		c.JSON(400, gin.H{"error": "invalid JSON"})
		return
	}
	opts := queue.TriggerOptions{IdempotencyKey: req.IdempotencyKey, Priority: req.Priority, Delay: req.Delay, Tags: req.Tags}
	var run *models.TaskRun
	var err error
	if wait {
		run, err = h.queue.TriggerAndWait(c.Request.Context(), c.Param("id"), req.Payload, opts)
	} else {
		run, err = h.queue.Trigger(c.Request.Context(), c.Param("id"), req.Payload, opts)
	}
	if err != nil {
		apiError(c, err)
		return
	}
	code := 202
	if wait {
		code = 200
	}
	c.JSON(code, run)
}
func (h *TaskHandler) TriggerTask(c *gin.Context)    { h.trigger(c, false) }
func (h *TaskHandler) TriggerAndWait(c *gin.Context) { h.trigger(c, true) }
func (h *TaskHandler) BatchTrigger(c *gin.Context) {
	var input struct {
		Payloads []any `json:"payloads"`
	}
	if c.ShouldBindJSON(&input) != nil || len(input.Payloads) == 0 || len(input.Payloads) > 100 {
		c.JSON(400, gin.H{"error": "1-100 payloads required"})
		return
	}
	runs, err := h.queue.BatchTrigger(c.Request.Context(), c.Param("id"), input.Payloads, queue.TriggerOptions{})
	if err != nil {
		apiError(c, err)
		return
	}
	c.JSON(202, gin.H{"runs": runs})
}
func (h *TaskHandler) ListRuns(c *gin.Context) {
	ctx, t := tenantContext(c)
	rows, err := h.store.Raw().QueryContext(ctx, `select r.id from task_runs r join task_definitions d on d.id=r.task_id where r.tenant_id=$1 and ($2='' or d.name=$2) and ($3='' or r.status=$3) order by r.created_at desc limit 100`, t, c.Query("task"), c.Query("status"))
	if err != nil {
		apiError(c, err)
		return
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			apiError(c, err)
			return
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		apiError(c, err)
		return
	}
	items := []*models.TaskRun{}
	for _, id := range ids {
		v, e := h.store.GetTaskRun(ctx, id)
		if e != nil {
			apiError(c, e)
			return
		}
		items = append(items, v)
	}
	c.JSON(200, gin.H{"runs": items})
}
func (h *TaskHandler) GetRun(c *gin.Context) {
	v, e := h.store.GetTaskRun(c.Request.Context(), c.Param("id"))
	if e != nil {
		apiError(c, e)
		return
	}
	c.JSON(200, v)
}
func (h *TaskHandler) StreamRun(c *gin.Context) {
	if _, e := h.store.GetTaskRun(c.Request.Context(), c.Param("id")); e != nil {
		apiError(c, e)
		return
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	last := ""
	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-tick.C:
			v, e := h.store.GetTaskRun(c.Request.Context(), c.Param("id"))
			if e != nil {
				return
			}
			if string(v.Status) != last {
				last = string(v.Status)
				data, _ := json.Marshal(v)
				fmt.Fprintf(c.Writer, "data: %s\n\n", data)
				c.Writer.Flush()
			}
			switch v.Status {
			case models.TaskRunCompleted, models.TaskRunFailed, models.TaskRunCancelled, models.TaskRunTimedOut:
				return
			}
		}
	}
}
func (h *TaskHandler) CancelRun(c *gin.Context) {
	if _, e := h.store.GetTaskRun(c.Request.Context(), c.Param("id")); e != nil {
		apiError(c, e)
		return
	}
	if e := h.queue.Cancel(c.Request.Context(), c.Param("id")); e != nil {
		apiError(c, e)
		return
	}
	c.JSON(200, gin.H{"status": "cancelled"})
}
func (h *TaskHandler) ReplayRun(c *gin.Context) {
	v, e := h.queue.Replay(c.Request.Context(), c.Param("id"))
	if e != nil {
		apiError(c, e)
		return
	}
	c.JSON(202, v)
}
func (h *TaskHandler) ResumeRun(c *gin.Context) {
	var body json.RawMessage
	if c.ShouldBindJSON(&body) != nil {
		c.JSON(400, gin.H{"error": "valid JSON decision required"})
		return
	}
	if err := h.store.ResolveRunToken(c.Request.Context(), c.Param("id"), body); err != nil {
		apiError(c, err)
		return
	}
	c.JSON(202, gin.H{"status": "resume_requested"})
}

func (h *TaskHandler) bulk(c *gin.Context, replay bool) {
	ctx, t := tenantContext(c)
	var req struct {
		Task   string `json:"task"`
		Status string `json:"status"`
	}
	if c.ShouldBindJSON(&req) != nil || req.Task == "" {
		c.JSON(400, gin.H{"error": "task and status required"})
		return
	}
	rows, e := h.store.Raw().QueryContext(ctx, `select r.id from task_runs r join task_definitions d on d.id=r.task_id where r.tenant_id=$1 and d.name=$2 and r.status=$3 limit 100`, t, req.Task, req.Status)
	if e != nil {
		apiError(c, e)
		return
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			apiError(c, e)
			return
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		apiError(c, e)
		return
	}
	for _, id := range ids {
		if replay {
			_, e = h.queue.Replay(ctx, id)
		} else {
			e = h.queue.Cancel(ctx, id)
		}
		if e != nil {
			apiError(c, e)
			return
		}
	}
	c.JSON(200, gin.H{"affected": len(ids)})
}
func (h *TaskHandler) BulkCancelRuns(c *gin.Context) { h.bulk(c, false) }
func (h *TaskHandler) BulkReplayRuns(c *gin.Context) { h.bulk(c, true) }
func (h *TaskHandler) ListQueues(c *gin.Context) {
	ctx, t := tenantContext(c)
	rows, e := h.store.Raw().QueryContext(ctx, `select name from queues where tenant_id=$1 union select queue_name from task_definitions where tenant_id=$1 order by 1`, t)
	if e != nil {
		apiError(c, e)
		return
	}
	names := []string{}
	for rows.Next() {
		var name string
		if e = rows.Scan(&name); e != nil {
			rows.Close()
			apiError(c, e)
			return
		}
		names = append(names, name)
	}
	rows.Close()
	items := []*models.Queue{}
	for _, name := range names {
		v, e := h.queue.GetQueue(ctx, name)
		if e != nil {
			apiError(c, e)
			return
		}
		items = append(items, v)
	}
	c.JSON(200, gin.H{"queues": items})
}
func (h *TaskHandler) GetQueue(c *gin.Context) {
	v, e := h.queue.GetQueue(c.Request.Context(), c.Param("name"))
	if e != nil {
		apiError(c, e)
		return
	}
	c.JSON(200, v)
}
func (h *TaskHandler) PauseQueue(c *gin.Context) {
	if e := h.queue.PauseQueue(c.Request.Context(), c.Param("name")); e != nil {
		apiError(c, e)
		return
	}
	c.JSON(200, gin.H{"paused": true})
}
func (h *TaskHandler) ResumeQueue(c *gin.Context) {
	if e := h.queue.ResumeQueue(c.Request.Context(), c.Param("name")); e != nil {
		apiError(c, e)
		return
	}
	c.JSON(200, gin.H{"paused": false})
}
func (h *TaskHandler) CreateSchedule(c *gin.Context) {
	var v models.TaskSchedule
	if c.ShouldBindJSON(&v) != nil || v.TaskName == "" {
		c.JSON(400, gin.H{"error": "invalid schedule"})
		return
	}
	if _, e := h.registry.Get(c.Request.Context(), v.TaskName); e != nil {
		apiError(c, e)
		return
	}
	next, e := scheduler.Next(v.Cron, v.Timezone, time.Now())
	if e != nil {
		c.JSON(400, gin.H{"error": e.Error()})
		return
	}
	v.ID = models.NewID("schedule")
	v.NextRunAt = next
	v.Enabled = true
	if e = h.store.InsertSchedule(c.Request.Context(), &v); e != nil {
		apiError(c, e)
		return
	}
	c.JSON(201, v)
}
func (h *TaskHandler) ListSchedules(c *gin.Context) {
	ctx, t := tenantContext(c)
	rows, e := h.store.Raw().QueryContext(ctx, `select id,task_name,cron,timezone,enabled,next_run_at from task_schedules where tenant_id=$1 order by created_at desc limit 100`, t)
	if e != nil {
		apiError(c, e)
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var id, name, cron, zone string
		var enabled bool
		var at time.Time
		if e = rows.Scan(&id, &name, &cron, &zone, &enabled, &at); e != nil {
			apiError(c, e)
			return
		}
		items = append(items, gin.H{"id": id, "task_name": name, "cron": cron, "timezone": zone, "enabled": enabled, "next_run_at": at})
	}
	if rows.Err() != nil {
		apiError(c, rows.Err())
		return
	}
	c.JSON(200, gin.H{"schedules": items})
}
func (h *TaskHandler) scheduleExists(c *gin.Context) bool {
	var one int
	e := h.store.Raw().QueryRowContext(c.Request.Context(), "select 1 from task_schedules where id=$1 and tenant_id=$2", c.Param("id"), c.GetString("tenant_id")).Scan(&one)
	if e == sql.ErrNoRows {
		apiError(c, store.ErrNotFound)
		return false
	}
	if e != nil {
		apiError(c, e)
		return false
	}
	return true
}
func (h *TaskHandler) UpdateSchedule(c *gin.Context) {
	if !h.scheduleExists(c) {
		return
	}
	var req struct {
		Cron     string `json:"cron"`
		Timezone string `json:"timezone"`
		Enabled  bool   `json:"enabled"`
	}
	if c.ShouldBindJSON(&req) != nil {
		c.JSON(400, gin.H{"error": "invalid schedule"})
		return
	}
	next, e := scheduler.Next(req.Cron, req.Timezone, time.Now())
	if e != nil {
		c.JSON(400, gin.H{"error": e.Error()})
		return
	}
	_, e = h.store.Raw().ExecContext(c.Request.Context(), "update task_schedules set cron=$1,timezone=$2,enabled=$3,next_run_at=$4 where id=$5 and tenant_id=$6", req.Cron, req.Timezone, req.Enabled, next, c.Param("id"), c.GetString("tenant_id"))
	if e != nil {
		apiError(c, e)
		return
	}
	c.JSON(200, gin.H{"updated": true})
}
func (h *TaskHandler) DeleteSchedule(c *gin.Context) {
	if !h.scheduleExists(c) {
		return
	}
	_, e := h.store.Raw().ExecContext(c.Request.Context(), "delete from task_schedules where id=$1 and tenant_id=$2", c.Param("id"), c.GetString("tenant_id"))
	if e != nil {
		apiError(c, e)
		return
	}
	c.Status(204)
}
func (h *TaskHandler) toggle(c *gin.Context, enabled bool) {
	if !h.scheduleExists(c) {
		return
	}
	var next time.Time
	if enabled {
		var cron, zone string
		if e := h.store.Raw().QueryRowContext(c.Request.Context(), "select cron,timezone from task_schedules where id=$1 and tenant_id=$2", c.Param("id"), c.GetString("tenant_id")).Scan(&cron, &zone); e != nil {
			apiError(c, e)
			return
		}
		var e error
		next, e = scheduler.Next(cron, zone, time.Now())
		if e != nil {
			apiError(c, e)
			return
		}
	}
	_, e := h.store.Raw().ExecContext(c.Request.Context(), "update task_schedules set enabled=$1,next_run_at=case when $1 then $2 else next_run_at end where id=$3 and tenant_id=$4", enabled, next, c.Param("id"), c.GetString("tenant_id"))
	if e != nil {
		apiError(c, e)
		return
	}
	c.JSON(200, gin.H{"enabled": enabled})
}
func (h *TaskHandler) DisableSchedule(c *gin.Context) { h.toggle(c, false) }
func (h *TaskHandler) EnableSchedule(c *gin.Context)  { h.toggle(c, true) }

func (h *TaskHandler) SetRunner(r *runner.Runner) { h.runner = r }
func (h *TaskHandler) WaitRun(c *gin.Context) {
	var req struct {
		Duration    string `json:"duration"`
		TokenTTL    string `json:"token_ttl"`
		Description string `json:"description"`
	}
	if c.ShouldBindJSON(&req) != nil || h.runner == nil {
		c.JSON(400, gin.H{"error": "invalid wait request"})
		return
	}
	var duration, ttl time.Duration
	var e error
	if req.TokenTTL != "" {
		ttl, e = time.ParseDuration(req.TokenTTL)
	} else {
		duration, e = time.ParseDuration(req.Duration)
	}
	if e != nil || duration < 0 || ttl < 0 || duration+ttl <= 0 || duration+ttl > 24*time.Hour || (req.Duration != "" && req.TokenTTL != "") {
		c.JSON(400, gin.H{"error": "provide one positive duration or token_ttl up to 24h"})
		return
	}
	if e = h.runner.Wait(c.Request.Context(), c.Param("id"), duration, ttl, req.Description); e != nil {
		c.JSON(409, gin.H{"error": "run could not be checkpointed; no successful wait is claimed"})
		return
	}
	c.JSON(202, gin.H{"status": "waiting"})
}
