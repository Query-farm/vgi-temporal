// Command seeder fills a fresh `temporal server start-dev` with a small, known
// set of workflow executions for the SQL tests:
//
//	order-success-1     completed (one activity)
//	order-failure-1     failed (non-retryable activity failure)
//	order-retry-1       completed after its activity failed twice (attempt 3)
//	order-running-1     running, its activity waiting out an hour-long retry
//	parent-1            completed, started child workflow parent-1-child
//	o'brien\path-1      completed; an id that needs escaping in a List Filter
//	batch-target-1      terminated by a batch operation
//	nightly-report      a paused schedule, triggered once
//
// Custom search attributes (registered by the dev server's --search-attribute
// flags): CustomerTier (Keyword) and OrderCount (Int).
//
// It is test-only code and lives in its own Go module so the worker binary
// does not depend on the Temporal Go SDK.
//
//	go run . -address 127.0.0.1:7233
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	batchpb "go.temporal.io/api/batch/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const taskQueue = "orders"

var (
	tierKey  = temporal.NewSearchAttributeKeyKeyword("CustomerTier")
	countKey = temporal.NewSearchAttributeKeyInt64("OrderCount")
)

// ChargeCard always succeeds.
func ChargeCard(_ context.Context, order string) (string, error) {
	return "charged " + order, nil
}

// Flaky fails on its first two attempts.
func Flaky(ctx context.Context, order string) (string, error) {
	if a := activity.GetInfo(ctx).Attempt; a < 3 {
		return "", fmt.Errorf("card processor unavailable (attempt %d)", a)
	}
	return "charged " + order + " eventually", nil
}

// Decline fails without retry.
func Decline(_ context.Context, order string) (string, error) {
	return "", temporal.NewNonRetryableApplicationError("card declined for "+order, "CardDeclined", nil)
}

// AlwaysFail fails every attempt; its retry policy waits an hour between attempts.
func AlwaysFail(_ context.Context, order string) (string, error) {
	return "", errors.New("warehouse offline for " + order)
}

// OrderWorkflow runs one activity chosen by mode.
func OrderWorkflow(ctx workflow.Context, mode string) (string, error) {
	opts := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, BackoffCoefficient: 1},
	}
	var fn any = ChargeCard
	switch mode {
	case "failure":
		fn = Decline
	case "retry":
		fn = Flaky
	case "running":
		fn = AlwaysFail
		opts.RetryPolicy = &temporal.RetryPolicy{InitialInterval: time.Hour, BackoffCoefficient: 1}
	case "wait":
		workflow.GetSignalChannel(ctx, "never").Receive(ctx, nil)
		return "", nil
	}
	var out string
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, opts), fn, workflow.GetInfo(ctx).WorkflowExecution.ID).Get(ctx, &out)
	return out, err
}

// ParentWorkflow runs one child workflow.
func ParentWorkflow(ctx workflow.Context) (string, error) {
	cctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID + "-child"})
	var out string
	err := workflow.ExecuteChildWorkflow(cctx, OrderWorkflow, "success").Get(ctx, &out)
	return out, err
}

func main() {
	addr := flag.String("address", "127.0.0.1:7233", "Temporal frontend host:port")
	ns := flag.String("namespace", "default", "namespace to seed")
	flag.Parse()
	log.SetOutput(os.Stderr)
	if err := run(*addr, *ns); err != nil {
		log.Fatalf("seeder: %v", err)
	}
}

func run(addr, ns string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	// Expected activity failures are logged at ERROR by the SDK; keep the fixture quiet.
	quiet := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 4}))
	c, err := client.Dial(client.Options{HostPort: addr, Namespace: ns, Logger: tlog.NewStructuredLogger(quiet)})
	if err != nil {
		return err
	}
	defer c.Close()

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(OrderWorkflow)
	w.RegisterWorkflow(ParentWorkflow)
	for _, a := range []any{ChargeCard, Flaky, Decline, AlwaysFail} {
		w.RegisterActivity(a)
	}
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()

	start := func(id string, wf any, tier string, count int64, args ...any) (client.WorkflowRun, error) {
		opts := client.StartWorkflowOptions{ID: id, TaskQueue: taskQueue}
		if tier != "" {
			opts.TypedSearchAttributes = temporal.NewSearchAttributes(tierKey.ValueSet(tier), countKey.ValueSet(count))
		}
		return c.ExecuteWorkflow(ctx, opts, wf, args...)
	}
	type spec struct {
		id, mode, tier string
		count          int64
		wantErr        bool
	}
	for _, s := range []spec{
		{"order-success-1", "success", "gold", 3, false},
		{"order-failure-1", "failure", "silver", 1, true},
		{"order-retry-1", "retry", "gold", 2, false},
		{`o'brien\path-1`, "success", "", 0, false},
	} {
		run, err := start(s.id, OrderWorkflow, s.tier, s.count, s.mode)
		if err != nil {
			return fmt.Errorf("start %s: %w", s.id, err)
		}
		if err := run.Get(ctx, nil); (err != nil) != s.wantErr {
			return fmt.Errorf("%s: unexpected result %v", s.id, err)
		}
	}
	parent, err := start("parent-1", ParentWorkflow, "", 0)
	if err != nil {
		return err
	}
	if err := parent.Get(ctx, nil); err != nil {
		return fmt.Errorf("parent-1: %w", err)
	}

	// A running workflow whose activity has failed once and waits an hour to retry.
	if _, err := start("order-running-1", OrderWorkflow, "bronze", 5, "running"); err != nil {
		return err
	}
	if err := waitFor(ctx, func() (bool, error) {
		d, err := c.DescribeWorkflowExecution(ctx, "order-running-1", "")
		if err != nil {
			return false, err
		}
		p := d.GetPendingActivities()
		return len(p) == 1 && p[0].GetAttempt() >= 2 && p[0].GetLastFailure() != nil, nil
	}); err != nil {
		return fmt.Errorf("order-running-1 never retried: %w", err)
	}

	// A batch operation that terminates batch-target-1.
	if _, err := start("batch-target-1", OrderWorkflow, "", 0, "wait"); err != nil {
		return err
	}
	if err := waitVisible(ctx, c, ns, "WorkflowId = 'batch-target-1' AND ExecutionStatus = 'Running'"); err != nil {
		return err
	}
	if _, err := c.WorkflowService().StartBatchOperation(ctx, &workflowservice.StartBatchOperationRequest{
		Namespace: ns, JobId: "terminate-batch-targets", Reason: "fixture cleanup",
		VisibilityQuery: "WorkflowId = 'batch-target-1'",
		Operation: &workflowservice.StartBatchOperationRequest_TerminationOperation{
			TerminationOperation: &batchpb.BatchOperationTermination{Identity: "seeder"}},
	}); err != nil {
		return fmt.Errorf("start batch: %w", err)
	}
	if err := waitFor(ctx, func() (bool, error) {
		d, err := c.WorkflowService().DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{Namespace: ns, JobId: "terminate-batch-targets"})
		if err != nil {
			return false, err
		}
		return d.GetState() == enumspb.BATCH_OPERATION_STATE_COMPLETED, nil
	}); err != nil {
		return fmt.Errorf("batch never completed: %w", err)
	}

	// A schedule, triggered once and then paused so it never fires on its own.
	h, err := c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID:   "nightly-report",
		Spec: client.ScheduleSpec{CronExpressions: []string{"0 3 * * *"}, Intervals: []client.ScheduleIntervalSpec{{Every: 6 * time.Hour}}},
		Action: &client.ScheduleWorkflowAction{ID: "nightly-report-run", Workflow: OrderWorkflow,
			Args: []any{"success"}, TaskQueue: taskQueue},
		Note: "fixture schedule",
	})
	if err != nil {
		return fmt.Errorf("create schedule: %w", err)
	}
	if err := h.Trigger(ctx, client.ScheduleTriggerOptions{}); err != nil {
		return err
	}
	if err := waitFor(ctx, func() (bool, error) {
		d, err := h.Describe(ctx)
		if err != nil {
			return false, err
		}
		ra := d.Info.RecentActions
		return len(ra) == 1 && ra[0].StartWorkflowResult != nil, nil
	}); err != nil {
		return fmt.Errorf("schedule never ran: %w", err)
	}
	if err := waitVisible(ctx, c, ns, "TemporalScheduledById = 'nightly-report' AND ExecutionStatus = 'Completed'"); err != nil {
		return err
	}
	if err := h.Pause(ctx, client.SchedulePauseOptions{Note: "paused by the fixture"}); err != nil {
		return err
	}

	// Visibility is eventually consistent: wait until every run is listed in its final state.
	for _, q := range []string{
		"ExecutionStatus = 'Completed' AND TaskQueue = 'orders'",
		"ExecutionStatus = 'Terminated'",
		"ExecutionStatus = 'Failed'",
	} {
		if err := waitVisible(ctx, c, ns, q); err != nil {
			return err
		}
	}
	if err := waitFor(ctx, func() (bool, error) {
		r, err := c.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: ns, Query: "ExecutionStatus = 'Completed'"})
		return err == nil && r.GetCount() == 6, err
	}); err != nil {
		return fmt.Errorf("visibility never showed 6 completed runs: %w", err)
	}
	if err := waitFor(ctx, func() (bool, error) {
		r, err := c.ScheduleClient().List(ctx, client.ScheduleListOptions{})
		if err != nil {
			return false, err
		}
		for r.HasNext() {
			e, err := r.Next()
			if err != nil {
				return false, err
			}
			if e.ID == "nightly-report" && e.Paused {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return fmt.Errorf("schedule never listed as paused: %w", err)
	}
	log.Printf("seeded namespace %s at %s", ns, addr)
	return nil
}

func waitVisible(ctx context.Context, c client.Client, ns, query string) error {
	err := waitFor(ctx, func() (bool, error) {
		r, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Namespace: ns, Query: query})
		return err == nil && len(r.GetExecutions()) > 0, err
	})
	if err != nil {
		return fmt.Errorf("visibility query %q: %w", query, err)
	}
	return nil
}

func waitFor(ctx context.Context, cond func() (bool, error)) error {
	var last error
	for i := 0; i < 600; i++ {
		ok, err := cond()
		if ok {
			return nil
		}
		last = err
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), last)
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out: %v", last)
}
