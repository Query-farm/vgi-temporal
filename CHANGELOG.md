# Changelog

## 0.1.0 (unreleased)

Initial release: read-only SQL over Temporal (self-hosted and Temporal Cloud) through the gRPC
WorkflowService.

- `main`: `workflow_executions` (visibility-query pushdown, typed custom search attribute
  columns), `count_workflow_executions`, `workflow_history`, `pending_activities`, `namespaces`,
  `schedules`, `task_queues`, `search_attributes`, `batch_operations`, `unified_run_status`.
- `unified`: `runs` and `task_runs` in the shared orchestrator shape.
- `temporal` secret: host, namespace, API key, TLS / mTLS, UI URL.
- SQL tests against a seeded `temporal server start-dev` (pinned, checksummed Temporal CLI 1.10.1).

Targets: vgi-go 0.42 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
