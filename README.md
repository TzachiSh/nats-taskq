# Task queue assignment

Build a small distributed task queue on NATS. This is the whole spec; you choose the design.

## Requirements

- A producer seeds the queue with 1,000 tasks (task id + a small payload).
- Workers process tasks. Processing a task takes 500ms (simulate it) and fails 10% of the time (simulate that too).
- A failed task is retried until it succeeds. No task may be lost, and no task may be processed to completion twice.
- Running three workers at once splits the load between them.
- The system survives a worker being killed mid-process (Ctrl+C): its in-flight tasks still complete somewhere.
- A worker must not grab unbounded work: it processes at most 5 tasks at a time and takes more only when it has capacity.
- `docker compose up` brings up everything needed. Node.js for the code.
- A way to see progress and verify at the end that exactly 1,000 tasks completed.

## Notes

Keep it pragmatic. We care about the choices you make under real-world constraints, not about framework ceremony.
