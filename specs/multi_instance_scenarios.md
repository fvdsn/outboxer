# Multi-Instance Parallelization Scenarios

Status: implementation test plan (paired with a design proposal).

These scenarios accompany
[`multi_instance_requirements.md`](multi_instance_requirements.md). They focus on
preserving ordering and the at-least-once and transactional guarantees while
multiple instances claim from one outbox table. Several scenarios depend on
design decisions still marked pending in the requirements; they are written to
the intended behavior and should be revisited if those decisions change. Layer
hints: claiming and lock behavior are integration-testable with concurrent
connections against real Postgres; crash and throughput behavior are e2e tests
running multiple real binaries.

## Configuration & Validation

| ID | Scenario | Expected |
| --- | --- | --- |
| MI-CFG-01 | `ordering_key` generated column configured and present. | Startup validation succeeds and the column is usable for claiming. |
| MI-CFG-02 | `ordering_key` column configured but missing from the table. | Startup validation fails. |
| MI-CFG-03 | Single instance, no `ordering_key` column, ordered and unordered events. | Allowed; ordering preserved by id order. |
| MI-CFG-04 | `SKIP LOCKED` claiming with a single instance. | No behavior change versus today; no rows skipped because none are locked elsewhere. |
| MI-CFG-05 | No dedicated multi-instance enable flag. | Claiming is always safe; there is no mode toggle. |

## Unordered Claiming

| ID | Scenario | Expected |
| --- | --- | --- |
| MI-UNORD-01 | Two instances, unordered events. | Disjoint rows are claimed via `SKIP LOCKED`; no row is processed twice within a commit. |
| MI-UNORD-02 | N instances, high-volume unordered. | Throughput scales until DB or broker bound; no double-claim. |
| MI-UNORD-03 | One instance while all eligible rows are locked by others. | It claims nothing, returns empty, and waits without error. |

## Ordered Claiming (Advisory Locks)

| ID | Scenario | Expected |
| --- | --- | --- |
| MI-ORD-01 | Two instances, many distinct keys. | Each key is owned by one instance at a time; per-key id order is preserved; throughput scales across keys. |
| MI-ORD-02 | Two instances contend for one key. | Exactly one wins the advisory lock and processes the key; the other moves on; order is never violated. |
| MI-ORD-03 | One key with events e1 < e2 < e3 by id, single owner. | Published strictly in id order. |
| MI-ORD-04 | Same key string on two different destinations. | The two streams run in parallel and do not serialize against each other. |
| MI-ORD-05 | Two distinct keys hash to the same advisory lock value. | They serialize (parallelism loss only); order and correctness are preserved. |
| MI-ORD-06 | Lock domain is `(target, destination, key)`. | Keys are partitioned by the full tuple, not the key string alone. |
| MI-ORD-07 | An owned key has no remaining events. | The lock is released and the instance moves to another candidate key. |
| MI-ORD-08 | Operator forces global ordering by giving all events one shared key, multiple instances. | That single key has one owner at a time; the stream is effectively single-threaded; this is expected and documented as non-parallelizable. |

## Decoupled Claim / Publish / Delete

| ID | Scenario | Expected |
| --- | --- | --- |
| MI-TX-01 | Claim commits, publish succeeds, delete commits. | The row is gone and the advisory lock is released. |
| MI-TX-02 | Claim commits, publish partially fails (some retryable). | Confirmed events are deleted; unconfirmed remain; the lock is released; the next cycle retries. |
| MI-TX-03 | Delete transaction fails after a confirmed publish. | The events remain (at-least-once) and are re-published on retry; consumer/FIFO dedup covers duplicates. |
| MI-TX-04 | During the publish phase. | No transaction is held open across the network publish; the global xmin horizon is not pinned by the publish. |
| MI-TX-05 | A poison event is encountered in an owned key (DLQ enabled). | It is dead-lettered and deleted in a short transaction; the rest of the key continues in order. |

## Crash & Recovery

| ID | Scenario | Expected |
| --- | --- | --- |
| MI-CRASH-01 | Instance holding a key's session lock is killed mid-publish. | The lock auto-releases on disconnect; another instance reclaims the key from the last committed delete; order and at-least-once hold. |
| MI-CRASH-02 | Instance crashes after publish but before delete. | On reclaim, unconfirmed events are re-published; consumer dedup handles duplicates; order is preserved. |
| MI-CRASH-03 | Instance crashes during the claim transaction before commit. | The transaction rolls back; no rows are claimed; another instance proceeds. |
| MI-CRASH-04 | Network partition between an instance and Postgres. | The session and its advisory locks are released on connection loss; no orphaned locks remain. |

## Ordering Under Failure

| ID | Scenario | Expected |
| --- | --- | --- |
| MI-ORDFAIL-01 | Ordered key e1 has a retryable failure, e2 is valid. | e2 is not sent before e1 succeeds; per-key order is preserved. |
| MI-ORDFAIL-02 | Ordered key e1 is poison, e2 is valid (DLQ enabled). | e1 is dead-lettered, then e2 is sent; order is preserved. |
| MI-ORDFAIL-03 | The lock holder for key K stalls on a slow publish. | No other instance publishes K's later events; order is preserved at the cost of K's latency. |

## Static Ownership Composition

| ID | Scenario | Expected |
| --- | --- | --- |
| MI-OWN-01 | Two instances with disjoint `PUBSUB_DESTINATIONS` / `SQS_DESTINATIONS`. | Each selects only its owned destinations; advisory locks arbitrate ordered keys within them. |
| MI-OWN-02 | Overlapping ownership with ordered keys. | Advisory locks prevent concurrent same-key sends across the overlap. |
| MI-OWN-03 | An instance with no ownership of a key's destination. | It does not select or lock that key. |

## LISTEN/NOTIFY Interaction

| ID | Scenario | Expected |
| --- | --- | --- |
| MI-NTF-01 | All instances `LISTEN` the shared channel; one insert wakes all. | All run the claim query; exactly one wins each row/key; the herd causes no incorrect sends. |
| MI-NTF-02 | Herd wake under a burst of inserts. | Coalescing limits redundant claim queries; there is no correctness impact. |

## Safety Constraint

| ID | Scenario | Expected |
| --- | --- | --- |
| MI-SAFE-01 | Multiple instances, ordered events, `ordering_key` column absent. | Flagged as unsafe where detectable, or documented as unsafe; the risk is out-of-order delivery for keyed events. |
| MI-SAFE-02 | Single instance, ordered events, no `ordering_key` column. | Safe; one reader publishes in id order. |

## End-To-End Smoke Tests

| ID | Scenario | Expected |
| --- | --- | --- |
| MI-E2E-01 | Two real binaries + Postgres + emulators, `ordering_key` configured, ordered keys. | Messages are received per group in id order; no duplicates beyond at-least-once; throughput exceeds a single instance. |
| MI-E2E-02 | Two real binaries, unordered events, one killed mid-run. | The surviving instance drains all events; nothing is lost. |
| MI-E2E-03 | Two real binaries, mixed ordered and unordered, sustained load. | Ordered keys stay in order; unordered events parallelize; combined throughput scales. |
