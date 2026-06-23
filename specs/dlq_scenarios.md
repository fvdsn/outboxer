# Dead Letter Queue Scenarios

Status: implementation test plan.

These scenarios should be implemented before or alongside DLQ code. They focus
on preserving poison events without weakening the existing at-least-once and
transactional guarantees.

## Config Validation

| ID | Scenario | Expected |
| --- | --- | --- |
| DLQ-CFG-01 | `DLQ_TABLE` is empty. | DLQ is disabled; startup does not require a DLQ table. |
| DLQ-CFG-02 | `DLQ_TABLE=outboxer_dead_letters` and the table has `id` and `event`. | Startup validation succeeds. |
| DLQ-CFG-03 | `DLQ_TABLE` points to a missing table. | Startup validation fails. |
| DLQ-CFG-04 | DLQ table exists but has no `event` column. | Startup validation fails. |
| DLQ-CFG-05 | DLQ table exists but `event` is not JSON/JSONB-compatible. | Startup validation fails. |
| DLQ-CFG-06 | `DLQ_TABLE` equals `EVENT_TABLE`. | Startup validation fails. |
| DLQ-CFG-07 | DLQ table has extra columns or indexes. | Startup validation succeeds as long as `id` and `event` are usable. |

## Dead-Letter Payload

| ID | Scenario | Expected |
| --- | --- | --- |
| DLQ-PAYLOAD-01 | A poison event is dead-lettered. | `source_table`, `dead_lettered_at`, `error`, and `original_event` are present. |
| DLQ-PAYLOAD-02 | Original event has configured optional columns. | `original_event` includes the complete selected row, including optional columns present in the event table. |
| DLQ-PAYLOAD-03 | Original event omits optional columns because they are not configured/present. | `original_event` contains only the selected base event columns; no synthetic resolved columns are added. |
| DLQ-PAYLOAD-04 | Original event contains JSON attributes/options. | JSON values are preserved in `original_event`, not stringified except where the source column itself is text. |
| DLQ-PAYLOAD-05 | Event id is numeric, UUID text, or another database type. | `original_event.id` preserves the normalized value Outboxer saw. |
| DLQ-PAYLOAD-06 | Target column is absent or empty and exactly one backend is enabled. | DLQ JSON has `target` set to the resolved enabled backend. |
| DLQ-PAYLOAD-07 | Destination column is absent or empty and the routed backend has a default destination. | DLQ JSON has `destination` set to the resolved default destination. |
| DLQ-PAYLOAD-08 | Original event explicitly sets target and destination. | DLQ JSON `target` and `destination` match the explicit resolved route. |
| DLQ-PAYLOAD-09 | A poison event is dead-lettered. | DLQ JSON does not include a machine-readable `reason` field. |

## Poison Classification

| ID | Scenario | Expected |
| --- | --- | --- |
| DLQ-POISON-01 | Pub/Sub payload exceeds the local maximum. | Event is inserted into DLQ and deleted from the outbox table in the same transaction. |
| DLQ-POISON-02 | Pub/Sub attributes exceed provider limits. | Event is inserted into DLQ and deleted from the outbox table; no provider call is made. |
| DLQ-POISON-03 | Pub/Sub topic is syntactically invalid. | Event is inserted into DLQ and deleted from the outbox table; no provider call is made. |
| DLQ-POISON-04 | Pub/Sub bundled publish returns ambiguous permanent error, then isolated publish proves one event permanent. | Only the isolated permanently bad event is inserted into DLQ; valid or retryable isolated events are handled by their own result. |
| DLQ-POISON-05 | SQS body is empty or contains invalid Unicode. | Event is inserted into DLQ and deleted from the outbox table; no provider call is made. |
| DLQ-POISON-06 | SQS message body plus attributes exceeds 1 MiB. | Event is inserted into DLQ and deleted from the outbox table; no provider call is made. |
| DLQ-POISON-07 | SQS FIFO ordering key is invalid. | Event is inserted into DLQ and deleted from the outbox table; no provider call is made. |
| DLQ-POISON-08 | SQS queue URL is syntactically invalid. | Event is inserted into DLQ and deleted from the outbox table; no provider call is made. |
| DLQ-POISON-09 | SQS batch returns `SenderFault=true` for one entry. | That event is inserted into DLQ; successful entries are deleted normally; retryable failed entries remain. |

## Not Dead-Lettered

| ID | Scenario | Expected |
| --- | --- | --- |
| DLQ-KEEP-01 | Unknown target such as `kafka`. | Event remains in outbox table; no DLQ row is inserted. |
| DLQ-KEEP-02 | Target names a disabled backend. | Event remains in outbox table; no DLQ row is inserted. |
| DLQ-KEEP-03 | Empty target while both backends are enabled. | Event remains in outbox table; no DLQ row is inserted. |
| DLQ-KEEP-04 | Destination missing and no default is configured. | Event remains in outbox table; no DLQ row is inserted. |
| DLQ-KEEP-05 | Destination is syntactically valid but provider says not found. | Event remains in outbox table; no DLQ row is inserted. |
| DLQ-KEEP-06 | Provider returns permission denied or auth failure. | Event remains in outbox table; no DLQ row is inserted. |
| DLQ-KEEP-07 | Provider returns throttling, timeout, or service unavailable. | Event remains in outbox table; no DLQ row is inserted. |
| DLQ-KEEP-08 | Context is canceled during send. | Unconfirmed events remain in outbox table; no DLQ row is inserted. |
| DLQ-KEEP-09 | Pub/Sub ordered key is paused/requires resume. | Event remains in outbox table unless isolated classification proves content poison. |

## Transaction Semantics

| ID | Scenario | Expected |
| --- | --- | --- |
| DLQ-TX-01 | Batch contains one confirmed send and one poison event. | Confirmed event is deleted; poison event is inserted into DLQ and deleted; transaction commits once. |
| DLQ-TX-02 | Batch contains one poison event and one retryable failure. | Poison event is inserted into DLQ and deleted; retryable event remains. |
| DLQ-TX-03 | DLQ insert fails before delete. | Original poison event remains in the outbox table; no delete is committed. |
| DLQ-TX-04 | DLQ insert succeeds but delete fails. | Transaction rolls back; DLQ row is not committed and original event remains. |
| DLQ-TX-05 | DLQ insert and delete succeed but commit fails. | Transaction is not committed; original event may be retried, and any provider-confirmed sends may duplicate on retry. |
| DLQ-TX-06 | DLQ is disabled and a poison event is classified. | Current behavior remains: poison event is deleted after classification, with no DLQ insert. |
| DLQ-TX-07 | Fatal-after-commit sender error occurs with known poison events. | Known poison events are inserted into DLQ and deleted before commit; after commit, processing stops as today. |

## Ordering

| ID | Scenario | Expected |
| --- | --- | --- |
| DLQ-ORDER-01 | Pub/Sub ordered key event 1 is poison and event 2 is valid. | Event 1 is inserted into DLQ and deleted; event 2 may then be sent without violating order. |
| DLQ-ORDER-02 | Pub/Sub ordered key event 1 has retryable failure and event 2 is valid. | Neither event is dead-lettered; event 2 is not sent before event 1 succeeds or is classified poison. |
| DLQ-ORDER-03 | SQS FIFO group event 1 is poison and event 2 is valid. | Event 1 is inserted into DLQ and deleted; event 2 may then be sent. |
| DLQ-ORDER-04 | SQS FIFO group event 1 has retryable failure and event 2 is valid. | Neither event is dead-lettered; event 2 is not sent before event 1 succeeds or is classified poison. |

## Multi-Instance And Ownership

| ID | Scenario | Expected |
| --- | --- | --- |
| DLQ-MULTI-01 | Two Outboxers share one outbox table and one DLQ table with disjoint destination ownership. | Each process dead-letters only poison events from its owned destinations. |
| DLQ-MULTI-02 | Two Outboxers have overlapping destination ownership. | Existing `FOR UPDATE` locking prevents both from dead-lettering the same selected row concurrently. |
| DLQ-MULTI-03 | One process has no ownership of a poison event destination. | That process does not select or dead-letter the event. |

## End-To-End Smoke Tests

| ID | Scenario | Expected |
| --- | --- | --- |
| DLQ-E2E-01 | Local Postgres + Pub/Sub emulator, DLQ enabled, one locally invalid Pub/Sub event. | Outbox table drains the poison event; DLQ table contains one JSON snapshot. |
| DLQ-E2E-02 | Local Postgres + ElasticMQ, DLQ enabled, one locally invalid SQS event. | Outbox table drains the poison event; DLQ table contains one JSON snapshot. |
| DLQ-E2E-03 | Mixed valid and poison events. | Valid messages are received by the provider; poison messages are in DLQ; retryable failures remain in outbox. |
| DLQ-E2E-04 | DLQ table missing at startup. | Outboxer exits with validation error before processing events. |
