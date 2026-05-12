# Part 3: Prompt Preparation

**Chosen PR:** [aio-libs/aiokafka #193 — Add `seek_to_beginning` and `seek_to_end` consumer API](https://github.com/aio-libs/aiokafka/pull/193)

---

## 3.1.1 Repository Context (~260 words)

aiokafka is a Python client library for Apache Kafka built on top of `asyncio`. Kafka is a distributed, partitioned, replicated log used by backend systems to publish and consume streams of events at high throughput. The reference client for Kafka is written in Java, and the most popular synchronous Python client is `kafka-python`. Neither of those plays well with modern Python services that are already written around the `asyncio` event loop — calling a blocking socket from inside a coroutine stalls every other task. aiokafka exists to fill that gap: it speaks the Kafka wire protocol natively over `asyncio`, exposes two main entry points (`AIOKafkaProducer` and `AIOKafkaConsumer`), and re-uses the protocol parsing and error hierarchy from `kafka-python` rather than re-implementing them.

The intended users are Python backend engineers who are already comfortable with `async`/`await` and need to integrate with a Kafka cluster — typically for log/event ingest, change-data-capture, microservice messaging, or stream-processing pre-aggregation. They expect the library to feel similar to the Java/`kafka-python` API while behaving correctly inside an event loop: never blocking the loop, supporting cancellation, and integrating with structured concurrency.

The problem domain is therefore *streaming/messaging*: durable, ordered, at-least-once delivery of records across services, with a consumer group abstraction that lets many processes share the work of reading a partitioned topic. The bulk of the library's complexity is concentrated in the consumer (offset management, group coordination, rebalances, fetch batching) and in the producer (batching, compression, idempotent/transactional sends). The change in PR #193 is purely a consumer-side, public-API addition.

---

## 3.1.2 Pull Request Description (~270 words)

Before this PR, the only way for a user of `AIOKafkaConsumer` to jump to the earliest or latest offset of a partition was to do it manually: call `await consumer.beginning_offsets(partitions)` (or `end_offsets`), receive a mapping of partition → offset, and then call `consumer.seek(tp, offset)` for each partition. That works but it is awkward — `end_offsets` returns an *exclusive* upper bound, the call needs to be issued for every partition, and it requires two round-trips when the consumer is happy to defer the offset lookup until the next fetch.

The reference Java client and the synchronous `kafka-python` client both expose `seek_to_beginning(*partitions)` and `seek_to_end(*partitions)` directly on the consumer. Issue #154 asked for the same API on aiokafka.

This PR adds those two coroutines on `AIOKafkaConsumer`. Each accepts zero or more `TopicPartition` arguments; with no arguments it defaults to the partitions currently assigned to the consumer. The methods validate the input, wait for partition assignment to be stable (via a new shared helper `ensure_partitions_assigned()` on the coordinator), mark each partition as needing an offset reset with `OffsetResetStrategy.EARLIEST` or `LATEST` respectively, and then call into the existing `Fetcher.update_fetch_positions()` to resolve those resets to concrete offsets on the next interaction with the broker.

**Previous behaviour:** users had to combine `beginning_offsets`/`end_offsets` with `seek` to position the consumer at the boundary of a partition.
**New behaviour:** a single `await consumer.seek_to_beginning()` (or `seek_to_end()`) does it — and matches the Java/`kafka-python` API verbatim.

No existing behaviour, default value, or wire-level interaction is altered.

---

## 3.1.3 Acceptance Criteria

✓ **When** the consumer is started and partitions have been assigned, **calling** `await consumer.seek_to_beginning()` (with no arguments) **should** cause the next `getmany()`/`getone()`/`__anext__` to return records starting from the oldest offset still retained by the broker for every assigned partition.

✓ **When** the consumer is started and `await consumer.seek_to_end()` is called with no arguments, the next poll **should** only return records produced *after* the call returns (i.e. no historical records are delivered).

✓ **When** the caller passes one or more specific `TopicPartition` instances (e.g. `await consumer.seek_to_beginning(tp1, tp2)`), only those partitions are repositioned; offsets of other assigned partitions are untouched.

✓ **When** any argument is not a `TopicPartition` (e.g. a tuple, a string, `None`), the method **should** raise `TypeError` immediately and **must not** issue any network request or mutate subscription state.

✓ **When** a passed partition is not currently assigned to the consumer, the method **should** raise `IllegalStateError` (re-exported from `aiokafka.errors`).

✓ **The implementation should handle** the case where the consumer is in the middle of a group rebalance **by** awaiting `ensure_partitions_assigned()` before mutating any per-partition state, so the call deterministically applies to the *post-rebalance* assignment.

✓ **After** the call returns, the subscription state for each affected partition is in `NEED_RESET` with the correct reset strategy (`EARLIEST` for `seek_to_beginning`, `LATEST` for `seek_to_end`) until the fetcher resolves it on the next round-trip.

✓ Both methods are exposed as coroutines and are listed in the public `AIOKafkaConsumer` API documentation (`docs/consumer.rst`).

✓ The PR ships with unit/integration tests that cover: zero-argument call, explicit-partition call, `TypeError` for bad input types, and `IllegalStateError` for unassigned partitions.

---

## 3.1.4 Edge Cases (model should specifically consider)

1. **Empty partition / no committed records.** If a partition has been created but never had records produced to it, `beginning` and `end` offsets are equal (both 0, or both equal to the log-start offset after a retention deletion). The call must still succeed; the next poll should simply block waiting for new records.

2. **Caller passes the same `TopicPartition` twice** (e.g. `seek_to_beginning(tp, tp)`). The method should treat it idempotently — marking the partition for reset more than once must be safe and must not double-issue `ListOffsets` requests.

3. **Manual assignment vs group subscription.** A consumer created with `assign([...])` instead of `subscribe([...])` has no `GroupCoordinator` in the traditional sense; the `ensure_partitions_assigned()` helper must be a no-op in that case rather than blocking forever waiting for a JoinGroup that will never happen.

4. **Call issued before `consumer.start()`** — should raise a clear `IllegalStateError` (or the existing equivalent) rather than `AttributeError` from touching a `None` fetcher.

5. **Rebalance races a call in flight.** If the broker triggers a rebalance after `ensure_partitions_assigned()` returns but before `update_fetch_positions()` completes, the partition list the user passed may no longer be owned by this consumer. The code path should not crash; it should either complete the reset for what is still owned or surface a `CommitFailedError`-style signal — never silently apply a reset to a partition the consumer has just lost.

6. **Log truncation / out-of-range offsets.** Between marking a partition `EARLIEST` and the fetcher actually issuing `ListOffsets`, the broker could have its log-start offset advance due to retention. The fetcher path already handles `OffsetOutOfRangeError`, so the new methods must rely on that path rather than caching a stale offset locally.

7. **Performance: large assignment.** A consumer assigned to hundreds of partitions calling `seek_to_end()` with no arguments must batch the underlying `ListOffsets` request, not issue one request per partition.

---

## 3.1.5 Initial Prompt (~430 words)

```
You are working in the aiokafka repository (https://github.com/aio-libs/aiokafka),
an asyncio-based Apache Kafka client written in Python. Your task is to implement
the two new consumer methods `seek_to_beginning` and `seek_to_end` on
`AIOKafkaConsumer`, matching the API of the Java and kafka-python reference
clients and resolving issue #154.

Scope of work
1. In `aiokafka/consumer.py`, add two public coroutines:
   - `async def seek_to_beginning(self, *partitions)`
   - `async def seek_to_end(self, *partitions)`
   Both must accept zero or more `TopicPartition` instances; with no arguments
   they default to every partition currently in `self._subscription.assigned_partitions()`.

2. Validate inputs eagerly:
   - Raise `TypeError` if any positional argument is not a `TopicPartition`.
   - Raise `IllegalStateError` (re-exported from `aiokafka.errors`) if any passed
     partition is not currently assigned to this consumer.
   No network call or state mutation may happen before validation succeeds.

3. Before mutating per-partition state, `await` a single helper on the coordinator
   so the call is deterministic relative to in-progress rebalances. Add
   `ensure_partitions_assigned()` to `BaseCoordinator` (default: pass / no-op),
   and alias it to the existing `ensure_active_group()` on `GroupCoordinator`.

4. For each target partition, call
   `self._subscription.need_offset_reset(tp, OffsetResetStrategy.EARLIEST)`
   for `seek_to_beginning` and `OffsetResetStrategy.LATEST` for `seek_to_end`,
   then `await self._fetcher.update_fetch_positions(partitions)` exactly once
   for the full set (do not loop per-partition — see edge case 7).

5. Ensure `IllegalStateError` is exported in `aiokafka/errors.py`.

6. Update the consumer's docstring/RST documentation under `docs/` to list the
   two new methods alongside the existing `seek()`.

Acceptance criteria
Implement all eight criteria from section 3.1.3 of the prompt-prep document.
In particular, behaviour must match the existing `seek()` method for everything
that is not explicitly described (cancellation, error propagation, idempotency).

Edge cases
Address all seven edge cases from section 3.1.4. The trickiest ones are:
 - manually-assigned consumers (no `GroupCoordinator` rebalance to wait on);
 - rebalance races between `ensure_partitions_assigned()` and
   `update_fetch_positions()`;
 - large assignments — `update_fetch_positions` must be called once with the
   full partition set so the underlying `ListOffsets` request is batched.

Testing requirements
 - Add tests in `tests/test_consumer.py` (or the existing per-feature test
   module) covering:
     * happy path with no arguments and the consumer assigned to multiple
       partitions, asserting offsets after the call;
     * explicit-partition call repositioning only the chosen partitions;
     * `TypeError` for non-TopicPartition input;
     * `IllegalStateError` for partitions not in the assignment.
 - Run the full pytest suite (`make test` or `pytest tests/`) — it depends on
   a real broker via docker; ensure no existing test regresses.
 - Maintain or improve the diff coverage reported by CI (target: 100% of new
   lines covered, overall coverage non-decreasing).

Do not modify the producer, the wire protocol layer, or the fetcher's offset
resolution logic — the new methods are strictly a thin wrapper around existing
primitives.
```
