# Part 2: Pull Request Analysis

**Repository chosen:** [aio-libs/aiokafka](https://github.com/aio-libs/aiokafka)

I reviewed the ten listed PRs against the codebase. Most of them were either very large protocol-level rewrites (e.g. #25 producer batches, #1006 transactional producer, #232 SASL plaintext support) or required deep familiarity with the Kafka rebalance protocol (#115 compacted-topic offset gaps, #143 metadata listener for group-less consumers, #217 / #237 group-coordinator fixes). I chose two PRs whose scope is small, whose intent is documented, and whose change can be reasoned about end-to-end:

1. **PR #193 — Add `seek_to_beginning` / `seek_to_end` consumer API** ([link](https://github.com/aio-libs/aiokafka/pull/193))
2. **PR #196 — Separate socket groups for the client** ([link](https://github.com/aio-libs/aiokafka/pull/196))

---

## PR #193 — Add `seek_to_beginning` and `seek_to_end` consumer API

### PR Summary (~130 words)
`AIOKafkaConsumer` already exposed a `seek(tp, offset)` method that lets the application jump to a specific absolute offset in a partition. There was, however, no convenience for the two most common cases: *"start from the oldest record still retained in the partition"* and *"jump to the newest record so I only see what arrives from now on"*. Users had to fetch the beginning/end offsets themselves with `beginning_offsets()`/`end_offsets()` and feed them back into `seek()`, which is verbose and easy to get wrong (e.g. forgetting that `end_offsets` is exclusive). The Java and synchronous `kafka-python` clients already expose `seek_to_beginning` / `seek_to_end`, so this PR closes the API-parity gap and resolves issue #154. It is a small, additive change — no existing behaviour is modified.

### Technical Changes (files / components)
- [`aiokafka/consumer.py`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/consumer.py) — two new coroutines, `seek_to_beginning(*partitions)` and `seek_to_end(*partitions)`, added to the public consumer surface.
- [`aiokafka/group_coordinator.py`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/group_coordinator.py) — a new method `ensure_partitions_assigned()` on `BaseCoordinator`, plus an alias on `GroupCoordinator` that points at the existing `ensure_active_group()`. This gives the new consumer methods a single, polymorphic way to wait until the partition assignment is settled, regardless of whether the consumer is using a group or manual assignment.
- [`aiokafka/errors.py`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/errors.py) — exports `IllegalStateError` so it can be raised when callers pass a partition that has not been assigned to the consumer.
- Test additions covering both happy-path and `TypeError` validation (e.g. passing a non-`TopicPartition`).

### Implementation Approach (~190 words)
The two methods follow the same three-step recipe used by the rest of the consumer:

1. **Validate input.** If the caller passes positional partitions, each one must be a `TopicPartition` instance — anything else raises `TypeError`. If no partitions are passed the methods default to *all currently assigned partitions* (`self._subscription.assigned_partitions()`), which mirrors the Java client behaviour.
2. **Make sure the assignment exists.** Both methods `await self._coordinator.ensure_partitions_assigned()`. For a group-coordinated consumer this is just an alias for the existing `ensure_active_group()` JoinGroup/SyncGroup flow; for a manually assigned consumer it is a no-op. Centralising this call avoids racing with a rebalance that hasn't completed yet.
3. **Mark the partitions for reset.** Instead of querying the broker synchronously, the methods call `self._subscription.need_offset_reset(tp, OffsetResetStrategy.EARLIEST | LATEST)` for each partition and then `await self._fetcher.update_fetch_positions(partitions)`. The `Fetcher` already knows how to resolve a partition flagged for reset by issuing a `ListOffsets` request, so the new APIs piggy-back on that machinery rather than duplicating offset-lookup code.

The net effect is ~33 added lines and zero deletions.

### Potential Impact (~85 words)
The public surface of `AIOKafkaConsumer` gains two methods, which makes API documentation and any user code that wraps the consumer slightly broader. Internally, `BaseCoordinator` and `GroupCoordinator` get a new method/alias, so any subclass that someone has built outside the project also needs to be aware of `ensure_partitions_assigned`. Because the new methods only delegate to existing primitives (`SubscriptionState`, `Fetcher.update_fetch_positions`), the blast radius is small: no change to the producer, the network layer, or the rebalance state machine.

---

## PR #196 — Separate socket groups for client connections

### PR Summary (~140 words)
A Kafka client opens a long-lived TCP connection per broker. In aiokafka prior to this PR, *every* request to a given broker — fetch polls, metadata refreshes, offset commits, heartbeats — was serialised over the same socket. That is normally fine because Kafka uses correlation IDs to multiplex responses. The problem is that the consumer's fetch poll can intentionally block for up to `fetch_max_wait_ms` (commonly 500 ms) waiting for new records, and while that long-poll is in flight on the wire, an offset commit or heartbeat sent through the same connection cannot be flushed until the poll returns. The result is *slow commits* (issue #128) and, in the worst case, missed heartbeats. This PR fixes #128 / #137 by letting the client open multiple connections to the same broker, partitioned by *purpose* (called "socket groups"), so coordination traffic uses its own socket and is not held up by fetcher polls.

### Technical Changes (files / components)
- [`aiokafka/client.py`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/client.py) — adds a "socket group" dimension to the per-node connection map. Where the client previously kept a `dict[node_id, AIOKafkaConnection]`, it now keeps a nested `dict[node_id, dict[group, AIOKafkaConnection]]`. The `send()` method (and any internal `_get_conn`) gains a `group=` keyword to pick the right socket.
- [`aiokafka/group_coordinator.py`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/group_coordinator.py) — every request the `GroupCoordinator` issues (JoinGroup, SyncGroup, Heartbeat, OffsetCommit, OffsetFetch) now passes `group=ConnectionGroup.COORDINATION` so it goes through the coordinator's dedicated socket rather than the default one.
- A small `ConnectionGroup` enum (DEFAULT, COORDINATION) is introduced to keep the group identifiers symbolic rather than stringy.

### Implementation Approach (~180 words)
The change is deliberately minimal: it does *not* introduce a connection pool, a load balancer, or per-request socket allocation. Instead it adds exactly one extra axis of identity — the socket group — to the existing one-connection-per-node abstraction. The client lazily opens a new `AIOKafkaConnection` the first time a `(node_id, group)` pair is used, caches it for the lifetime of the client, and tears it down at `close()`.

The chosen groups are pragmatic. `DEFAULT` continues to carry the fetcher, the metadata refresher, and the producer. `COORDINATION` carries everything emitted by the `GroupCoordinator`. Splitting just these two is enough to fix the original symptom: heartbeats and commits can be flushed while a 500 ms `Fetch` request sits on the default socket. The cost is one extra TCP connection per broker per client when a consumer group is in use — a negligible amount.

Because Kafka brokers identify clients by the API version handshake on each connection, the new coordination socket goes through the normal `ApiVersionsRequest` dance on first use; no change to the protocol layer was required.

### Potential Impact (~95 words)
The networking layer is the most central piece of the library, so this change touches code that everything else depends on. Every internal caller that builds requests now has to be aware of which socket group its request belongs to. Operationally, deployments see a small uptick in the number of open sockets (roughly +1 connection per broker per consuming client). On the upside, commit latency and heartbeat reliability under high-throughput consumers improve substantially. There is no observable change to the public API; users get the benefit automatically by upgrading.

---

## Why I picked these two and not the others

- **#25** (producer batches) and **#1006** (transactional producer) are very large multi-commit redesigns; comprehension would require reading the Kafka protocol spec around `Produce` v2/v3 and idempotent producer semantics — outside the scope of a focused review.
- **#115** depends on understanding compacted-topic offset gaps and the fetcher's per-message-set tracking.
- **#143** and **#217/#237** are subtle rebalance-protocol corrections — comprehensible only with prior context on JoinGroup/SyncGroup flows.
- **#201** and **#232** add transport features (SSL, SASL) where the change is mostly configuration plumbing; the per-PR diff is short but the security-protocol context is significant.

PRs **#193** and **#196** sit in a sweet spot: small diffs, clear motivation in a linked issue, observable user-facing benefit, and each touches code (the consumer API surface, the client connection map) that can be reasoned about without the full Kafka protocol in hand.
