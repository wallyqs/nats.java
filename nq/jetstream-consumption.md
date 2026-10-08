# Gap analysis: JetStream consumption and acks, jnats (v2.26.x) vs NQ Java (`dev.nq`)

Scope: consuming JetStream messages and acknowledging them. Stream and consumer CRUD, publish, KV and ObjectStore are out of scope.

Sources read:
- jnats: `nats.java/src/main/java/io/nats/client/**`.
- NQ Java: `nq.dev/packages/java/src/main/java/dev/nq/JetStream.java`. Every `dev.nq` symbol below is `JetStream.<Name>` in that file, and every bare line number is a line of that file.
- Oracle: nats.go v1.54.0. `jetstream/*` is the new API; `js.go`/`nats.go` is the legacy API.

Abbreviations: **JS.java** = `dev/nq/JetStream.java`; **jn/** = `io/nats/client/`; **go/** = nats.go root; **gojs/** = `nats.go/jetstream/`.

## (a) Summary

- **What NQ Java covers.** It reproduces the nats.go `jetstream` consumption API in full, and the code matches the docs:
  - pull `Consumer`: `fetch`, `fetchBytes`, `fetchNoWait`, `next`, `consume`, `messages`, `info`, `cachedInfo`;
  - every `Fetch*`, `Pull*` and `Next*` option, plus `stopAfter`, `consumeErrHandler` and `withMessagesErrOnMissingHeartbeat`;
  - priority groups: overflow (min pending / min ack pending), pinned (pin-ID tracking, 423 clears the pin) and prioritized (priority), plus `unpinConsumer`;
  - heartbeat monitoring and re-pull, threshold refill, reconnect re-pull;
  - the ordered consumer, which is pull-based;
  - the push consumer's `consume`, including flow-control replies and missed-heartbeat detection;
  - the full `Msg` ack verb set, including `termWithReason`, which jnats lacks;
  - v1 and v2 metadata parsing.
- **Shape of the gaps.**
  1. **The whole legacy jnats `JetStream.subscribe(...)` family is absent**: push sync and async, pull subscribe, `JetStreamSubscription.pull*`, `PushSubscribeOptions`/`PullSubscribeOptions`, bind/fastBind, autoAck, ordered push, dispatcher-based delivery, `JetStreamReader`. This is by design: docs/JETSTREAM-PLAN.md:57 puts legacy `js.go` out of scope. nats.go has most of these only in legacy `js.go`, not in `jetstream/`. NQ keeps a legacy ordered push subscription internally (`LegacyOrdered`, JS.java:5238) for KV watch and Object Store only; it is not public.
  2. **jnats-only knobs and introspection in the simplified API**:
     - user `Dispatcher` for `consume`;
     - `FetchConsumeOptions.max(bytes, msgs)` and `noWaitExpiresIn`;
     - `thresholdPercent` (NQ uses absolute thresholds);
     - the `raiseStatusWarnings` toggle;
     - `isStopped`/`isFinished`, `AutoCloseable`, and `getConsumerName` on the context or the running consumer;
     - stopping an in-flight fetch;
     - JSON (de)serialisation of the options;
     - on `Message`: `lastAck()`, `isJetStream()`, `isStatusMessage`/`getStatus()` and `consumeByteCount()`.
  3. **ErrorListener-style hooks** (`pullStatusWarning`, `pullStatusError`, `heartbeatAlarm`, `unhandledStatus`, `flowControlProcessed`) have no connection-level counterpart. NQ routes everything to the per-consume `consumeErrHandler`, to `MessageBatch.error()`, or to the exception from `MessagesContext.next()`, as nats.go does.
- **No other NQ target fills a Java gap.** All 12 target manifests list the same implemented consumption symbols. go.native's 52 extra "implemented" symbols are management and lister items: Stream `PauseConsumer`/`ResetConsumer`, lister `Err`, `JSErrCode*` and `With*` stream options. A grep of every shell and package for the jnats-specific features (autoAck, fastBind/SkipConsumerLookup, raiseStatusWarnings, thresholdPercent, noWait+expires, ackSync, lastAck, consumeByteCount, isJetStream) found nothing. No target exposes legacy `js.Subscribe`/`PullSubscribe`.
- **The manifest is badly stale.** `ir/capabilities/jetstream/java.nio.json` lists about 70 consumption-related types and fields as `planned` ("J4/J5 … awaiting IR") that exist and work in Java; see (d).

Counts (consumption scope): **26 missing** (19 jnats-only, 7 legacy nats.go `js.go`-only), **14 partial**, **about 60 present**.

## (b) MISSING and PARTIAL features

Status key: **M** = missing, **P** = partial. In the "Other NQ targets" column, "same" means no NQ target has it either.

### b.1 Legacy jnats subscribe API (all missing in NQ by design)

| jnats feature | jnats ref | NQ Java status + ref | Oracle ref or jnats-only | Other NQ targets | Notes |
|---|---|---|---|---|---|
| Push **sync** subscribe: `js.subscribe(subject[, PushSubscribeOptions])`, then `sub.nextMessage(timeout)` | jn/JetStream.java:435,454; jn/JetStreamSubscription.java (`nextMessage` via Subscription) | **M**. `PushConsumer` (JS.java:3974) has only async `consume` (4007), with no pull-style iteration | go/js.go:1523 `SubscribeSync` (legacy only); gojs `PushConsumer` has `Consume` only (gojs/consumer.go:171) | same | Out of scope per docs/JETSTREAM-PLAN.md:57 |
| Push **async** subscribe with `Dispatcher` + `MessageHandler` | jn/JetStream.java:490,507 | **M**. The nearest equivalent is `PushConsumer.consume` (4007) on a pre-created consumer, run on the subscription's delivery thread | go/js.go:1514 `Subscribe` | same | |
| **autoAck** flag on async push subscribe | jn/JetStream.java:490; impl/NatsJetStream.java:576-588 | **M**. The handler must call `Msg.ack()` | go/js.go:2660 (`ManualAck`; legacy auto-acks by default) | same | gojs never auto-acks |
| Queue / deliver-group subscribe (`subscribe(subject, queue, …)`) | jn/JetStream.java:474,526; jn/PushSubscribeOptions.java:63 `deliverGroup` | **P**. `PushConsumer.consume` subscribes with `ConsumerConfig.deliverGroup` (JS.java:4025). There is no subject-plus-queue entry point and no check of the queue name against the consumer | go/js.go:1531,1540 `QueueSubscribe[Sync]` | same | Works if the user creates the push consumer with `deliverGroup` |
| Subject-driven subscribe: stream lookup by subject and implicit consumer create/lookup | impl/NatsJetStream.java:248,337 | **M**. Manual composition is possible: `Context.streamNameBySubject` (JS.java:1107), then `createConsumer` | go/js.go:1684 `subscribe` | same | |
| Validation that an existing consumer's config is compatible on (re)subscribe ("Changed fields") | impl/NatsJetStream.java:357-368,523 | **M** | go/js.go:1684 (legacy `checkConfig` path) | same | |
| `SubscribeOptions`: `stream`, `durable`, `name`, `configuration(ConsumerConfiguration)`, `bind` | jn/SubscribeOptions.java:186-205 | **P**. Equivalent through `Context.consumer(stream,name)` (1185), `Stream.consumer` (1842) and `create*Consumer(config)` | go/js.go:2833 `BindStream`, :2846 `Bind`, :2678 `Durable`, :2972 `ConsumerName` | same | Bind ≈ look up the consumer handle |
| `PullSubscribeOptions.fastBind` (skip the consumer-info lookup) | jn/PullSubscribeOptions.java:19,37 | **M**. `consumer()` always issues CONSUMER.INFO (JS.java:1185 → `getConsumer` 1220-1227) | go/js.go:2642 `SkipConsumerLookup` (legacy); gojs has no equivalent | same | jnats's own simplified API uses fastBind internally (impl/NatsConsumerContext.java:64) |
| `SubscribeOptions.messageAlarmTime` (custom heartbeat alarm interval) | jn/SubscribeOptions.java:210 | **M**. The alarm is fixed at 2 × heartbeat (JS.java:2752-2754) | jnats-only (nats.go also uses 2 × hb) | same | |
| `PushSubscribeOptions.pendingMessageLimit` / `pendingByteLimit` | jn/PushSubscribeOptions.java:68,73 | **M** for JS consumers. Core `Subscription.setPendingLimits` exists (dev/nq/Subscription.java:176), but the consumer's internal subscription is not exposed | go `Subscription.SetPendingLimits` (core); not in gojs | same | |
| **Ordered push** consumer (`PushSubscribeOptions.ordered(true)`): gap detection and reset | jn/PushSubscribeOptions.java:53; impl/OrderedMessageManager.java:29,56 | **P**. A public ordered consumer exists but is pull-based (`Context.orderedConsumer` 1260; `OrderedConsumer` 3450). The ordered **push** machinery exists only internally (`LegacyOrdered` 5238, used by KV watch and OBJ get/watch) | go/js.go:2652 `OrderedConsumer`, :2174 `checkOrderedMsgs`, :2304 `resetOrderedConsumer` | same (every target has an internal LegacyOrdered for KV/OBJ only) | |
| **Pull subscription** `js.subscribe(subject, PullSubscribeOptions)` | jn/JetStream.java:538 | **M** as an API. The functional equivalent is `Consumer` (1435) | go/js.go:1562 `PullSubscribe` | same | |
| Async pull subscription with `Dispatcher` + handler (user calls `pull()` manually) | jn/JetStream.java:553 | **M** | jnats-only | same | |
| `JetStreamSubscription.pull(int)` / `pull(PullRequestOptions)`: raw pull, messages via `nextMessage` | jn/JetStreamSubscription.java:48,58 | **M**. Pulls are sent only by `fetch*`/`consume`/`messages` (`PullConsumer.send` 1637) | jnats-only (legacy nats.go has only `Fetch`) | same | |
| `pullNoWait(batch[, expires])`, `pullExpiresIn(batch, expires)` | jn/JetStreamSubscription.java:69,82,95,113,132 | **P**. `fetchNoWait(batch)` (1518), and `fetch(batch, fetchMaxWait(ns))` (2433). There is no no-wait **with** expiry | jnats-only | same | |
| `PullRequestOptions` as a standalone builder (batchSize, maxBytes, noWait, expiresIn, idleHeartbeat, group, priority, minPending, minAckPending) | jn/PullRequestOptions.java:152-310 | **P**. All fields exist in the IR request (`PullRequest.req`, 2399; `js_fetch_request`), but only as `FetchOpt`s on `fetch`/`fetchBytes`/`next`. `fetchNoWait` takes no options, and messages and bytes cannot be combined | partly go/js.go:1254 `MaxWait`, :3043 `PullMaxBytes`, :3032 `PullHeartbeat`; gojs/jetstream_options.go:478-559 | same | See b.2 for the no-wait and max combinations |
| Legacy `sub.fetch(batch, maxWait)` → `List<Message>` and `iterate(batch, maxWait)` → `Iterator` | jn/JetStreamSubscription.java:147,162,178,194 | (present by equivalence) `Consumer.fetch` + `MessageBatch.messages()` (1514, 2767) | go/js.go:3126 `Fetch`, :3425 `FetchBatch` | — | Listed for completeness; see (c) |
| `JetStreamReader` (`reader(batch, repullAt)`, deprecated) | jn/JetStreamSubscription.java:208; jn/JetStreamReader.java:32-49 | **P**. `Consumer.messages(pullMaxMessages, pullThresholdMessages)` (1471) is the modern equivalent | jnats-only | same | |
| Flow control on legacy push, including the **`Nats-Consumer-Stalled` header on heartbeats** and de-duplication of the FC subject | impl/PushMessageManager.java:81-114 | **P**. `PushConsumer.consume` answers FC requests (`js_push_status_effect`, JS.java:4093-4100) as gojs push.go does. A heartbeat carrying `Nats-Consumer-Stalled` is **not** answered, and the same FC subject is not de-duplicated | go/nats.go:3936-3939 (stalled header, legacy only); gojs/push.go:180-188 (FC request only) | same | Matches the oracle `jetstream`; diverges from jnats and legacy nats.go |
| `ErrorListener.flowControlProcessed` | jn/ErrorListener.java:144 | **M** | jnats-only | same | |
| `ErrorListener.unhandledStatus` (push) | jn/ErrorListener.java:99 | **P**. Unknown push statuses are dropped. 409 Consumer Deleted and Leadership Change reach `consumeErrHandler` (4101-4106) | gojs/push.go:189-200 | same | |

### b.2 Simplified API (StreamContext / ConsumerContext family)

| jnats feature | jnats ref | NQ Java status + ref | Oracle ref or jnats-only | Other NQ targets | Notes |
|---|---|---|---|---|---|
| `consume(dispatcher, handler)` / `consume(opts, dispatcher, handler)`: user-supplied `Dispatcher` (and a shared default dispatcher per context) | jn/BaseConsumerContext.java:158,184; impl/NatsConsumerContext.java:113-123 | **M**. The handler runs on the subscription's own delivery thread (JS.java:1667-1683) | jnats-only | same | |
| `FetchConsumeOptions.max(maxBytes, maxMessages)`: both limits together | jn/FetchConsumeOptions.java:141 | **M**. `fetchBytes(maxBytes)` fixes the batch at the IR default (`js_fetch_defaults(fetch_bytes)`), as gojs/pull.go:872 does. `fetch(batch)` cannot take a byte limit | jnats-only | same | |
| `FetchConsumeOptions.noWait()` combined with other options (group, priority, minPending, bytes) | jn/FetchConsumeOptions.java:160 | **P**. `fetchNoWait(int batch)` takes no options (1447, 1518) | gojs/pull.go:908 `FetchNoWait(batch)`, which also takes no options | same | |
| `FetchConsumeOptions.noWaitExpiresIn(ms)`: no_wait **plus** expires ("at least one message") | jn/FetchConsumeOptions.java:173 | **M** | jnats-only | same | |
| `BaseConsumeOptions.thresholdPercent` (default 25%) | jn/BaseConsumeOptions.java:257 | **P**. Absolute `pullThresholdMessages` / `pullThresholdBytes` (2519, 2522); the IR default is 50% (`js_pull_set_defaults`) | gojs/jetstream_options.go:282,299; default gojs/pull.go:1201-1207 | same | Percentages can be computed by hand, but the defaults differ (25% vs 50%) |
| `raiseStatusWarnings()` toggle (send 404/408/409-warning statuses to `ErrorListener.pullStatusWarning`) | jn/BaseConsumeOptions.java:267,278; impl/PullMessageManager.java:140-170 | **P**. There is no toggle. Non-terminal statuses always go to `consumeErrHandler` as `notify` (`js_pull_status`, ir/jetstream-pull.nqir:1169; JS.java:3019-3033). 408/max-bytes/batch-completed are absorbed into pending counts and never reported | gojs/pull.go:738 `handleStatusMsg` | same | Which statuses are reported differs from jnats |
| `ErrorListener.pullStatusWarning` / `pullStatusError` (connection-level listener) | jn/ErrorListener.java:109,119 | **P**. Reported per run: `consumeErrHandler` (2548), `MessageBatch.error()` (2786), or the exception from `MessagesContext.next` (3182) | gojs/pull.go:131 `ConsumeErrHandlerFunc` | same | |
| `ErrorListener.heartbeatAlarm` | jn/ErrorListener.java:90; impl/MessageManager.java:106,140 | **P**. `NO_HEARTBEAT` goes to `consumeErrHandler` (consume, 3093-3104), to `messages().next()` (3273-3276), or ends a fetch (2754). There is no connection-level callback | gojs/pull.go:1085 `scheduleHeartbeatCheck`; gojs/errors.go:316 `ErrNoHeartbeat` | same | |
| `MessageConsumer.isStopped()` / `isFinished()` | jn/MessageConsumer.java:66,72; impl/NatsMessageConsumerBase.java:62,69 | **P**. `ConsumeContext.closed()` returns a `CompletableFuture` (2967); `isDone()` ≈ isFinished. There is no isStopped, and `MessagesContext` has no closed/finished signal (3118-3132) | gojs/pull.go:70 `Closed()` (ConsumeContext only) | same | |
| `MessageConsumer.stop()` semantics: no new pulls, but the in-flight pull completes and is delivered | jn/BaseMessageConsumer.java:25; impl/NatsMessageConsumerBase.java:108 | **P**. `stop()` unsubscribes immediately and discards buffered messages (2983). `drain()` drains the subscription (2985) but does not wait for the outstanding pull to expire or complete | gojs/pull.go:786 `Stop`, :804 `Drain` | same | The closest match for jnats `stop` is NQ `drain` |
| `AutoCloseable.close()` on MessageConsumer / FetchConsumer / IterableConsumer | jn/BaseMessageConsumer.java:20; jn/MessageConsumer.java:60 | **M**. `ConsumeContext` and `MessagesContext` are not `AutoCloseable` | jnats-only | same | Minor Java idiom gap |
| `FetchConsumer.stop()` / `isFinished()`: cancel an in-flight fetch | jn/FetchConsumer.java (extends MessageConsumer) | **M**. `MessageBatch` has only `messages()` and `error()` (2758-2786); the fetch ends only at batch, expiry or `fetchContext` deadline | jnats-only (gojs `MessageBatch` has no stop) | same | `fetchContext(timeout)` (2446) bounds it |
| `getConsumerInfo()` / `getCachedConsumerInfo()` / `getConsumerName()` on the **running** MessageConsumer | jn/MessageConsumer.java:31,40,48 | **P**. Available only on the `Consumer` handle (`info` 1708, `cachedInfo` 1713), not on `ConsumeContext`/`MessagesContext` | gojs/consumer.go:157,162 (on Consumer) | same | |
| `ConsumerContext.getConsumerName()` / `OrderedConsumerContext.getConsumerName()` | jn/BaseConsumerContext.java:33; jn/OrderedConsumerContext.java:39 | **P**. No accessor; use `cachedInfo().name`. For an ordered consumer, `cachedInfo()` gives the current consumer (3851) | jnats-only (gojs uses `CachedInfo().Name`) | same | |
| `IterableConsumer.nextMessage(timeout)` returns **null** on timeout | jn/IterableConsumer.java:32,44 | **P**. `MessagesContext.next(nextMaxWait(ns))` **throws** the core timeout (3125, 2595) | gojs/pull.go:614 `Next` returns `nats.ErrTimeout` | same | Behavioural difference |
| `BaseConsumerContext.next()` returns null on timeout; `next(ms)` requires at least 1000 ms | jn/BaseConsumerContext.java:46-78; impl/NatsConsumerContext.java:206-210 | **P**. `Consumer.next(FetchOpt...)` (1524) **throws** timeout; validation follows the IR (`js_check_fetch`) | gojs/pull.go:1043 | same | |
| Pinned-client restriction: jnats refuses `next()`/`fetch()` on a `PinnedClient` consumer | impl/NatsConsumerContext.java:146-151,215,264 | **P** (divergent). NQ allows fetch/next on pinned consumers and carries the pin ID (1551), as nats.go does | gojs/pull.go:917 (no such restriction) | same | NQ follows the oracle |
| JSON (de)serialisation of `ConsumeOptions` / `FetchConsumeOptions` / `OrderedConsumerConfiguration` (`json()`, `jsonValue()`, `toJson()`, JSON constructors) | jn/BaseConsumeOptions.java:86,190,199; jn/api/OrderedConsumerConfiguration.java:38,42,54 | **M** | jnats-only | same | |
| `PriorityGroupOptions` | — | n/a. **No such class exists in jnats 2.26.** Priority settings live on `BaseConsumeOptions` (group/priority/minPending/minAckPending) and `PullRequestOptions` | — | — | Covered in (c) |

### b.3 Message / ack API

| jnats feature | jnats ref | NQ Java status + ref | Oracle ref or jnats-only | Other NQ targets | Notes |
|---|---|---|---|---|---|
| `Message.lastAck()` (`AckType`) | jn/Message.java:121; impl/AckType.java:7-13 | **M**. NQ keeps a private `ackd` flag (2256) | jnats-only | same | |
| Ack-after-terminal semantics: jnats silently ignores an ack after ACK/NAK/TERM and allows `inProgress` repeatedly | impl/NatsJetStreamMessage.java:119-129 | **P** (divergent). NQ throws `MSG_ALREADY_ACKD` (`js_ack_prepare`, `ackReply` 2310), as nats.go does | gojs/message.go:405 `ackReply`; gojs/errors.go:260 | same | NQ follows the oracle |
| `Message.isJetStream()` | jn/Message.java:173 | **M**. `JetStream.Msg` is always a JetStream message, and core `dev.nq.Message` has no `isJetStream` | jnats-only (legacy nats.go only implicitly, via `Metadata()` error) | same | Minor |
| `Message.isStatusMessage()` / `getStatus()` on consumed messages | jn/Message.java:68,74 | **P**. Status messages never reach the handler. `Msg.headers()` exposes `Status`/`Description` (2276; `statusHeaders` 2327) | gojs/message.go:43 `Headers` | same | |
| `Message.consumeByteCount()` (the size used for byte-limit accounting) | jn/Message.java:180; impl/NatsMessage.java:383 | **M**. Package-private `Message.size()` only | jnats-only | same | |
| `NatsJetStreamMetaData.getMetaType()` (ACK vs FC); acceptance of an 8-token v1 reply without pending | impl/NatsJetStreamMetaData.java:62-119 | **P**. `MsgMetadata` (2225) has no meta type; the token rules follow nats.go (`js_parse_metadata`) | gojs/message.go:314 | same | Minor |
| `nakWithDelay(Duration)` / `nakWithDelay(long millis)` overloads | jn/Message.java:149,156 | **P**. Only `nakWithDelay(long nanos)` (2297). Different unit; no Duration overload | gojs/message.go:70 (Duration) | same | API ergonomics |
| `ackSync(Duration)` → throws `TimeoutException` | jn/Message.java:136 | Present as `doubleAck(Duration)` (2291). The exception type differs (`NqException` timeout) | gojs/message.go:58 `DoubleAck` | — | Listed as a behavioural note; also in (c) |

## (c) PRESENT features (jnats → dev.nq)

Context and handles:
- `Connection.jetStream()` / `JetStream` → `JetStream.create(client, …)` (55-58), `createWithDomain`/`createWithApiPrefix` (61-75).
- `js.getStreamContext(name)` → `Context.stream(name)` → `Stream` (1099).
- `js.getConsumerContext(stream, consumer)` / `StreamContext.getConsumerContext(name)` → `Context.consumer(stream,name)` (1185) / `Stream.consumer(name)` (1842).
- `StreamContext.createOrUpdateConsumer(cc)` → `Stream.createOrUpdateConsumer` (1833) / `Context.createOrUpdateConsumer` (1167).
- `StreamContext.createOrderedConsumer(occ)` → `Stream.orderedConsumer` (1848) / `Context.orderedConsumer` (1260).
- `ConsumerContext.getConsumerInfo()` → `Consumer.info()` (1474/1708).
- `getCachedConsumerInfo()` → `Consumer.cachedInfo()` (1477/1713).
- `ConsumerContext.unpin(group)` → `Stream.unpinConsumer(consumer, group)` (1875). Oracle gojs/stream.go:122.

Pull: next and fetch:
- `next()` / `next(Duration)` / `next(ms)` → `Consumer.next(FetchOpt...)` with `fetchMaxWait(ns)` (1454/1524, 2433). Oracle gojs/pull.go:1043.
- `fetchMessages(n)` → `Consumer.fetch(n)` (1441/1514). Oracle gojs/pull.go:839.
- `fetchBytes(n)` → `Consumer.fetchBytes(n)` (1444/1516). Oracle gojs/pull.go:872.
- `fetch(FetchConsumeOptions)` → `fetch(batch, FetchOpt...)` (1514), with:
  - `maxMessages` → batch;
  - `maxBytes` → `fetchBytes`;
  - `expiresIn` → `fetchMaxWait` (2433);
  - `group` → `fetchPriorityGroup` (2430);
  - `priority` → `fetchPrioritized` (2427);
  - `minPending` → `fetchMinPending` (2421);
  - `minAckPending` → `fetchMinAckPending` (2424);
  - heartbeat (auto in jnats) → `fetchHeartbeat` (2436), with the IR default of 5 s for expiries of at least 10 s (gojs/pull.go:855-861).
- `FetchConsumeOptions.noWait()` → `fetchNoWait(batch)` (1518). Oracle gojs/pull.go:908.
- `FetchConsumer.nextMessage()` → `MessageBatch.messages()` iterator + `error()` (2767, 2786). Oracle gojs/pull.go:160-161.
- Legacy `sub.fetch/iterate(batch, maxWait)` → the same `fetch` + `MessageBatch`.

Pull: iterate and consume:
- `iterate()` / `iterate(ConsumeOptions)` → `Consumer.messages(PullOpt...)` (1471/1690). Oracle gojs/pull.go:504.
- `IterableConsumer.nextMessage(timeout)` → `MessagesContext.next(nextMaxWait(ns) | nextContext(ns))` (3125, 2595-2601).
- `consume(handler)` / `consume(ConsumeOptions, handler)` → `Consumer.consume(handler, PullOpt...)` (1463/1661). Oracle gojs/pull.go:202.
- `ConsumeOptions` mappings:
  - `batchSize` → `pullMaxMessages` (2505);
  - `batchBytes` → `pullMaxBytes` (2516), or `pullMaxMessagesWithBytesLimit` (2508);
  - `expiresIn` → `pullExpiry` (2513);
  - heartbeat (auto) → `pullHeartbeat` (2537), whose default `expiry/2` capped at 30 s is the same rule as jnats `BaseConsumeOptions.java:73`;
  - `group` → `pullPriorityGroup` (2534);
  - `priority` → `pullPrioritized` (2531);
  - `minPending` → `pullMinPending` (2525);
  - `minAckPending` → `pullMinAckPending` (2528).
- Threshold refill (re-pull when pending falls below the threshold) → `checkPending` / `js_pull_check` (2839), `pullMessages` (2918).
- Pending-header accounting on 408/409 (`Nats-Pending-Messages`/`-Bytes`) → `js_pull_status` (ir/jetstream-pull.nqir:1169), `handleStatus` (2845). jnats: impl/PullMessageManager.java:69-118.
- Re-pull after a missed heartbeat and after a reconnect → `listenLoop` (3061) and `PullMessagesContext` (3190-3282).
- Terminal statuses (409 Consumer Deleted, 400) end the consume and are reported (3027-3032). jnats `pullStatusError`.
- `MessageConsumer.stop()` → `ConsumeContext.stop()` (2961) and `drain()` (2964); `closed()` (2967). `MessagesContext.stop/drain` (3128-3131).

Priority groups:
- Overflow (min pending / min ack pending) → as above.
- Pinned client: the `Nats-Pin-Id` header is captured (1605, 3040, 3268) and sent on later pulls (1551, 2827); 423 pin mismatch clears it (`clear_pin`, 1600, 2850). jnats: impl/PullMessageManager.java:96,131-137.
- Prioritized → `fetchPrioritized`/`pullPrioritized`.
- Group membership check before consume/messages → `checkGroup` / `js_check_priority_group` (1655-1658).
- `ConsumerConfig.priorityPolicy` (NONE/PINNED_CLIENT/OVERFLOW/PRIORITIZED), `pinnedTtl`, `priorityGroups` (7480-7527); `ConsumerInfo.priorityGroups` / `PriorityGroupState` (7542-7563).

Ordered consumer:
- `OrderedConsumerConfiguration` mappings to `OrderedConsumerConfig` (3306):
  - `filterSubject(s)` → `filterSubjects`;
  - `deliverPolicy`;
  - `startSequence` → `optStartSeq`;
  - `startTime` → `optStartTime`;
  - `replayPolicy`;
  - `headersOnly`;
  - `consumerNamePrefix` → `namePrefix`.
- NQ adds three fields jnats lacks: `inactiveThreshold`, `maxResetAttempts` and `metadata`. Oracle gojs/consumer_config.go:283-333.
- Ordered `next`/`fetch`/`fetchBytes`/`iterate`/`consume`:
  - `OrderedConsumer.next` (3776), `fetch` (3746), `fetchBytes` (3754), `fetchNoWait` (3766), `messages` (3698), `consume` (3523);
  - sequence-gap reset → `reset` (3790), with IR backoff and a background delete;
  - only one active run at a time → `js_ordered_begin` (`begin` 3507). jnats: impl/NatsConsumerContext.java:139-144.
  - Oracle gojs/ordered.go:82-558.

Push consumer:
- A push consumer with a deliver subject and deliver group → `createPushConsumer`/`pushConsumer` (1284/1302, 1851/1860), `PushConsumer.consume(handler, consumeErrHandler)` (4007). Oracle gojs/push.go:47.
- Flow-control request answered → JS.java:4093-4100. Oracle gojs/push.go:180-188.
- Idle-heartbeat monitoring on push (2 × `idleHeartbeat` → `NO_HEARTBEAT` to the error handler) → `scheduleHeartbeatCheck` 4073, `listenLoop` 4125.
- Push 409 Consumer Deleted (terminates) and Leadership Change (reported) → `js_push_status_effect` (4092-4108).
- One consume at a time (`CONSUMER_ALREADY_CONSUMING`) → 4018.

Message and ack:
- `ack()` → `Msg.ack()` (2285).
- `ackSync(Duration)` → `Msg.doubleAck()` / `doubleAck(Duration)` (2288/2291). Oracle gojs/message.go:58.
- `nak()` → `Msg.nak()` (2294).
- `nakWithDelay(...)` → `Msg.nakWithDelay(long ns)` (2297).
- `inProgress()` → `Msg.inProgress()` (2300).
- `term()` → `Msg.term()` (2303).
- NQ also has `termWithReason(String)` (2306), which jnats lacks. Oracle gojs/message.go:87.
- `metaData()` → `Msg.metadata()` → `MsgMetadata` (2261/2225), parsed by the IR `js_parse_metadata` for v1 and v2 (domain + account hash) reply subjects:
  - `streamSequence`/`consumerSequence` → `sequence.stream`/`.consumer` (`SequencePair` 2213);
  - `deliveredCount` → `numDelivered`;
  - `pendingCount` → `numPending`;
  - `timestamp`;
  - `getStream`/`getConsumer`/`getDomain` → `stream`/`consumer`/`domain`.
- `getData`/`getHeaders`/`getSubject`/`getReplyTo` → `data()` (2273) / `headers()` (2276) / `subject()` (2279) / `reply()` (2282).
- `MessageHandler` → `JetStream.MessageHandler` (2242).

Errors:
- `JetStreamStatusException` / `JetStreamStatusCheckedException` → `JetStream.JetStreamException` with a `Kind`: `NO_HEARTBEAT`, `CONSUMER_DELETED`, `BAD_REQUEST`, `MSG_ITERATOR_CLOSED`, `MSG_ALREADY_ACKD`, `ORDERED_CONSUMER_NOT_CREATED`, … (179-466, 502).
- Pull 408/503 → the core `NqException` timeout/no-responders (`pullError`, 2351).

## (d) Stale manifest entries (`ir/capabilities/jetstream/java.nio.json`)

> **Correction:** this section overstates the staleness. Most of these entries stay `planned` correctly under nq.dev's evidence rule; only the reason text was stale. See [README §5.3](README.md#5-problems-found-in-nqdev-now-fixed).


The manifest has `stage: evidence`, with 394 implemented and 510 planned symbols. The java.threaded, python, ruby, rust, ts and c manifests carry **identical** status sets, which suggests they are stamped from one template. The `planned` reasons still say "J4 (pull consumption): awaiting IR…" or "J5 (ordered and push consumers): awaiting IR…". However, docs/JETSTREAM-PLAN.md:1974 and :2277 (Java paragraph at :2425) record J4b and J5b as landed for Java.

The following consumption-scope symbols are marked `planned` but exist in Java:

- `jetstream.Consumer` (gojs/consumer.go:50) → `JetStream.Consumer` interface (1435).
- `jetstream.ConsumeContext` (pull.go:57) → `ConsumeContext` (2959).
- `jetstream.MessagesContext` (pull.go:35) → `MessagesContext` (3118).
- `jetstream.MessageBatch` (pull.go:159) → `MessageBatch` (2758).
- `jetstream.MessageHandler` (pull.go:74) → `MessageHandler` (2242).
- `jetstream.ConsumeErrHandlerFunc` (pull.go:131) → `ConsumeErrHandlerFunc` (2246).
- `jetstream.FetchOpt` (pull.go:172) → `FetchOpt` (2406).
- `jetstream.NextOpt` (pull.go:180) → `NextOpt` (2588).
- `jetstream.PullConsumeOpt` (pull.go:77), `jetstream.PullMessagesOpt` (pull.go:82) and `jetstream.PushConsumeOpt` (push.go:42) → `PullOpt` (2480), with consume/messages/push flags.
- `jetstream.Msg` (message.go:35) → `Msg` (2252).
- `jetstream.MsgMetadata` and all 7 fields (message.go:91-113) → `MsgMetadata` (2225).
- `jetstream.SequencePair`, `.Consumer`, `.Stream` (message.go:118-125) → `SequencePair` (2213).
- `jetstream.PushConsumer` (consumer.go:165) → `PushConsumer` (3974). Its methods are already marked implemented, but the type is not.
- `jetstream.OrderedConsumerConfig` and all 10 fields (consumer_config.go:283-333) → `OrderedConsumerConfig` (3306).
- `jetstream.PriorityPolicy` + `None`/`Pinned`/`Overflow`/`Prioritized` + Marshal/UnmarshalJSON (consumer_config.go:356-394) → `PriorityPolicy` enum (7480).
- `jetstream.PriorityGroupState` + `Group`/`PinnedClientID`/`PinnedTS` (consumer_config.go:90-98) → `PriorityGroupState` (7542).
- `jetstream.AckPolicy` + `AckExplicit`/`AckAll`/`AckNone`/`AckFlowControlPolicy` (consumer_config.go:341,493-507) → `AckPolicy` enum including `FLOW_CONTROL` (7446).
- `jetstream.ConsumerConfig.*` consumption fields → `ConsumerConfig` (7498), which has `ackPolicy`, `ackWait`, `maxDeliver`, `backOff`, `maxAckPending`, `priorityPolicy`, `pinnedTtl`, `priorityGroups`, `deliverSubject`, `deliverGroup`, `flowControl`, `idleHeartbeat`, `headersOnly`, `maxWaiting`, `maxRequest*` and others. The affected fields are `AckPolicy`, `AckWait`, `BackOff`, `DeliverGroup`, `DeliverSubject`, `FlowControl`, `IdleHeartbeat`, `MaxAckPending`, `MaxRequestBatch/Expires/MaxBytes`, `MaxWaiting`, `PinnedTTL`, `PriorityGroups` and `PriorityPolicy`.
- `jetstream.ConsumerInfo.*` (`NumAckPending`, `NumPending`, `NumWaiting`, `PriorityGroups`, `PushBound`, `Delivered`, `AckFloor`, …) → `ConsumerInfo` (7549).
- Adjacent, management scope but the same staleness: `jetstream.ConsumerManager.PauseConsumer`/`ResumeConsumer`/`ResetConsumer`/`ResetConsumerToSequence` (stream.go:107-134) are `planned` for Java and `implemented` for go.native, yet Java has `Stream.pauseConsumer` (1863), `resumeConsumer` (1866), `resetConsumer` (1869), `resetConsumerToSequence` (1872) and the Context-level equivalents (1197-1215). The same applies to `ConsumerPauseResponse`/`ConsumerResetResponse` (7569/7576).
- Publish-scope types (`MsgAckHandler`, `MsgErrHandler`, `PubAck`, `PubAckFuture`) appear `planned` but exist (713, 720, 7616, 814). They are out of scope here and listed only as more evidence of manifest staleness.

Manifest entries that are correctly `implemented`: every `Consumer.*`, `ConsumeContext.*`, `MessagesContext.*`, `MessageBatch.*` and `Msg.*` method; all `Fetch*`/`Pull*`/`Next*`/`StopAfter`/`ConsumeErrHandler`/`WithMessagesErrOnMissingHeartbeat` options; `ConsumerManager.UnpinConsumer`; `PushConsumer.*`; and `ErrNoHeartbeat`/`ErrMsgAlreadyAckd`/`ErrPinIDMismatch`/`ErrInvalidJSAck`.

Recommendation: re-derive the `planned` → `implemented` flags for types and fields from the shell source, or have the manifest generator treat a type as implemented when its methods or constructor are. Also refresh the reason strings, which still describe J4/J5 as pending.
