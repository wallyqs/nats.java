# Gap analysis: jnats JetStream context, management and publishing vs NQ Java (`dev.nq`)

Scope: the JetStream context, management API, publishing, config/info types and API errors. Consumption, subscriptions, KV and Object Store are out of scope.
jnats: `nats.java` (main, 2.26.5-SNAPSHOT). NQ Java: `nq.dev/packages/java`. Both Java profiles (NIO and threaded) share one shell file.

Reference abbreviations:
- `jn:` = `nats.java/src/main/java/io/nats/client/`
- `JS:` = `nq.dev/packages/java/src/main/java/dev/nq/JetStream.java`
- `CL:` = `.../dev/nq/Client.java`
- `CO:` = `.../dev/nq/Core.java`
- `ng:` = oracle `nats.go/` (`ng:js/` = `nats.go/jetstream/`)
- `ob:` = oracle `orbit.go/`

Every NQ status below was checked against the Java source, not the manifests.

## (a) Summary

- **NQ Java covers almost all of jnats' JetStream management and publishing.** All 24 `JetStreamManagement` operations have a counterpart, some reached through a `Stream` handle. These include:
  - account info
  - stream create, update, delete, info (with deleted details and subject filter) and purge (with subject, sequence or keep)
  - stream names and lists, with a subject filter
  - get by sequence, last-for-subject and next-for-subject, with direct get when the stream allows it
  - delete and secure-delete of a message
  - consumer create, update, create-or-update, delete, info and names/list
  - pause, resume, unpin, reset and reset-to-sequence of a consumer
- **Publishing is complete.** NQ has sync and async publishing, and every `PublishOptions` expectation header. Per-call timeouts are done with `Context.withTimeout`.
- **Every `StreamConfiguration` and `ConsumerConfiguration` field has a matching field** (39 and 34 fields respectively), up to `persistMode`, `allowBatched` and `priorityTimeout`.
- **NQ Java is a superset of jnats in several areas:**
  - `createOrUpdateStream` and `streamNameBySubject`
  - push-consumer management
  - message schedules and publish retry/stall options
  - async-publish flow control (max pending, ack timeout, handlers)
  - `ClientTrace`
  - full orbit.go atomic batch (`BatchPublisher`) and fast-ingest (`FastPublisher`) publishing. jnats 2.26.x has only the constants and config flags for these.
  - the API level advertised by the server
- **The real gaps are small and mostly specific to jnats:**
  - `publishNoAck`
  - `optOut290ConsumerCreate` and client-side server-version gating
  - the purged count from purge
  - get-first-message-by-start-time and a generic `MessageGetRequest`
  - typed `MessageInfo` extras (stream, lastSeq, numPending) and the direct-get header-name constants
  - `StreamInfo.alternates`, `StreamState.lost`, and `SourceInfo.external`/`error`
  - `ConsumerInfo.calculatedPending`
  - message TTL `"never"` and custom TTL strings
  - public JSON (de)serialization of config/info objects. The Go `nq.go` facade has this; Java does not.
  - nats.go-style string names for enums
- **Batch direct get (`multi_last` / `up_to_seq`) is missing in every NQ target.** It is not public API in jnats 2.26.x either: only the `MULTI_LAST` constant and an unused `directBatchGet211Available` flag exist. The oracle is orbit.go `jetstreamext/getbatch.go`.
- **Error-code bug (confirmed on nats-server v2.15.0 and fixed on `wallyqs/nq.dev` `dev` in `cefee41`).** NQ copies orbit.go's fast-batch error codes: 10203 for not-enabled through 10206 for unknown-id. jnats uses 10205–10209 instead. The NASIR server model, which is derived from nats-server `stream.go`, also uses 10205/10206/10207/10208 (`wallyqs/nasir: ir/natsserver/jsapi.nasir:5345-5411`). NQ's 10203 and 10204 also collide with nats.go's `ScheduleSourceInvalid` (10203) and `ConsumerInvalidReset` (10204). This needs a live check.
- **The capability manifests are badly out of date for Java.**
  - In `ir/capabilities/jetstream/java.nio.json` (and `java.threaded`), 311 in-scope symbols are marked `planned`. About 270 of them are present in `JetStream.java`: every struct type and field, the enums and their constants, every `JSErrCode*`, the option functions, pause/resume/reset and the listers.
  - In `ir/capabilities/orbit/java.nio.json`, 26 batch-publishing types and fields are marked `planned` but are implemented.
  - See section (d).

## (b) MISSING and PARTIAL features

| # | jnats feature | jnats ref | NQ Java status + ref | oracle ref or jnats-only | other NQ targets | notes |
|---|---|---|---|---|---|---|
| 1 | `JetStreamOptions.publishNoAck`: publish without waiting for an ack; returns `null` / no future | `jn:JetStreamOptions.java:90,202`; `jn:impl/NatsJetStream.java:153,166` | **MISSING.** `JS:101` `Options` has only `defaultTimeout` and `clientTrace` | jnats-only | none | Workaround: core `Client.publish` with headers built by hand. The expectation-header merge (`js_publish_headers`) is not public. |
| 2 | `JetStreamOptions.optOut290ConsumerCreate`: use the legacy `CONSUMER.CREATE.<stream>` / `CONSUMER.DURABLE.CREATE` subjects | `jn:JetStreamOptions.java:98,212`; `jn:impl/NatsJetStreamImpl.java:65,109-128`; `jn:support/NatsJetStreamConstants` (`JSAPI_CONSUMER_CREATE`, `JSAPI_DURABLE_CREATE`) | **MISSING.** NQ always uses `CONSUMER.CREATE.<stream>.<name>[.<filter>]` (IR `js_request_consumer_create`, called from `JS:1223-1243`) | jetstream pkg: `ng:js/api.go:51,55` (no legacy path). Legacy: `ng:js.go:164,168`; `ng:jsm.go:511` | none | Servers older than 2.9 are not supported, which is the jetstream-pkg behavior. |
| 3 | Client-side server-version feature gating: name needs ≥2.9.0, multiple filter subjects need >2.9.99, direct batch get flag >2.10.99 | `jn:impl/NatsJetStreamImpl.java:47-49,65-67,95-105`; client errors `jn:support/NatsJetStreamClientError.java:70,72` (90301, 90303) | **PARTIAL (different mechanism).** No version gates. Unsupported features are detected from the response: `js_consumer_create_check` (`JS:1236-1241`, `Kind.CONSUMER_MULTIPLE_FILTER_SUBJECTS_NOT_SUPPORTED`) and `js_stream_create_check` (`JS:2152-2159`, `STREAM_SUBJECT_TRANSFORM_NOT_SUPPORTED` / `STREAM_SOURCE_NOT_SUPPORTED`) | Response-based in the jetstream pkg: `ng:js/consumer.go:345`, `ng:js/jetstream.go:638-656,764` | Same in all targets (shared IR) | Not a functional gap against nats.go. jnats' gate can refuse a request before it is sent; NQ finds out after the fact. |
| 4 | `ServerInfo` version comparators: `isNewerVersionThan`, `isSameOrNewerThanVersion`, `isOlderThanVersion`, … | `jn:api/ServerInfo.java:263-299`; `jn:support/ServerVersion.java` | **MISSING.** Only the string `CL:248` `connectedServerVersion()` and `CL:287` `connectedServerJetStream()` (enabled, apiLevel) | Only private `ng:nats.go:2787` `serverMinVersion`; public `ConnectedServerVersion` `ng:nats.go:2796` | none | Core-level. NQ does expose `api_lvl`, which jnats does not. Neither jnats nor nats.go v1.54 sends a required-API-level header, so that part of the brief does not apply. |
| 5 | `purgeStream` returns `PurgeResponse` (`isSuccess`, `getPurged`/`getPurgedCount`) | `jn:JetStreamManagement.java:102,113`; `jn:api/PurgeResponse.java:45,58,66`; `jn:StreamContext.java:69,80` | **PARTIAL.** `JS:1746,1749` `Stream.purge` returns `void`. The core decodes the count (`CO:1800-1807` `JsStreamPurgeResponse.purged`), but the shell drops it. | nats.go `Purge` returns only `error` (`ng:js/stream.go:516`); the count sits in a private struct (`ng:js/stream.go:207`) | none expose it | Cheap fix: return `purged` from the existing decode. |
| 6 | `getMessage(stream, MessageGetRequest)`: a generic request object (`forSequence`, `lastForSubject`, `firstForSubject`, `firstForStartTime`, `firstForStartTimeAndSubject`, `nextForSubject`) | `jn:JetStreamManagement.java:274`; `jn:api/MessageGetRequest.java:35-60` | **PARTIAL.** Fixed methods only: `JS:1770` `getMsg(seq)`, `JS:1777` `getMsg(seq, subject)`, `JS:1785` `getLastMsgForSubject`. No public request type. | Closest is nats.go's `GetMsg(seq, ...GetMsgOpt)` (`ng:js/stream.go:548`; `WithGetMsgSubject` `ng:js/jetstream_options.go:130`) | none | — |
| 7 | `getFirstMessage(stream, ZonedDateTime startTime)` and `getFirstMessage(stream, startTime, subject)` (`start_time` in the message-get request) | `jn:JetStreamManagement.java:309,323`; `jn:api/MessageGetRequest.java:50,55,108` | **MISSING.** `CO:1363-1368` `JsMsgGetRequest` has only `seq`, `last_for` and `next_for` | jnats-only for single get. nats.go `apiMsgGetRequest` has no start time (`ng:js/stream.go:259-263`). orbit has it only for batch get: `GetBatchStartTime` `ob:jetstreamext/getbatch.go:113` | none | Needs IR work: add a `start_time` field to `js_msg_get_request`. |
| 8 | `MessageInfo` extras: `getStream()`, `getLastSeq()`, `getNumPending()` (direct-get headers), `getStatus()`/`isStatus()`, and stripping of control headers | `jn:api/MessageInfo.java:84-120,188-245` | **PARTIAL.** `JS:572` `RawStreamMsg` has subject, sequence, header, data and time. For a direct get the raw header map keeps `Nats-Stream`, `Nats-Num-Pending` and `Nats-Last-Sequence` (`JS:1802`), so the values can be read by hand, but there are no typed fields and no stripping. | nats.go `RawStreamMsg` `ng:js/stream.go:162` has no such fields and keeps the headers (`ng:js/stream.go:630-685`) | none | NQ follows nats.go. Note that jnats' `numPending` is decremented by 1. |
| 9 | Direct-get header-name constants (`NATS_STREAM`, `NATS_SEQUENCE`, `NATS_TIMESTAMP`, `NATS_SUBJECT`, `NATS_LAST_SEQUENCE`, `NATS_NUM_PENDING`) and `DEFAULT_API_PREFIX` | `jn:support/NatsJetStreamConstants.java` (`NATS_STREAM`…`NATS_NUM_PENDING`, `DEFAULT_API_PREFIX`) | **MISSING.** Not in `JS:586-608`; only the expectation, TTL, rollup and schedule headers are public | `ng:js/message.go:270,273,276,279,292` (`StreamHeader`, `SequenceHeader`, `TimeStampHeaer`, `SubjectHeader`, `LastSequenceHeader`); `ng:js/api.go:39` `DefaultAPIPrefix` | none export them (only inside the generated cores) | The manifest is right to list these as planned. |
| 10 | `StreamInfo.getAlternates()` / `StreamAlternate` (name, domain, cluster) | `jn:api/StreamInfo.java:154`; `jn:api/StreamAlternate.java:29` | **MISSING.** `JS:7794` `StreamInfo` has no `alternates`, and the IR record has no such field (`ir/jetstream-json.nqir`) | Only the legacy `ng:jsm.go:1059,1063-1067`; the jetstream pkg `StreamInfo` (`ng:js/stream_config.go:26`) lacks it | none | — |
| 11 | `StreamState.getLostStreamData()` / `LostStreamData` (msgs, bytes) | `jn:api/StreamState.java:190`; `jn:api/LostStreamData.java:30` | **MISSING.** `JS:7769` `StreamState` | jnats-only (absent from both nats.go `StreamState`s: `ng:js/stream_config.go:250`, `ng:jsm.go:1083-1093`) | none | — |
| 12 | `SourceInfo`/`MirrorInfo` `getExternal()` and `getError()` | `jn:api/SourceInfoBase.java:88,106` | **PARTIAL.** `JS:7784` `StreamSourceInfo` has name, lag, seq, active, filterSubject and subjectTransforms, but no `external` or `error` | Legacy `ng:jsm.go:1075-1076`; the jetstream pkg `StreamSourceInfo` (`ng:js/stream_config.go:224`) lacks them | none | — |
| 13 | `ConsumerInfo.getCalculatedPending()` (numPending + delivered.consumerSeq) | `jn:api/ConsumerInfo.java:254` | **MISSING** (trivial to derive from `JS:7549`) | jnats-only | none | — |
| 14 | `PublishAck.getBatchId()` / `getBatchSize()` | `jn:api/PublishAck.java:111,119` | **PARTIAL.** `JS:7616` `PubAck` lacks them. `JS:6627` `BatchAck` has `batchId` and `batchSize`, returned by batch commits. | `ng:js/publish.go:138` `PubAck` lacks them; orbit `BatchAck` (`ob:jetstreamext/publishbatch.go`) | Same split in all targets | In practice equivalent: jnats has no batch publish API to fill these. |
| 15 | Message TTL forms: `messageTtlNever()` (`"never"`), `messageTtlCustom(String)`, `messageTtlSeconds(int)`, `MessageTtl` | `jn:PublishOptions.java:309,321,330,340`; `jn:MessageTtl.java:46,59,70` | **PARTIAL.** `JS:651` `withMsgTtl(long nanos)` writes a Go duration string only when > 0 (`ir/jetstream-publish.nqir:369-377`). `"never"` cannot be expressed; arbitrary custom strings cannot either. | `WithMsgTTL(time.Duration)` `ng:js/jetstream_options.go:622` (no never) | Same in all targets | Per-message TTL `"never"` is a nats-server feature that only jnats exposes. `withScheduleTtlNever` (`JS:703`) covers only schedules. |
| 16 | `PublishOptions.stream(name)` (deprecated): client-side check that the ack's stream matches | `jn:PublishOptions.java:96,221`; `jn:impl/NatsJetStream.java:189-195` | **MISSING** | jnats-only (deprecated) | none | Use `withExpectStream` (`JS:654`), which the server checks. |
| 17 | `PublishOptions.Builder(Properties)` (`PROP_STREAM_NAME`, `PROP_PUBLISH_TIMEOUT`), `clearExpected()` | `jn:PublishOptions.java:83-88,199,364` | **MISSING** | jnats-only | none | Options are varargs; `clearExpected` has no meaning there. |
| 18 | Public JSON (de)serialization of config/info: `StreamConfiguration.instance(json)` / `toJson()`, `ConsumerConfiguration.toJson()` / `Builder.json(String)`, `*Info(JsonValue)` constructors | `jn:api/StreamConfiguration.java:175,186`; `jn:api/ConsumerConfiguration.java:232,855,864` | **MISSING** from the public API. Encode and decode exist only internally (`fillJsStreamConfigFromStreamConfig`, `streamInfoFromJsStreamInfoResponse`, …). | `encoding/json` on the tagged structs (`ng:js/stream_config.go:58ff`, `ng:js/consumer_config.go:103ff`) | **Go has it:** `packages/go/nq.go/jetstream/jetstream.go:2981ff` (json tags) and enum `MarshalJSON` (`:2585-2970`). Python, TS, Rust, Ruby and C do not. | — |
| 19 | Enum wire/display names (`RetentionPolicy.toString()` → `"limits"`, `get(String)`; same for every policy enum) | `jn:api/RetentionPolicy.java` etc. | **PARTIAL.** Java enums (`JS:7374,7426,7446,7464,7480,7632,7648,7688,7710`) carry only nats.go's numeric `value`. There is no `String()`, `MarshalJSON` or `UnmarshalJSON` equivalent. | `ng:js/*.go` `String`, `MarshalJSON`, `UnmarshalJSON` | **Go has it** (`packages/go/nq.go/jetstream/jetstream.go:2585ff`) | The manifest correctly lists these as planned. |
| 20 | `getConsumerInfo(stream, name)` for any kind of consumer, from management or `StreamContext` | `jn:JetStreamManagement.java:193`; `jn:StreamContext.java:137` | **PARTIAL.** The kind-agnostic `JS:1325` `consumerInfo` is package-private. The public routes are kind-checked: `consumer()` (`JS:1185,1248`, refuses push via `js_consumer_kind`) and `pushConsumer()` (`JS:1302`, refuses pull), then `.info()` / `.cachedInfo()`. | Same split as nats.go: `ng:js/jetstream.go:930`, `ng:js/consumer.go:375` (`ErrNotPullConsumer`) | Same in all targets | Matches the oracle. jnats is more permissive. |
| 21 | Management calls by stream name, with no handle: `getMessage`, `deleteMessage`, `purgeStream`, `getConsumerNames`/`getConsumers`, `unpinConsumer`, `resetConsumer` | `jn:JetStreamManagement.java:102-372` | **PARTIAL (ergonomics/cost).** Purge, msg get/delete, consumer listing and unpin are only on a `Stream`, and `Context.stream(name)` costs a `STREAM.INFO` round trip (`JS:1099-1104`). Pause, resume, reset and delete-consumer are also on `Context` (`JS:1191-1218`). | Same as nats.go (`ng:js/stream.go:36-159`) | Same in all targets | jnats sends `STREAM.INFO` only for `getMessage` (cached allowDirect, `jn:impl/NatsJetStreamImpl.java:276`). |
| 22 | `StreamContext.getStreamName()` | `jn:StreamContext.java:34` | **PARTIAL.** `JS:1717-1720` `Stream` has no public name accessor; use `cachedInfo().config.name` | nats.go `Stream` has none either | same | Minor. |
| 23 | `JetStreamOptions.getPrefix()` (resolved `$JS.<domain>.API.`), `isDefaultPrefix()`, static `convertDomainToPrefix(domain)` | `jn:JetStreamOptions.java:74,82,231` | **PARTIAL.** `JS:107` `JetStreamOptions` reports `apiPrefix` and `domain` as given; the resolved prefix is not exposed | `ng:js/jetstream.go` `JetStreamOptions{APIPrefix, Domain}` | same | Minor. |
| 24 | `ApiResponse.getType()` (response `type`) | `jn:api/ApiResponse.java:208` | **MISSING** | jnats-only | none | Minor. |
| 25 | Default JS request timeout = the connection's connect timeout (2 s) | `jn:JetStreamOptions.java:34`; `jn:impl/NatsJetStreamImpl.java:60` | **DIFFERENT.** `JS:48` `DEFAULT_TIMEOUT` is 5 s (nats.go) | nats.go 5 s | same | Behavior difference, not a gap. |
| 26 | Atomic and fast-batch error-code constants: `JS_ATOMIC_PUBLISH_*` (10174-10201, 10210), `JS_BATCH_PUBLISH_*` (fast ingest 10205-10209, 10211), `JS_MIRROR_WITH_ATOMIC_PUBLISH` 10198, `JS_MIRROR_WITH_BATCH_PUBLISH` 10209 | `jn:support/NatsJetStreamConstants.java` (bottom block) | **PARTIAL / possibly wrong.** `JS:6358-6372` has orbit.go's set: `FAST_BATCH_NOT_ENABLED` 10203, `INVALID_PATTERN` 10204, `INVALID_ID` 10205, `UNKNOWN_ID` 10206. The two mirror-with-batch codes (10198, 10209) are missing. | `ob:jetstreamext/errors.go:24-42` (orbit's 10203-10206); nats.go `ScheduleSourceInvalid` 10203 and `ConsumerInvalidReset` 10204 (`ng:js/errors.go:81,83`) | All NQ targets copy orbit | jnats' 10205-10208 match the NASIR server model (`nasir/ir/natsserver/jsapi.nasir:5345-5411`, citing `stream.go:7785-7845`). orbit's 10203/10204 collide with nats.go's codes. Check against a live server. |
| 27 | Batch direct get (`MessageBatchGetRequest`, `multi_last`, `up_to_seq`, batch) | Not public in jnats 2.26.x: only `jn:support/ApiConstants.java:145` (`MULTI_LAST`) and the unused flag `jn:impl/NatsJetStreamImpl.java:49,67` | **MISSING** | `ob:jetstreamext/getbatch.go:128` (`GetBatch`), `:191` (`GetLastMsgsFor`), options `:77-175` | **None:** `packages/go/nq.go/JetStreamExt.md` says it is excluded | For completeness; the brief expected jnats to have it, but it does not. |
| 28 | Counters API | jnats has only `StreamConfiguration.allowMessageCounter` and `PublishAck.getVal` | Config flag `JS:7761` and `PubAck.value` `JS:7616` are present; no counter API | `ob:counters/counters.go:17-197` | none | Not a jnats gap; listed for context. |

## (c) PRESENT features (jnats → dev.nq)

**Context and options**
- `Connection.jetStream(jso)` / `jetStreamManagement(jso)` → `JetStream.create(nc[, Options], JetStreamOpt...)` `JS:56,59`
- `JetStreamOptions.prefix` → `createWithApiPrefix` `JS:62,65`
- `JetStreamOptions.domain` → `createWithDomain` `JS:70,73`
- `JetStreamOptions.requestTimeout` → `Options.defaultTimeout` `JS:101-104`, and per call `Context.withTimeout` `JS:1005`
- `getRequestTimeout` / `getPrefix` → `Context.options()` → `JetStreamOptions` `JS:107,999`
- `JetStream.getStreamContext(name)` → `Context.stream(name)` `JS:1099` (a `Stream` handle)
- `getConsumerContext(stream, name)` → `Context.consumer(stream, name)` `JS:1185` and `pushConsumer` `JS:1302`

**Account and streams**
- `getAccountStatistics` → `Context.accountInfo()` `JS:1059`, returning `AccountInfo` `JS:7336`
  - fields: memory, store, reservedMemory, reservedStore, streams, consumers, limits, domain, api, tiers
  - `AccountTier` → `Tier` `JS:7325`; `AccountLimits` → `JS:7305`; `ApiStats` (level, total, errors, inflight) → `APIStats` `JS:7317`
- `addStream` → `createStream` `JS:1067`
- `updateStream` → `updateStream` `JS:1070`. NQ also has `createOrUpdateStream` `JS:1073`, which jnats lacks.
- `deleteStream` → `deleteStream` `JS:1123` (void; errors are thrown)
- `getStreamInfo(name[, StreamInfoOptions])` → `stream(name).info([StreamInfoOptions])` `JS:1725,1732`, with `StreamInfoOptions{deletedDetails, subjectFilter}` `JS:132` and subject paging `JS:1914-1941`
  - `allSubjects()` = `subjectFilter=">"`
- `purgeStream(name[, PurgeOptions{subject, sequence, keep}])` → `Stream.purge([PurgeOptions])` `JS:1746,1749`, `PurgeOptions` `JS:144`
  - sequence and keep together are refused (`js_check_purge_option`), as in `jn:PurgeOptions.java:139`
  - the purged count is lost; see #5
- `getStreamNames()` / `getStreamNames(subjectFilter)` → `streamNames()` / `streamNames(subject)` `JS:1149,1152` (paged `Lister`)
- `getStreams()` / `getStreams(subjectFilter)` → `listStreams()` / `listStreams(subject)` `JS:1131,1134`
- jnats internal `lookupStreamBySubject` → public `streamNameBySubject` `JS:1107`

**Stored messages**
- `getMessage(stream, seq)` → `Stream.getMsg(seq)` `JS:1770`, returning `RawStreamMsg` `JS:572`
- `getNextMessage(stream, seq, subject)` → `Stream.getMsg(seq, subject)` `JS:1777` (`next_by_subj`)
- `getFirstMessage(stream, subject)` → `Stream.getMsg(1, subject)` `JS:1777` (composition)
- `getLastMessage(stream, subject)` → `Stream.getLastMsgForSubject` `JS:1785`
- Direct get (`DIRECT.GET.<s>` and `DIRECT.GET.<s>.<subj>` when allowDirect) → `JS:1792-1813` (`js_msg_get_direct`)
- `deleteMessage(stream, seq)` (jnats default erase=true, which overwrites) → `Stream.secureDeleteMsg` `JS:1821`
- `deleteMessage(stream, seq, false)` → `Stream.deleteMsg` `JS:1818` (no_erase)
  - The naming is inverted: jnats' default "delete" is NQ's "secure delete".

**Consumers**
- `addOrUpdateConsumer` → `createOrUpdateConsumer` `JS:1167` (also on `Stream` `JS:1833`)
- `createConsumer` → `JS:1173` / `JS:1836`
- `updateConsumer` → `JS:1179` / `JS:1839`
- Push variants → `createPushConsumer` / `createOrUpdatePushConsumer` / `updatePushConsumer` `JS:1284-1300`
- `deleteConsumer` → `JS:1191` / `JS:1845`
- `pauseConsumer(stream, name, ZonedDateTime)` → `pauseConsumer(stream, name, Time)` `JS:1197` / `JS:1863`, returning `ConsumerPauseResponse` `JS:7569` (paused, pauseUntil, pauseRemaining)
- `resumeConsumer` (boolean) → `resumeConsumer` `JS:1203` / `JS:1866`, returning `ConsumerPauseResponse`
- `unpinConsumer(stream, name, group)` → `Stream.unpinConsumer(consumer, group)` `JS:1875`
- `resetConsumer(stream, name[, seq])` (returns `ConsumerInfo`) → `resetConsumer` / `resetConsumerToSequence` `JS:1209,1215` / `JS:1869,1872`, returning `ConsumerResetResponse{consumerInfo, resetSeq}` `JS:7576`
- `getConsumerNames(stream)` → `Stream.consumerNames()` `JS:1896`
- `getConsumers(stream)` → `Stream.listConsumers()` `JS:1882`
- Unnamed consumer gets a generated name → IR names it from sha256(NUID) `JS:1229-1231`
- Create action (create, update, createOrUpdate) → `Core.JsConsumerAction` `JS:1167-1182`
- Consumer-create subject with filter (`CONSUMER.CREATE.s.n.f`) → IR `js_request_consumer_create`

**Publishing**
- `publish(subject, body)` → `Context.publish(subject, payload, opts...)` `JS:1008`
- `publish(subject, headers, body[, opts])` and `publish(Message[, opts])` → `publishMsg(Message, opts...)` `JS:1022`
- `publishAsync(...)` (all 6 overloads) → `publishAsync` / `publishMsgAsync` `JS:1046,1047`, returning `PubAckFuture.ok()` (`CompletableFuture<PubAck>`) `JS:814-831`
- NQ extras: `publishAsyncPending` `JS:1048`, `publishAsyncComplete` `JS:1050`, `cleanupPublisher` `JS:1051`, `withPublishAsyncMaxPending` / `withPublishAsyncTimeout` / `withPublishAsyncAckHandler` / `withPublishAsyncErrHandler` `JS:747-760`
- `PublishOptions` mapping:
  - `messageId` → `withMsgId` `JS:648`
  - `expectedStream` → `withExpectStream` `JS:654`
  - `expectedLastSequence` → `withExpectLastSequence` `JS:657`
  - `expectedLastSubjectSequence` → `withExpectLastSequencePerSubject` `JS:660`
  - `expectedLastSubjectSequenceSubject` (+ seq) → `withExpectLastSequenceForSubject(seq, subject)` `JS:665`
  - `expectedLastMsgId` → `withExpectLastMsgId` `JS:670`
  - `messageTtlSeconds(n)` → `withMsgTtl(n*1e9)` `JS:651`
  - `streamTimeout` → `Context.withTimeout(d)` `JS:1005`
- NQ extras: `withRetryWait`, `withRetryAttempts`, `withStallWait`, `withSchedule*` `JS:673-709`
- Header constants (`MSG_ID_HDR`, `EXPECTED_*`, `MSG_TTL_HDR`, `ROLLUP_HDR*`, `NATS_SCHEDULE_*`) → `JS:586-608`
- `PublishAck` (`getStream`, `getSeqno`, `isDuplicate`, `getDomain`, `getVal`) → `PubAck{stream, sequence, duplicate, domain, value}` `JS:7616`
- An error ack raises `JetStreamApiException` → `JetStreamException` with `apiError()` `JS:502-531`
- A no-responders status raises `IOException` in jnats → NQ retries per `js_publish_retry`, then throws `Kind.NO_STREAM_RESPONSE`

**StreamConfiguration** (every jnats builder field) → `StreamConfig` `JS:7726-7766`:
- name, description, subjects
- retentionPolicy → retention (`RetentionPolicy` `JS:7374`)
- compressionOption → compression (`StoreCompression` `JS:7688`)
- maxConsumers, maxMessages → maxMsgs, maxMessagesPerSubject → maxMsgsPerSubject, maxBytes, maxAge (nanos), maxMsgSize
- storageType → storage (`StorageType` `JS:7648`)
- replicas, noAck, templateOwner → template
- discardPolicy → discard (`DiscardPolicy` `JS:7632`)
- duplicateWindow → duplicates
- placement (`Placement{cluster, tags}` `JS:7368`)
- republish → rePublish (`RePublish` `JS:7625`)
- subjectTransform (`SubjectTransformConfig` `JS:7664`)
- consumerLimits (`StreamConsumerLimits` `JS:7704`)
- mirror / sources (`StreamSource` `JS:7676`, with `ExternalStream` `JS:7582`, `StreamConsumerSource` `JS:7670`, and `domain` converted to external `JS:2130-2149`)
- sealed / seal(), allowRollup, allowDirect, mirrorDirect, denyDelete, denyPurge, discardNewPerSubject, metadata
- firstSequence → firstSeq, subjectDeleteMarkerTtl, allowMessageTtl → allowMsgTtl, allowMessageSchedules → allowMsgSchedules, allowMessageCounter → allowMsgCounter, allowAtomicPublish, allowBatched → allowBatchPublish (JSON `allow_batched`)
- persistMode (`PersistModeType{DEFAULT, ASYNCHRONOUS}` `JS:7710`)

**StreamInfo / StreamState / ClusterInfo**
- `StreamInfo` (configuration, streamState, createTime, mirrorInfo, sourceInfos, clusterInfo, timestamp) → `StreamInfo` `JS:7794` (alternates missing, #10)
- `StreamState` (msgCount, byteCount, first/last seq and time, consumerCount, subjectCount, subjects/subjectMap, deletedCount, deleted) → `StreamState` `JS:7769` (lost missing, #11)
- `ClusterInfo` (name, raftGroup, leader, leaderSince, systemAccount, trafficAccount, replicas) → `ClusterInfo` `JS:7414`, plus `desired` (`DesiredClusterInfo` `JS:7405`)
- `Replica` / `PeerInfo` (name, current, offline, active, lag) → `PeerInfo` `JS:7350`, plus peer and pending
- `SourceInfo` / `MirrorInfo` (name, filterSubject, lag, active, subjectTransforms) → `StreamSourceInfo` `JS:7784` (external and error missing, #12)

**ConsumerConfiguration** (every jnats field) → `ConsumerConfig` `JS:7498-7533`:
- deliverPolicy (`DeliverPolicy` `JS:7426`), ackPolicy (`AckPolicy` incl. `FLOW_CONTROL` `JS:7446`), replayPolicy (`JS:7464`)
- description, durable, name, deliverSubject, deliverGroup, sampleFrequency
- startTime → optStartTime, startSequence → optStartSeq
- ackWait, idleHeartbeat, maxExpires → maxRequestExpires, inactiveThreshold, maxDeliver, rateLimit, maxAckPending
- maxPullWaiting → maxWaiting, maxBatch → maxRequestBatch, maxBytes → maxRequestMaxBytes
- numReplicas → replicas, pauseUntil, flowControl, headersOnly, memStorage → memoryStorage
- backoff → backOff, metadata, filterSubject(s), priorityGroups
- priorityPolicy (`PriorityPolicy` `JS:7480`: NONE, PINNED_CLIENT, OVERFLOW, PRIORITIZED)
- priorityTimeout → pinnedTtl (JSON `priority_timeout`)

**ConsumerInfo** (config, name, stream, created, delivered, ackFloor, numPending, numWaiting, numAckPending, redelivered, paused, pauseRemaining, cluster, pushBound, timestamp, priorityGroupStates) → `ConsumerInfo` `JS:7549`
- `SequenceInfo` → `JS:7535`; `PriorityGroupState` → `JS:7542`; `SequencePair` → `JS:2213`

**Errors**
- `JetStreamApiException` (`getErrorCode`, `getApiErrorCode`, `getErrorDescription`) → `JetStreamException.apiError()` → `APIError{code, errorCode, description}` `JS:466-531`, with sentinel `Kind` enum `JS:179` and `matches(kind)` `JS:545`
- jnats `Error.JsBadRequestErr` / `JsNoMessageFoundErr` and the `JS_CONSUMER_NOT_FOUND_ERR` / `JS_NO_MESSAGE_FOUND_ERR` / `JS_WRONG_LAST_SEQUENCE` / `JS_SEQUENCE_TEMPORARILY_UNKNOWN` constants → `ErrorCode` enum `JS:7812-7845` (`BAD_REQUEST`, `MESSAGE_NOT_FOUND`, `CONSUMER_NOT_FOUND`, `STREAM_WRONG_LAST_SEQUENCE`, `STREAM_WRONG_LAST_SEQUENCE_CONSTANT`=10164, …)
- Direct-get 404 status → `Kind.MSG_NOT_FOUND`
- `ServerInfo.isJetStreamAvailable` → `Client.connectedServerJetStream().enabled()` `CL:287`, plus `apiLevel`, which jnats lacks

**Atomic and fast batch** (jnats has only flags and constants; NQ implements orbit.go)
- `allowAtomicPublish` / `allowBatched` flags → present
- `NATS_BATCH_*` headers → `BATCH_ID_HEADER` / `BATCH_SEQ_HEADER` / `BATCH_COMMIT_HEADER` / `BATCH_COMMIT_EOB` `JS:6351-6354`
- `BatchPublisher` `JS:6701` with `newBatchPublisher` `JS:6690` and `publishMsgBatch` `JS:6838,6864`
- `FastPublisher` `JS:6991` with `newFastPublisher` `JS:6942`
- `BatchAck` `JS:6627`, `FastPubAck` `JS:6934`

## (d) Where the capability manifests disagree with the source

> **Correction:** this section overstates the staleness. Most of these entries stay `planned` correctly under nq.dev's evidence rule; only the reason text was stale. See [README §5.3](README.md#5-problems-found-in-nqdev-now-fixed).


1. **`ir/capabilities/jetstream/java.nio.json` and `java.threaded.json`** list 510 symbols as `planned` and 394 as `implemented`. Every `implemented` binding was checked and exists in `JetStream.java`/`Client.java`; none is overstated. Of the 311 in-scope `planned` symbols (excluding KV, ObjectStore and consume types), about 270 are in fact present. The plan's own rule explains this: "Struct types and fields stay `planned` in the contract … evidence … not yet expressible in the field rule" (`docs/JETSTREAM-PLAN.md:870-872`). The manifest therefore understates Java's coverage. Out-of-date entries, by group:
   - **Records and every field:**
     - `StreamConfig.*` (`JS:7726`), `StreamInfo.*`, `StreamState.*`, `StreamSourceInfo.*`, `StreamSource.*`
     - `ClusterInfo.*`, `PeerInfo.*`, `DesiredClusterInfo*`
     - `ConsumerConfig.*` (`JS:7498`), `ConsumerInfo.*`, `SequenceInfo.*`, `SequencePair.*`, `PriorityGroupState.*`
     - `ConsumerPauseResponse.*`, `ConsumerResetResponse.*`
     - `AccountInfo.*`, `Tier.*`, `AccountLimits.*`, `APIStats.*`
     - `PubAck.*`, `RawStreamMsg.*`, `RePublish.*`, `Placement.*`, `SubjectTransformConfig.*`, `StreamConsumerLimits.*`, `StreamConsumerSource.*`, `ExternalStream.*`, `StreamPurgeRequest.*`
     - `APIError` with `Code`, `ErrorCode`, `Description`, `Error` (`toString` `JS:476`) and `Is` (`JS:485`)
   - **Enums and constants:**
     - `AckPolicy`, `DeliverPolicy`, `ReplayPolicy`, `RetentionPolicy`, `DiscardPolicy`, `StorageType`, `StoreCompression`, `PersistModeType`, `PriorityPolicy`, and all of their constants (`LimitsPolicy`, `AckFlowControlPolicy`, `S2Compression`, `AsyncPersistMode`, `PriorityPolicyPinned`, …)
     - All 23 `JSErrCode*` → `ErrorCode` `JS:7812`
     - `ErrorCode`, `JetStreamError` → `JetStreamException`
   - **Options and functions:**
     - `WithDeletedDetails` / `WithSubjectFilter` → `StreamInfoOptions` `JS:132`
     - `WithPurgeSubject` / `WithPurgeSequence` / `WithPurgeKeep` → `PurgeOptions` `JS:144`
     - `WithStreamListSubject` → `listStreams(subject)` / `streamNames(subject)`
     - `WithGetMsgSubject` → `getMsg(seq, subject)`
     - `WithDefaultTimeout` / `WithClientTrace` / `ClientTrace.*` → `Options` and `ClientTrace` `JS:101,125`
     - `JetStreamOptions.*` → `JS:107`; `JetStream.Conn` / `JetStream.Options` → `conn()` / `options()` `JS:999-1002`
     - `ConsumerManager.PauseConsumer` / `ResumeConsumer` / `ResetConsumer` / `ResetConsumerToSequence` → `JS:1197-1218`
     - `StreamInfoLister` / `StreamNameLister` (`Info`, `Name`, `Err`) and `ConsumerInfoLister` / `ConsumerNameLister` → `Lister<T>` `JS:1949`
     - Type-level `JetStream`, `Stream`, `StreamManager`, `StreamConsumerManager`, `PubAckFuture`, `Publisher`, `PublishOpt`, `JetStreamOpt`, `MsgAckHandler`, `MsgErrHandler`, `StreamInfoOpt`, `StreamListOpt`, `StreamPurgeOpt`, `GetMsgOpt`
   - **Go-only `implemented`** among these (marked implemented in `go.native.json` but planned for Java although Java binds them): `APIError.Error`, `APIError.Is`, the four `ConsumerManager.*`, every `JSErrCode*`, `JetStream.Conn` / `JetStream.Options`, the lister methods, `WithClientTrace`, `WithDefaultTimeout`, `WithDeletedDetails`, `WithGetMsgSubject`, `WithPurge*`, `WithStreamListSubject`, `WithSubjectFilter`. These are the clearest candidates to flip to `implemented`, with bindings such as `JetStream.Context.pauseConsumer`, `JetStream.StreamInfoOptions.deletedDetails` and `JetStream.ErrorCode`.
   - **Correctly `planned`** (absent from the Java source, about 41):
     - `DefaultAPIPrefix`
     - `StreamHeader`, `SequenceHeader`, `TimeStampHeaer`, `SubjectHeader`, `LastSequenceHeader`
     - `ErrEndOfData`
     - the eight `MigrationStatus*` constants and `MigrationStatusType` (Java uses a plain `String` in `DesiredClusterInfoStatus.type`)
     - every enum's `String` / `MarshalJSON` / `UnmarshalJSON`
     - `ErrConsumerHasActiveSubscription` (out of scope; push consume)
2. **`ir/capabilities/orbit/java.nio.json` and `java.threaded.json`** list 40 symbols as planned.
   - **26 are out of date.** They are present in `JetStream.java`:
     - `BatchAck` and its `.Stream`, `.Sequence`, `.Domain`, `.Value`, `.BatchID`, `.BatchSize` (`JS:6627-6640`)
     - `BatchFlowControl` and its `.AckFirst`, `.AckEvery`, `.AckTimeout` (`JS:6589-6597`)
     - `BatchMsgOpt` (`JS:6543`), `BatchPublisher` (type, `JS:6701`), `BatchPublisherOpt` (`JS:6578`), `PublishMsgBatchOpt` (`JS:6581`)
     - `FastPubAck` and its `.BatchSequence`, `.AckSequence` (`JS:6934-6938`)
     - `FastPublishErrHandler` (`JS:6870`)
     - `FastPublishFlowControl` and its `.Flow`, `.MaxOutstandingAcks`, `.AckTimeout` (`JS:6896-6903`)
     - `FastPublisher` (type, `JS:6991`), `FastPublisherOpt` (`JS:6873`)
   - **14 are correctly planned:** the batch-get group (`GetBatch*`, `GetLastMsgs*`, `GetLastForOpt`, `ErrBatchUnsupported`, `ErrInvalidResponse`, `ErrNoMessages`, `ErrSubjectRequired`), which no NQ target implements.
3. **Other targets:** every non-Go JetStream manifest has the same 510/394 split, so the same out-of-date pattern applies. `go.native.json` has 458/446.
4. **Doc note:** `packages/java/JetStream.md:865-869` ("Not available yet") mentions only `OrderedConsumerConfig` and `PushConsumeOpt` types. It does not list the real gaps above (#1, #5, #7, #9-#13, #15, #18, #19, #27).
