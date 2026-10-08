# Gap analysis: jnats (2.26.4) KV / Object Store / Services / extras vs NQ Java (`dev.nq`)

Scope: `nats.java` main (CHANGELOG top = 2.26.4) vs `nq.dev/packages/java` (`dev.nq.JetStream`, `dev.nq.Micro`), oracles nats.go v1.54.0 (`jetstream/kv.go`, `jetstream/object.go`, `micro/`) and orbit.go (`jetstreamext`, `counters`, `kvcodec`).
Every NQ status below comes from reading the source. The manifests were not trusted (see section d).

Path abbreviations:
- **jn** = `nats.java/src/main/java/io/nats/`
- **JS** = `nq.dev/packages/java/src/main/java/dev/nq/JetStream.java`
- **MI** = `.../dev/nq/Micro.java`
- **ng** = `oracle/nats.go/`
- **ob** = `oracle/orbit.go/`

---

## (a) Summary

- **Key-Value: functionally complete.** NQ Java has every nats.go KeyValue operation:
  - get / getRevision, put / putString, create with KeyTTL, update, delete / purge with LastRevision and PurgeTTL
  - watch / watchAll / watchFiltered with every WatchOpt, including ResumeFromRevision
  - keys, listKeys / listKeysFiltered, history, purgeDeletes with DeleteMarkersOlderThan
  - status, create / update / createOrUpdate / delete bucket, and name and status listers
  - limitMarkerTtl, mirror / sources / republish, and the mirror and cross-domain read/write prefixes (`js_kv_map_handle`)

  It is wired in all 12 profiles (JETSTREAM-PLAN J6b "landed"). The only jnats gaps are jnats-only conveniences: `put(Number)`, `getValueAsLong`, `getDataLen` (Nats-Msg-Size), the `MessageTtl.never()` / custom TTL strings, a push-callback watcher plus `getConsumerNamePrefix`, the `KeyResult` queue with an exception slot, public `KeyValueEntry(Message)` constructors and flat status getters.
- **Object Store: functionally complete.**
  - put from a stream, bytes, string or file; get as a stream, bytes, string or file, with SHA-256 digest verification
  - getInfo with show-deleted, updateMeta (with rename), delete, addLink / addBucketLink, seal
  - watch (IGNORE_DELETE / INCLUDE_HISTORY / UPDATES_ONLY), list, status and the management calls

  Gaps are API-shape only: jnats returns `ObjectInfo` from `updateMeta` / `delete` and `ObjectStoreStatus` from `seal`, where NQ returns void (nats.go shape). jnats also has a push `ObjectStoreWatcher`, builder and `toJson` objects, a `Headers` type and `ObjectStoreStatus.getConfiguration()`.
- **Services: complete against nats.go micro** (manifest 117/121 implemented; the other 4 are justified `unsupported`). The real jnats-only gaps:
  1. The **`Discovery`** client (ping/info/stats across instances). NQ excluded it on purpose (SERVICES-PLAN.md:54, "nats.go has none").
  2. **Auto-reply with error 500 when a handler throws** (now fixed in `wallyqs/nq.dev` `fc1ef2c`; before, the exception stopped the service).
  3. **`drainTimeout`**, and `stop(drain=false)` / `stop(Throwable)` with a `CompletableFuture` from `startService()`.
  4. **Per-endpoint `Dispatcher`** and ping/info/stats dispatchers.
  5. **Per-endpoint `statsDataSupplier`**. NQ has a service-wide `StatsHandler(endpoint)`, so this is partial.
  6. JSON-parseable `PingResponse` / `InfoResponse` / `StatsResponse` (only needed for Discovery) and a `getPingResponse()` accessor.

  `SchemaResponse` no longer exists in jnats; `schemaDispatcher` is a deprecated no-op (jn/service/ServiceBuilder.java:149).
- **Public support utilities are jnats-only. NQ Java exposes none of them publicly:**
  - `NKey`: create, sign, verify
  - `NUID`
  - `JsonUtils`, `JsonValue`, `JsonSerializable`
  - `Digester`
  - `ServerVersion` comparisons
  - `Validator`
  - `Encoding`: base32/64 and JSON escaping

  In the Go ecosystem NKey and NUID live in separate libraries (`nkeys`, `nuid`). NQ keeps these capabilities internal (compiled `nkeys.nqir`, `nuid.nqir`, JSON IR). Only `newInbox` / `defaultInbox`, `connectedServerVersion()` and `getObjectDigestValue` / `decodeObjectDigest` are public. No other NQ target exposes them either.
- **Counters / scheduling / atomic batch:**
  - **Counters:** jnats has only the stream flag `allowMessageCounter` and `PublishAck.getVal()`. NQ Java has the same (`StreamConfig.allowMsgCounter`, `PubAck.value`, `BatchAck.value`). Neither has orbit.go's `counters` API. **No NQ target has `counters` or `kvcodec`**, and neither is in `ir/orbit-contract.json`.
  - **Scheduling:** jnats has header constants plus the `allowMessageSchedules` flag only. NQ Java has typed `withSchedule*` publish options.
  - **Atomic batch and fast ingest:** jnats has constants and flags only (2.23.0 "Support Batch Publish" and 2.26.3 "ADR 50 Fast Ingest with constants"; no `Nats-Batch-*` header is used outside `NatsJetStreamConstants`). NQ Java has the full orbit.go `BatchPublisher` / `PublishMsgBatch` / `FastPublisher`. **This is the main area where NQ is ahead of jnats.**
- **Manifests are stale for KV/OS:** all 71 KV/OS `planned` rows in `jetstream/java.nio.json` are types or fields that exist in JetStream.java. The policy is stated in packages/java/JetStream.md:865-869: types and config fields stay "planned". Of the orbit manifest's 40 `planned` rows, 22 are batch/fast types or fields that exist; 18 are batch-get rows that are genuinely missing (see d).

---

## (b) MISSING and PARTIAL features

### b.1 Key-Value

| jnats feature | jnats ref | NQ Java status + ref | oracle ref | other NQ targets | notes |
|---|---|---|---|---|---|
| `put(String key, Number value)` | jn/client/KeyValue.java:92; impl NatsKeyValue.java:154 | **MISSING** (only `put(byte[])` JS:4945, `putString` JS:4951) | jnats-only | none | Trivial facade: `putString(key, n.toString())` |
| `update(key, String value, rev)` | KeyValue.java:145 | **PARTIAL**: `update(key, byte[], rev)` only (JS:4973) | ng/jetstream/kv.go:1117 (bytes only) | same everywhere | Facade overload |
| `create(key, value, MessageTtl)` and `purge(key[, rev], MessageTtl)` with `MessageTtl.seconds / custom / never` | KeyValue.java:119,193,204; client/MessageTtl.java:46,59,70 | **PARTIAL**: `keyTtl(long ns)` JS:4411 and `purgeTtl(long ns)` JS:4401 send a duration only. There is no `"never"` or custom Nats-TTL string for a message TTL (`withMsgTtl(long)` JS:651; only `withScheduleTtlNever` JS:703 exists, for schedules) | ng kv_options.go:110 (PurgeTTL), :126 (KeyTTL), both `time.Duration` | none has msg-TTL "never" | `MessageTtl.never()` / `custom()` is jnats-only (ADR-43 allows "never"). jnats `seconds()` requires at least 1 s |
| `KeyValueEntry.getValueAsString()` / `getValueAsLong()` | api/KeyValueEntry.java:105,115 | **MISSING** (`value()` bytes JS:4447) | jnats-only | none | Facade helpers |
| `KeyValueEntry.getDataLen()` (taken from the `Nats-Msg-Size` header under META_ONLY) | api/KeyValueEntry.java:123 | **MISSING**: the entry has no size field, and the header is not surfaced with metaOnly (JS:4428-4461) | jnats-only (nats.go kve has no size) | none | Needs the header kept on the entry, or a shell read of `Nats-Msg-Size` |
| Public `KeyValueEntry(MessageInfo)` / `KeyValueEntry(Message)` constructors (decode a raw message as a KV entry) | api/KeyValueEntry.java:45,63 | **MISSING**: the constructor is package-private (JS:4437); decoding is internal `js_kv_entry_op` (JS:4922) | jnats-only | none | |
| `KeyValueEntry.getCreated()` as `ZonedDateTime` | api/KeyValueEntry.java:132 | PARTIAL (shape): `created()` returns `JetStream.Time` (JS:4456), with `Time.toInstant()` JS:169 | ng kv.go:1002 (`time.Time`) | n/a | Conversion only |
| Push-style `KeyValueWatcher` (`watch(T)`, `endOfData()` callbacks) | api/Watcher.java:28,34; KeyValue.java:217-283 | **PARTIAL (shape)**: NQ is pull-style. `KeyWatcher.updates()` returns `Updates<KeyValueEntry>` (JS:5193), and a `null` entry marks the end of data | ng kv.go:351 (channel) | all NQ targets follow the nats.go channel model | The facade needs an adapter thread that pumps `updates()` into callbacks |
| `Watcher.getConsumerNamePrefix()` (names the watch's ordered consumer) | api/Watcher.java:43; impl NatsWatchSubscription.java:59 | **MISSING** (the consumer config comes from `js_kv_watch_consumer`, JS:5037) | jnats-only | none | |
| `watch(..., long fromRevision, ...)` overloads | KeyValue.java:231,258,283 | PRESENT via `resumeFromRevision(rev)` JS:4381 | ng kv_options.go:70 | all | Shape only |
| Watch handle `NatsKeyValueWatchSubscription` implements `AutoCloseable` (`unsubscribe` / `close`) | impl/NatsWatchSubscription.java:27,92,103 | PARTIAL: `KeyWatcher.stop()` (JS:5196) is not `AutoCloseable` | ng kv.go:1235 `Stop()` | n/a | Minor |
| `keys(String filter)` / `keys(List<String> filters)` returning `List<String>` | KeyValue.java:305,317 | **PARTIAL**: `keys(WatchOpt...)` lists every key (JS:5058). Filtered listing exists only as the streaming `listKeysFiltered(String...)` (JS:5086) | ng kv.go:1402 (Keys), :1461 (ListKeysFiltered) | same in all | Facade: drain `listKeysFiltered(...).keys()` into a list and sort |
| `consumeKeys([filter / filters])` returning `LinkedBlockingQueue<KeyResult>`, with an exception slot and a done marker | KeyValue.java:324-342; api/KeyResult.java:28-89 | **PARTIAL**: `listKeys` / `listKeysFiltered` return a `KeyLister` (JS:5145) with `Updates<String>`; the channel closes at the end. No per-item exception: errors are thrown at creation or silently end the stream | ng kv.go:285, :1432, :1461 | same in all | The facade can wrap it and convert the end into `KeyResult()` done |
| `KeyValueStatus` flat getters (`getDescription`, `getMaxBucketSize`, `getMaxValueSize` / `getMaximumValueSize`, `getStorageType`, `getReplicas`, `getPlacement`, `getRepublish`, `getEntryCount`, `getByteCount`, `getMaxHistoryPerKey`, `getTtl`, `getLimitMarkerTtl`) | api/KeyValueStatus.java:44-200 | PARTIAL: `KeyValueBucketStatus` (JS:4812) has `bucket / values / history / ttl / backingStore / streamInfo / bytes / isCompressed / limitMarkerTtl / metadata / config()`. The rest are reachable through `config()` (JS:4849) or `streamInfo()` | ng kv.go:311, :817-868 | same in all | Only `description` / maxBytes / storage / replicas / placement / republish need a `config()` hop |
| `KeyValueManagement.create / update` return `KeyValueStatus`; `getBucketInfo` (deprecated) / `getStatus(name)` / `getStatuses()` / `getBucketNames()` as Lists | KeyValueManagement.java:35-95 | PARTIAL (shape): `createKeyValue / updateKeyValue` return a `KeyValue` handle (JS:1365,1368). Statuses come from the `keyValueStores()` lister (JS:1382), names from the `keyValueStoreNames()` lister (JS:1379), a single status from `keyValue(b).status()` | ng kv.go:534,575,749,780 | same | Facade: drain listers into Lists |
| `KeyValueOptions` per bucket handle (`jsPrefix`, `jsDomain`, `jsRequestTimeout`, `JetStreamOptions`) | client/KeyValueOptions.java; client/FeatureOptions.java:67-102 | PRESENT via context: `JetStream.createWithDomain` / `createWithApiPrefix` (JS:62-75) and `Context.withTimeout(Duration)` (JS:1005) | ng jetstream.go (`NewWithDomain` / `NewWithAPIPrefix`) | all | Shape: per-context, not per-bucket |

### b.2 Object Store

| jnats feature | jnats ref | NQ Java status + ref | oracle ref | other NQ targets | notes |
|---|---|---|---|---|---|
| `put(String objectName, InputStream)` | ObjectStore.java:57 | PARTIAL: `put(ObjectMeta, InputStream)` (JS:5844); the caller builds an `ObjectMeta` with `name` | ng object.go `Put(ctx, meta, reader)` | same | Facade overload |
| `put(File)` uses `file.getName()` as the object name | ObjectStore.java:78 | PARTIAL: `putFile(String path)` uses the **full path** as the name (JS:5976-5984), as nats.go `PutFile` does | ng object.go `PutFile` | same | **Behavioral difference** in the stored name. A facade must pass `ObjectMeta(name=file.getName())` + `FileInputStream` |
| `get(String, OutputStream)` returning `ObjectInfo` | ObjectStore.java:90 | PARTIAL (shape): `get()` returns `ObjectResult extends InputStream` (JS:5725, 5993) with `info()`. `getBytes` / `getString` / `getFile` (JS:6056-6068). Digest verified (`DIGEST_MISMATCH`, JS:5721) | ng object.go `Get` / `GetBytes` / `GetFile` | same | Facade: `result.transferTo(out); return result.info()` |
| `updateMeta` / `delete` return `ObjectInfo` | ObjectStore.java:119,128 | PARTIAL: both return void (JS:6117, 6141) | ng object.go `UpdateMeta` / `Delete` (error only) | same | Facade can re-read with `getInfo(name, showDeleted)` |
| `seal()` returns `ObjectStoreStatus` | ObjectStore.java:156 | PARTIAL: void (JS:6237) | ng object.go `Seal` (error only) | same | Facade: `seal(); return status()` |
| `getList()` | ObjectStore.java:165 | PRESENT as `list(ListObjectsOpt...)` (JS:6291). Note: NQ throws `NO_OBJECTS_FOUND` on an empty store (`js_obj_found`) | ng object.go `List` | same | jnats returns an empty list; a facade must catch `NO_OBJECTS_FOUND` |
| Push `ObjectStoreWatcher` (`watch` / `endOfData`) + `NatsObjectStoreWatchSubscription` | api/ObjectStoreWatcher.java; ObjectStore.java:176 | PARTIAL (shape): `ObjectWatcher.updates()` (JS:5801-5811), pull with a `null` end marker. The options are KV `WatchOpt` (`ignoreDeletes` / `includeHistory` / `updatesOnly`, JS:6248) | ng object.go:250 | same | `ObjectStoreWatchOption` maps 1:1 |
| `ObjectStoreStatus.getConfiguration()` / `getMaxBucketSize()` / `getPlacement()` | api/ObjectStoreStatus.java:71,87,132 | PARTIAL: `ObjectBucketStatus` (JS:5645-5682) has no `config()` and no maxBytes / placement accessors; they are only reachable through `streamInfo().config` | ng object.go:1398-1428 (no Config either) | same | jnats-only accessors |
| `ObjectInfo` / `ObjectMeta` / `ObjectMetaOptions` / `ObjectLink` builders, `toJson()`, `equals`, the `ObjectInfo(Message)` constructor, `ObjectLink.bucket() / object()` factories, `isLink()`, `Headers` type | api/ObjectInfo.java:56-495; ObjectMeta.java:68-306; ObjectLink.java:52-130 | PARTIAL: plain mutable POJOs (JS:5458, 7588-7614). `headers` is a `Map<String,List<String>>`. JSON encode / decode is package-private (`encodeObjectInfo` JS:5686, `decodeObjectInfo` JS:5697); `isLink` is package-private (JS:5531) | ng object.go:354-429 (plain structs) | same | Facade |
| `ObjectStoreManagement.getStatuses()` / `getBucketNames()` / `getStatus(name)` as Lists | ObjectStoreManagement.java:45-64 | PARTIAL (shape): `objectStores()` / `objectStoreNames()` listers (JS:1423,1426) and `objectStore(b).status()` | ng object.go:1536,1569 | same | |

### b.3 Services (`io.nats.service`)

| jnats feature | jnats ref | NQ Java status + ref | oracle ref | other NQ targets | notes |
|---|---|---|---|---|---|
| **`Discovery`**: `ping / info / stats` (all, by name, by name+id) with `maxTimeMillis`, `maxResults`, `setInboxSupplier`; discoverMany | jn/service/Discovery.java:35-173 | **MISSING** (only `Micro.controlSubject(verb, name, id)` MI:185) | **jnats-only**. nats.go micro has no client discovery; SERVICES-PLAN.md:54 excludes it ("the nats CLI does this") | none | Facade: request-many over `controlSubject` with a timer / max count, then decode JSON. jnats 2.26.3 fixed no-responders handling here (#1620) |
| `PingResponse` / `InfoResponse` / `StatsResponse` / `EndpointStats` / `Endpoint`: JSON construct, `toJson`, `serialize`, `equals` | service/PingResponse.java, InfoResponse.java:34-105, StatsResponse.java:66-126, EndpointStats.java:73-246, ServiceResponse.java:33-163 | PARTIAL: POJOs `Ping` / `Info` / `Stats` / `EndpointStats` / `EndpointInfo` (MI:193-236). No public JSON decode; encoding is compiled (`svc_encode_*` in `handleVerb`, MI:595-603) and private | ng micro/service.go:114-160 (structs with json tags) | same | Needed only for Discovery |
| `Service.getPingResponse()` | service/Service.java:413 | **MISSING** (only `info()` MI:739 and `stats()` MI:761) | jnats-only (nats.go Service has no Ping()) | none | Trivial: build from the identity in `info()` |
| Handler exception produces an automatic `respondStandardError(t.toString(), 500)` and counts the error | service/EndpointContext.java:74-97 | **MISSING**: `Service.request()` (MI:574-588) records timing only. A thrown handler exception is not answered, and not counted unless the handler called `error()` | jnats-only (nats.go: a panic is not recovered) | none | **Behavioral**: a facade must wrap handlers in try/catch and call `req.error("500", t.toString(), null)` |
| `startService()` returns `CompletableFuture<Boolean>`, completed on stop (exceptionally with `stop(Throwable)`); `isStarted()`, `isStarted(timeout)` | service/Service.java:205,245,262,378,388 | PARTIAL: `addService` starts at once (MI:425); `stopped()` (MI:726); `Config.doneHandler` (MI:271, 307). No future and no error-cause completion | ng micro/service.go:325 (AddService), :711 Stop, :822 Stopped, DoneHandler | same in all | Facade: a future completed from `doneHandler` |
| `stop(boolean drain)`: a non-draining stop | Service.java:253 | **PARTIAL**: `stop()` always drains (MI:693) | ng micro/service.go:711 (always drains) | same | |
| `drainTimeout(Duration / millis)`, `getDrainTimeout()` | ServiceBuilder.java:108,118; Service.java:405 | **MISSING**: `subscription.drain()` has no service-level timeout (MI:697-713) | jnats-only | none | |
| Per-endpoint `Dispatcher` (`ServiceEndpoint.Builder.dispatcher`) and `ping / info / statsDispatcher` | ServiceEndpoint.java:220; ServiceBuilder.java:128,138,159 | **MISSING**: endpoints run on the client's subscription callback path; there is one internal service `Dispatcher` for done and error callbacks only (MI:479-510) | jnats-only | none | Threading-model knob |
| `ServiceEndpoint.Builder.statsDataSupplier(Supplier<JsonValue>)` per endpoint | ServiceEndpoint.java:230; EndpointStats.getData / getDataAsJson :200,208 | PARTIAL: one service-wide `Config.statsHandler` with `StatsHandler.stats(Endpoint)` (MI:269, 313), marshalled by `marshal(Object)` (MI:899). `EndpointStats.data` is a `byte[]` (MI:224) | ng micro/service.go `StatsHandler` | same | Facade: dispatch on `endpoint.name` |
| `EndpointStats.getStarted()` (per-endpoint start time) | EndpointStats.java:216 | **MISSING** (`Stats.started` exists at service level only, MI:234) | jnats-only (nats.go EndpointStats has no started; ng micro/service.go:114-124) | none | |
| `Service.getEndpointStats(String name)` | Service.java:443 | PARTIAL: filter `stats().endpoints` | jnats-only | n/a | |
| `Service.getId / getName / getVersion / getDescription` | Service.java:346-370 | PARTIAL: through `info()` (MI:739); `state` is package-private | ng micro `Info()` | same | |
| `ServiceMessage.respond(conn, JsonSerializable [, Headers])` | ServiceMessage.java:68,98 | PARTIAL: `respondJson(Object, RespondOpt...)` (MI:847) has its own reflective-free marshaller (Map / Collection / primitives / byte[]). It has no `JsonSerializable` interface | ng micro/request.go:122 | same | Facade: `respond(bytes(obj.toJson()))` |
| `ServiceBuilder`-style construction (build, then start; `addServiceEndpoints` before or after start) | ServiceBuilder.java:41-168; Service.java:106,116 | PRESENT in nats.go shape: `Micro.addService(nc, Config)` + `svc.addEndpoint` / `addGroup` at any time (MI:425, 533, 542) | ng micro/service.go:325,403,486 | all | Shape only |

### b.4 Support utilities and other public extras

| jnats feature | jnats ref | NQ Java status + ref | oracle ref | other NQ targets | notes |
|---|---|---|---|---|---|
| `NKey`: `createUser / Account / Cluster / Operator / Server`, `fromSeed`, `fromPublicKey`, `sign`, `verify`, `getPublicKey`, `isValidPublic*Key` | client/NKey.java:102-500+ | **MISSING (public)**. Internals: `Jwt.Keys` / `Jwt.signer` (Jwt.java:52-140, package-private), `Credentials.nkeyFromSeedFile` (Credentials.java:67, package-private), compiled `ir/nkeys.nqir`. Public use is limited to `Options` nkey / creds auth and `Jwt.encodeUserClaims(..., seed)` (Jwt.java:180) | Go: separate `nkeys` lib (not nats.go) | none exposes it (Go / Py / TS only have `NkeyOptionFromSeed`-style auth helpers) | |
| `NUID` (`nextGlobal`, instance `next`) | client/NUID.java | **PARTIAL**: `Client.newInbox()` (Client.java:110), `Client.defaultInbox()` (Client.java:531), compiled `nuid.nqir`. No public NUID generator | Go: separate `nuid` lib | none | |
| `JsonUtils`, `JsonValue`, `JsonValueUtils`, `JsonParser`, `JsonSerializable` | client/support/*.java | **MISSING (public)**: JSON is compiled IR, internal | jnats-only | none | |
| `Digester` (SHA-256 / base64url digest entries, `matches`) | client/support/Digester.java:24-99 | PARTIAL: `JetStream.getObjectDigestValue(MessageDigest)` (JS:5496) and `decodeObjectDigest(String)` (JS:5513) are public | ng object.go:817,823 | all (J7) | |
| `ServerVersion` (compare, `isNewer / isOlder / isSame...`) | client/support/ServerVersion.java:16-110 | **MISSING (public)**: `Client.connectedServerVersion()` (Client.java:248) returns the string only. Version gating is compiled | jnats-only (nats.go has unexported `serverMinVersion`) | none | |
| `Validator` (`validateSubject`, stream / consumer / durable / KV key / prefix checks) | client/support/Validator.java:29-283 | **MISSING (public)**: validation is compiled into calls (e.g. `checkKey` JS:4568, `kvNames` JS:4559) | jnats-only | none | |
| `Encoding` (base64 / base64url / base32, `jsonEncode / Decode`, `uriDecode`) | client/support/Encoding.java:22-417 | MISSING (public) | jnats-only | none | |
| Counter helper | jnats: only `StreamConfiguration.allowMessageCounter` (api/StreamConfiguration.java:553,1232) + `PublishAck.getVal()` (api/PublishAck.java:102) | PRESENT at the same level: `StreamConfig.allowMsgCounter` (JS:7761), `PubAck.value` (JS:7621), `BatchAck.value` (JS:6632). **No orbit `counters` API** (`Counter.Add / AddInt / Load / Get / GetMultiple`, `CounterSources`) | ob/counters/counters.go:17-261 | **no NQ target** (not in `ir/orbit-contract.json`) | Both are at parity; both are behind orbit.go. A facade wanting `Add` must set `Nats-Incr` itself (no NQ constant) |
| orbit `kvcodec` (key / value codecs wrapping a KeyValue) | n/a in jnats | MISSING | ob/kvcodec/kv.go:57-309 | none | Not a jnats feature; listed for completeness |
| Batch direct get (`GetBatch`, `GetLastMsgsFor` + options, `ErrBatchUnsupported / ErrNoMessages / ErrSubjectRequired / ErrInvalidResponse`) | jnats: none public (only `directBatchGet211Available` flag, impl/NatsJetStreamImpl.java:49,67) | MISSING (`Kind.INVALID_BATCH_OPTION` exists for `ErrInvalidOption`) | ob/jetstreamext/getbatch.go:34-191 | none (manifest says "outside J8") | Not a jnats public feature either |

---

## (c) PRESENT features (one-liners, NQ Java ref, oracle)

**Key-Value** (all 12 NQ profiles, J6b landed):
- Bucket lookup / create / update / createOrUpdate / delete: `Context.keyValue` / `createKeyValue` / `updateKeyValue` / `createOrUpdateKeyValue` / `deleteKeyValue`, JS:1358-1376 (ng kv.go:509-734). `createOrUpdate` is not in jnats.
- Bucket names / statuses listers: JS:1379,1382, classes JS:4741,4755 (ng kv.go:749,780).
- `KeyValueConfig` with every jnats field: bucket, description, maxValueSize, history, ttl, maxBytes, storage, replicas, placement, rePublish, mirror, sources, compression, limitMarkerTtl, metadata. JS:4339-4355 (ng kv.go:213-277). Stream derivation in `prepareKeyValueConfig`, JS:4674.
- Mirror / sources / external-domain read and write prefixes, from `mapStreamToKVS` + `js_kv_map_handle` (JS:4871-4881); direct get when `allowDirect` (JS:1795).
- `get` / `getRevision` (JS:4939,4942); a marker counts as KEY_NOT_FOUND.
- `put` / `putString` (JS:4945,4951).
- `create` with `keyTtl`; it replaces a delete or purge marker at its revision (the jnats #1356 revision guard). JS:4958 (ng kv.go:1062).
- `update(key, value, rev)` (JS:4973).
- `delete` / `purge` with `lastRevision(rev)` and `purgeTtl(ns)` (JS:4991-4994, 4398-4401) (ng kv_options.go:98,110).
- `KeyValueEntry`: bucket, key, value, revision, created, delta, operation (JS:4428-4461); KV-Operation / Nats-Marker-Reason mapping (`js_kv_entry_op`, ir/jetstream-kv.nqir:558-585; jnats #1323).
- `KeyValueOp` PUT / DELETE / PURGE (JS:4302-4326); constants `KEY_VALUE_MAX_HISTORY`, `ALL_KEYS`, `MARKER_REASON_HEADER` (JS:4329-4333).
- `watch` / `watchAll` / `watchFiltered` (multi-key) (JS:5020-5055); options `includeHistory` / `updatesOnly` / `ignoreDeletes` / `metaOnly` / `resumeFromRevision` (JS:4369-4381), which cover all four jnats `KeyValueWatchOption`s plus fromRevision.
- `keys()` sorted (JS:5058); `listKeys` / `listKeysFiltered` (JS:5083,5086); `history(key)` (JS:5091).
- `purgeDeletes(deleteMarkersOlderThan(ns))`, default 30 min, negative = all (JS:5111, 4425; ir/jetstream-kv.nqir:1427) (ng kv.go:1528, :1525).
- Bucket status (`KeyValueBucketStatus` JS:4812-4868, including `config()` readback and `limitMarkerTtl`).
- KV error sentinels (`Kind.*`, e.g. KEY_NOT_FOUND, KEY_EXISTS, KEY_REVISION_MISMATCH (via kvMapError JS:4471), BUCKET_NOT_FOUND, BAD_BUCKET, LIMIT_MARKER_TTL_NOT_SUPPORTED).

**Object Store** (all 12 profiles, J7b landed):
- Store lookup / create / update / createOrUpdate / delete: JS:1385-1413 (ng object.go:491-619). `update` and `createOrUpdate` are not in jnats.
- Store names / statuses listers: JS:1423,1426, classes JS:5574,5588.
- `ObjectStoreConfig` (bucket, description, ttl, maxBytes, storage, replicas, placement, compression, metadata), JS:5445.
- `put(ObjectMeta, InputStream)` with chunking (default 128 KiB, `ObjectMetaOptions.chunkSize`), SHA-256 digest, purge of the previous upload's chunks (jnats #1491). JS:5844.
- `putBytes` / `putString` / `putFile` (JS:5966-5976).
- `get` returning a streaming `ObjectResult`, digest verified, links followed; `getBytes` / `getString` / `getFile` (JS:5993-6068); `getObjectShowDeleted` (JS:5482).
- `getInfo(name, getObjectInfoShowDeleted())` (JS:6093, 5485).
- `updateMeta` with rename (JS:6117); `delete` (JS:6141).
- `addLink` / `addBucketLink`, with no-link-to-link / deleted checks (JS:6173,6199).
- `seal` (JS:6237); `watch(ignoreDeletes / includeHistory / updatesOnly)` (JS:6248); `list(listObjectsShowDeleted())` (JS:6291).
- `status()` / `ObjectBucketStatus`: sealed, size, ttl, storage, replicas, description, metadata, isCompressed, backingStore (JS:5645-5682, 6309).
- `ObjectMeta` / `ObjectInfo` / `ObjectLink` / `ObjectMetaOptions` with metadata (JS:5458, 7588-7614; jnats #1399 / #1418 parity).
- Digest helpers `getObjectDigestValue` / `decodeObjectDigest` (JS:5496,5513).

**Services** (manifest 117/121 implemented; all targets 111-121 implemented):
- `addService(Client, Config)` with name / version (SemVer) / description / metadata validation and a unique id (MI:425).
- Endpoints with subject, metadata, queue group or disabled queue group, pending limits (MI:356-379, 533).
- Nested groups with their own queue group (MI:542, 796-818).
- PING / INFO / STATS monitoring on all 9 `$SRV` subjects (MI:450-469).
- `info()`, `stats()` with processing / average time, errors, lastError, custom data (MI:739, 761).
- `reset()` (MI:788); `stop()` drains (MI:693); `stopped()` (MI:726).
- `Request.respond` / `respondJson` / `error(code, desc, data)` with `withHeaders` (MI:831-863); `Nats-Service-Error(-Code)` constants (MI:43-44).
- Request getters: data / headers / subject / reply (MI:880-889).
- `controlSubject()` (MI:185); response type constants (MI:46-48); `DEFAULT_QUEUE_GROUP = "q"` (MI:37).

**Other**: JetStream domain / API-prefix contexts (JS:62-75); `Client.connectedServerVersion()` (Client.java:248); `Client.newInbox()` (Client.java:110); stream flags `allowMsgTtl`, `subjectDeleteMarkerTtl`, `allowMsgCounter`, `allowAtomicPublish`, `allowMsgSchedules`, `allowBatchPublish`, `persistMode` (JS:7759-7765).

### Reverse: notable NQ Java features that jnats lacks (facade-relevant)
- **orbit.go atomic batch publishing**: `newBatchPublisher(js, BatchFlowControl)` (JS:6690); `BatchPublisher.add / addMsg / commit / commitMsg / discard / close / size / isClosed` (JS:6701, 6742-6783); `publishMsgBatch` (JS:6838,6864); `BatchMsgOpt` `withBatchMsgTTL` / `withBatchExpect*` (JS:6543-6566); `BatchAck` with value / batchId / batchSize (JS:6627); `BatchContext` timeout (JS:6459-6473). Oracle: ob/jetstreamext/publishbatch.go:34-422. **jnats has only `Nats-Batch-*` constants** (support/NatsJetStreamConstants.java:153-157).
- **orbit.go fast ingest**: `newFastPublisher` (JS:6942), `FastPublisher` (JS:6991; add / addMsg / commit / commitMsg / close / isClosed), `FastPublishFlowControl` (JS:6896), `withFastPublisherContinueOnGap` / `ErrorHandler` (JS:6916,6925), `FastPubAck` (JS:6934). Oracle: ob/jetstreamext/fastpublish.go:35-275. **jnats has only `$FI` op constants** (NatsJetStreamConstants.java:167-176; CHANGELOG 2.26.3 "ADR 50 Fast Ingest with constants").
- **Message scheduling publish options**: `withScheduleAt(Time / Instant)`, `withScheduleEvery`, `withScheduleCron`, `withScheduleTarget`, `withScheduleSource`, `withScheduleTtl`, `withScheduleTtlNever`, `withScheduleTimeZone`, `withScheduleRollup` + header constants + `@yearly` ... `@hourly` (JS:596-711) (ng jetstream_options.go, e.g. :811). **jnats has header constants + the stream flag only** (NatsJetStreamConstants.java:143-151; StreamConfiguration.java:1213).
- **Batch / fast / schedule error sentinels** (`Kind.BATCH_*`, `FAST_BATCH_*`, `SCHEDULE_*`, JS:208-220, 393-405).
- KV `createOrUpdateKeyValue`; OS `updateObjectStore` / `createOrUpdateObjectStore`; OS `getBytes` / `getString` / `getFile`.
- Services: `Config.errorHandler` / `doneHandler`, service- and group-level queue group and `queueGroupDisabled`, `withEndpointPendingLimits`, the default endpoint in `Config`, typed `MicroException.Kind` sentinels (MI:85-137). jnats `Group` has no queue group; jnats `ServiceBuilder` has no service-level queue group.

---

## (d) Stale manifest entries

> **Correction:** this section overstates the staleness. Most of these entries stay `planned` correctly under nq.dev's evidence rule; only the reason text was stale. See [README §5.3](README.md#5-problems-found-in-nqdev-now-fixed).


1. **`ir/capabilities/jetstream/java.nio.json`** has 510 planned / 394 implemented. **71 planned rows sit in kv.go / object.go / kv_options.go / object_options.go, and all 71 are types or fields that exist in JetStream.java:**
   - `jetstream.KeyValue` (JS:4887), `KeyValueManager` (methods on Context, JS:1358-1382), `KeyValueConfig` and all 15 fields (JS:4339)
   - `KeyValueEntry` (JS:4428), `KeyValueStatus` / `KeyValueBucketStatus` (JS:4812)
   - `KeyWatcher` (JS:5181), `KeyLister` (JS:5145), `KeyValueLister` / `KeyValueNamesLister` (JS:4741-4755)
   - `WatchOpt` (JS:4358), `KVCreateOpt` / `KVDeleteOpt` / `KVPurgeOpt` (JS:4391-4414)
   - `ObjectStore` (JS:5815), `ObjectStoreManager` (Context JS:1385-1426), `ObjectStoreConfig` and fields (JS:5445)
   - `ObjectInfo` / `ObjectMeta` / `ObjectMetaOptions` / `ObjectLink` and fields (JS:5458, 7588-7614)
   - `ObjectResult` (JS:5725), `ObjectWatcher` (JS:5801), `ObjectBucketStatus` (JS:5645), the listers (JS:5574,5588)
   - `GetObjectOpt` / `GetObjectInfoOpt` / `ListObjectsOpt` (JS:5467-5477)

   Every row carries the reason "J6/J7: awaiting IR, measured oracle vectors and a runtime binding". JETSTREAM-PLAN.md says J6b and J7b **landed in all twelve profiles**. The cause is policy: packages/java/JetStream.md:865-869 says "The contract's types and config fields ... stay `planned`, as for nats.go's own types". Methods on these types are marked `implemented`. The same pattern holds in **every** target's manifest; even go.native lists the same KV/OS types as planned. Also out of scope but visibly stale: `StreamConfig.DenyPurge` (exists at JS:7748), `StreamPurgeRequest` (JS:7805), and `PurgeOptions` (JS:144) behind `WithPurge*`.
2. **`ir/capabilities/orbit/java.nio.json`** has 40 planned:
   - **22 are stale.** Types and fields of `BatchAck` (JS:6627), `BatchFlowControl` (JS:6589), `BatchMsgOpt` (JS:6543), `BatchPublisher` (JS:6701), `BatchPublisherOpt` (JS:6578), `PublishMsgBatchOpt` (JS:6581), `FastPubAck` (JS:6934), `FastPublishErrHandler` (JS:6870), `FastPublishFlowControl` (JS:6896), `FastPublisher` (JS:6991) and `FastPublisherOpt` (JS:6873) all exist. JETSTREAM-PLAN.md says J8 landed in all twelve profiles. The reason text "J8 ... awaiting IR" is outdated.
   - **18 are accurate.** Batch get (`GetBatch*`, `GetLastMsgs*`, `GetLastForOpt`, `ErrBatchUnsupported`, `ErrNoMessages`, `ErrSubjectRequired`, `ErrInvalidResponse`) is really absent, marked "outside J8".
   - The orbit contract does not cover orbit.go `counters` or `kvcodec` at all, so their absence never shows up in any manifest.
3. **`ir/capabilities/services/java.nio.json`** (117 implemented / 4 unsupported) is accurate. The unsupported rows are `ContextHandler` and `Headers` / `.Get` / `.Values`, idiom differences that are justified. Discovery is out of contract by design (SERVICES-PLAN.md:54), so the manifest cannot report the jnats `Discovery` gap.
