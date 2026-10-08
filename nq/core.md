# jnats (core) vs NQ Java (`dev.nq`) feature gap

Scope: core NATS only. JetStream, KV, Object Store and services are out of scope.

Reference roots (all line numbers were checked against the source):

- **jnats**: `nats.java/src/main/java/io/nats/client/` (upstream main, 2.26.x). Refs are relative to this root, e.g. `Options.java:1450`.
- **NQ Java**: `nq.dev/packages/java/src/main/java/dev/nq/`. Refs are e.g. `Client.java:68`.
- **nats.go oracle** (v1.54.0): `nats.go@v1.54.0/`. Refs are e.g. `nats.go:4833`.
- **Other NQ targets**: taken from `nq.dev/ir/capabilities/*.json`, plus greps of `packages/{rust,c,typescript,python,ruby}` for features nats.go doesn't have.

## (a) Summary

- **Coverage.** NQ Java covers essentially all of the **nats.go-shaped core**:
  - publish (with and without headers), sync/callback/queue subscribe, blocking request (new and old style)
  - flush, RTT, drain, close
  - pending limits and per-subscription counters, auto-unsubscribe
  - reconnect with jitter and a custom delay, a reconnect buffer, forceReconnect
  - server discovery and lame duck
  - TLS (SSLContext, PEM, KeyStore, callbacks, TLS-first), WebSocket
  - user/pass, token and their callbacks; NKey; JWT/creds (file, bytes, JWT+seed)
  - all six connection lifecycle callbacks; async error with the subscription it concerns, which carries slow-consumer and -ERR
  - stats (5 counters), server metadata accessors, status

  The manifest `ir/capabilities/java.nio.json` lists 248/282 nats.go symbols as implemented. The 34 unsupported ones are Go channel, `context` and `*net.Dialer` APIs, the `Header` type and `NewMsg`.
- **Main gaps against jnats.** Almost all of them are **jnats-only**, with no nats.go counterpart, so they need facade decisions rather than oracle IR:
  - **Concurrency and API shape:** the `CompletableFuture` request API; `Dispatcher` (many subscriptions sharing one thread, default handler, per-subject unsubscribe); executor and thread-factory injection.
  - **Listeners:** the single-object `ConnectionListener` with `RESUBSCRIBED` and multi-listener add/remove; typed `ErrorListener` methods (`errorOccurred`, `messageDiscarded`, `socketWriteTimeout`); `ReadListener`, `TimeTraceLogger`, `traceConnection`.
  - **Statistics:** 11 extra counters (pings, oks, errs, exceptions, dropped, requests/replies/duplicate/orphan, flushes, outstanding), plus `StatisticsCollector` and advanced stats.
  - **Outgoing queue:** `maxMessagesInOutgoingQueue` and discard-when-full; `writeQueuePushTimeout`; `outgoingPendingMessageCount`; `flushBuffer`.
  - **Socket knobs:** `socketReadTimeout`, `SO_LINGER`, `SO_RCVBUF`/`SO_SNDBUF`; `java.net.Proxy`; `maxControlLine`; `hostnameResolveMode` (first-only, IPv4-only, Happy Eyeballs).
  - **CONNECT flags:** `noHeaders`, `noNoResponders`, `clientSideLimitChecks(false)`.
  - **Options:** the `Options.Builder` and `Properties` file configuration; `ForceReconnectOptions` (forceClose, flush wait); a pluggable `ServerPool`; `DataPort`; `SSLContextFactory`, keystore paths and passwords, `opentls`.
  - **Message:** the `Headers` class; status messages, `getSubscription`, `getSID`, `getConnection`.
  - **Utilities:** public `NUID`, `NKey`, `Validator`; `clearDroppedCount`, `clearLastError`.
- **Behaviour differences that matter for a facade:**
  - **Request failures:** jnats sync `request` returns `null` on timeout or no-responders. NQ throws `NqException`, with `code()` `timeout` or `no_responders`.
  - **Reconnect buffer overflow:** jnats silently drops and counts the message. NQ (following nats.go) throws `reconnect_buf_exceeded` to the publisher.
  - **TLS:** NQ always verifies the TLS **hostname** (`TlsTransport.java:34`). jnats does not set endpoint identification (`impl/SocketDataPort.java:149`).
  - **Inbox prefix:** jnats appends `.` to the prefix (default `_INBOX.`). NQ's default is `_INBOX`.
  - **Pending limits:** jnats defaults to 512Ki msgs / 64 MiB. NQ uses 500,000 / 64 MiB async and 65,536 sync (nats.go).
  - **Subjects:** jnats encodes ASCII by default. NQ always uses UTF-8.
- **Precedent.** The same kind of compatibility layer already exists for Python: `docs/NATS-PY-PAST-FACADE.md` (`nq.aio.past`). Most jnats-only items in section (b) can be a pure shell facade over existing roots. The exceptions need new IR or decisions because they change wire, statistics or policy:
  - the CONNECT flags (`noHeaders`, `noNoResponders`)
  - the extra statistics counters
  - outgoing-queue message limits and discard
  - `maxControlLine`
  - `reconnectDelayBehavior`
  - `hostnameResolveMode`
  - strict subject validation

Counts: section (b) has about 70 MISSING or PARTIAL rows; one of them is the out-of-scope JetStream listener row, kept for completeness. Section (c) maps about 70 jnats features to NQ. Most gaps are jnats-only. The few with a nats.go counterpart are API shape rather than behaviour: `getOptions` (`Conn.Opts`), `Msg.Sub`, status headers, the `Header` type, `NewMsg`, async/context request, and the shape of the statistics struct.

## (b) MISSING and PARTIAL features

Legend for the status column: **M** = missing, **P** = partial.

### Connection / `Nats`

| jnats feature | jnats ref | NQ Java status + ref | nats.go ref / jnats-only | other NQ targets | notes |
|---|---|---|---|---|---|
| Async request: `request(subj, body)`, `request(subj, headers, body)`, `request(Message)` returning `CompletableFuture<Message>` | `Connection.java:236,248,287` | **M**. Only blocking `Client.request` (`Client.java:68,82`) and `requestMsg` (`:95`) | jnats-only shape. nats.go: blocking `Request` `nats.go:4833` and `RequestWithContext` `context.go:36` | Rust tokio `async fn request` (`rust/src/tokio.rs:550`); TS `Promise` (`typescript/src/client.ts:3010`); Python asyncio (`python/nq/aio.py:1844`) | Facade: complete a future from the response mux (`Core.send_request_held` with a future-backed `HostReceive`). Cancel must release the mux entry. |
| `requestWithTimeout(...)` ×3 (future plus timeout) | `Connection.java:260,273,302` | **M** | jnats-only | as above | |
| Sync request returns `null` on timeout or no-responders | `Connection.java:319,337,357`; `impl/NatsConnection.java:1383-1388` | **P**. NQ throws `NqException` with code `timeout` or `no_responders` (`Client.java:1221,1544`) | nats.go returns `ErrTimeout` / `ErrNoResponders` (`nats.go:4833`, `:4860`) | every NQ target throws or returns an error | Facade must translate exception to `null`. |
| `reportNoResponders` (future completes with `JetStreamStatusException`), `advancedRequestBehavior` (`RequestFailureMessage`), `useTimeoutException` | `Options.java:1429,2320,2307`; `impl/NatsConnection.java:1523-1533` | **P**. NQ always reports `no_responders` and `timeout` distinctly through `NqException.code()` | jnats-only | none | Pure facade mapping. |
| `dontForceFlushOnRequest` (jnats flushes after each request by default) | `Options.java:2338` | **M**. No such knob. The request path follows nats.go: publish, then kick the flusher | jnats-only (nats.go `createNewRequestAndSend` `nats.go:4789`) | none | Latency and policy knob. It would be IR if it is to be a decision. |
| `createDispatcher(handler)`, `createDispatcher()`, `closeDispatcher(d)` | `Connection.java:412,421,430` | **M**. Each callback subscription gets its own worker (`Client.subscribe` `Client.java:40`). Opt-in shared elastic pool: `Options.callbackPool` (`Options.java:125-127`) | jnats-only (nats.go uses one goroutine per async sub, `nats.go:5006`) | C evented has opt-in loop delivery (`docs/C-NATSC-COMPARISON.md`); no target has a Dispatcher object | Facade: a Dispatcher is a group of NQ callback subs on one serial executor. Dispatcher-level pending limits and drain would have to aggregate. |
| `Dispatcher.subscribe(subject[, queue])` with the dispatcher's default handler; `unsubscribe(subject)`, `unsubscribe(subject, after)`, `unsubscribe(sub[, after])` | `Dispatcher.java:57,73,121,135,158,179` | **M** | jnats-only | none | Subject-keyed unsubscribe is shell bookkeeping. |
| Dispatcher as `Consumer` (pending limits, counts, drain for all its subs) | `Dispatcher.java` (extends `Consumer`) | **M** | jnats-only | none | |
| `addConnectionListener` / `removeConnectionListener` (multiple listeners) | `Connection.java:441,448` | **P**. One handler per event, replaceable via `setClosedHandler` / `setDisconnectHandler` / `setReconnectHandler` / ... (`Client.java:573-584`) | nats.go `Set*Handler` `nats.go:1828-1928` (single handler) | all targets single-handler | Facade fan-out list. |
| `drain(Duration)` returning `CompletableFuture<Boolean>` | `Connection.java:492` | **P**. `Client.drain()` (`Client.java:133`) returns void and runs in the background (`Drainer` `Client.java:1244-1283`). The timeout is global `Options.drainTimeout` (`Options.java:128`). Completion is signalled only through `closedHandler`. A timeout is reported as `drain_timeout` to `errorHandler` | nats.go `Drain` `nats.go:6400` plus `DrainTimeout` `nats.go:1402` | same in all targets | Facade: complete the future from `closedHandler`. A per-call timeout needs the Core drain root to take a timeout argument, which it already does (`drain_connection(..., timeout)`). |
| `getServerInfo()`: full `ServerInfo` (host, port, goVersion, proto, connectURLs, nonce, tlsAvailable, lameDuckMode, `isNewerVersionThan`, ...) | `Connection.java:550`; `api/ServerInfo.java:104-299` | **P**. Individual accessors exist (`Client.java:210-378`: `connectedServerId` / `Name` / `Version` / `ClusterName` / `Domain` / `ServerJetStream`, `isSystemAccount`, `maxPayload`, `headersSupported`, `authRequired`, `tlsRequired`, `getClientId`, `getClientIp`). Host, port, proto, connect_urls, nonce, tls_available and ldm are parsed into `Core.InfoValues` but are only exposed to `reconnectToServerHandler` (`Options.java:120`) | nats.go also exposes only individual accessors (`nats.go:2720-2845`, `6518-6539`); jnats-only object and version compare | same set in all targets | Facade: a public snapshot of `Core.InfoValues`. Version comparison is jnats-only. |
| `getOptions()` | `Connection.java:541` | **M**. Options are copied into the private `Client.options` (`Client.java:465`) | nats.go `Conn.Opts` exported field `nats.go:656` | not tracked in manifests | Trivial accessor. |
| `getLastError()` as String; `clearLastError()` | `Connection.java:571,576` | **P**. `lastError()` returns `RuntimeException` (`Client.java:563`). No clear | `LastError` `nats.go:4355`; clear is jnats-only | — | |
| `flushBuffer()` (push the buffer to the socket without PING) | `Connection.java:591` | **M**. `flush()` always does PING/PONG. Internally `Force.leaf_flush` / `outbound.flushLocked` (`Client.java:1317-1321`) exists | jnats-only | none | Shell-only. |
| `forceReconnect(ForceReconnectOptions)`: `forceClose`, `flush(wait)` | `Connection.java:611`; `ForceReconnectOptions.java` | **P**. `forceReconnect()` (`Client.java:177`) flushes the pending outbound bytes and wakes reconnect. No force-close (skip flush) and no PING-flush wait | nats.go `ForceReconnect` `nats.go:2622` (no options) | all targets plain | jnats-only options. |
| `outgoingPendingMessageCount()` | `Connection.java:814` | **M**. The outbound path is a byte buffer with no message count | jnats-only | none | |
| `Nats.connectAsynchronously(options, reconnectOnConnect)` | `Nats.java:276` | **P**. `retryOnFailedConnect` (`Options.java:132`) returns a reconnecting client if the first connect fails, but `connect` itself still blocks for the first attempt | nats.go `RetryOnFailedConnect` `nats.go:1696` | all targets | |
| `Nats.connect(url, AuthHandler)`, `Builder.authHandler(AuthHandler)`, where `AuthHandler` has `sign`, `getID` and `getJWT` | `Nats.java:194`; `Options.java:1989`; `AuthHandler.java` | **P**. `Options.userJwtHandler`, `signatureHandler` and `nkey` (`Options.java:13-15`). `nkey` is static, so `getID()` is not called per handshake. NQ rejects nkey plus JWT together (`Options.java:60`) | nats.go `UserJWT` `nats.go:1589`, `Nkey` `nats.go:1611` | all targets | The facade must decide JWT vs NKey mode at configure time, because jnats chooses by `getJWT()!=null` at handshake. |
| `Nats.CLIENT_VERSION` / `CLIENT_LANGUAGE` | `Nats.java:77,82` | **P**. CONNECT sends lang `java` and version `0.1.0`, hard-coded (`Client.java:1027`). No public constant | n/a | — | |

### Listeners and events

| jnats feature | jnats ref | NQ Java status + ref | nats.go ref / jnats-only | other NQ targets | notes |
|---|---|---|---|---|---|
| `ConnectionListener.connectionEvent(conn, Events, time, uriDetails)` as a single listener object | `ConnectionListener.java:24,102,113` | **P**. Six separate `Runnable`s: `connectedHandler`, `disconnectedHandler`, `closedHandler`, `reconnectedHandler`, `discoveredServersHandler`, `lameDuckHandler` (`Options.java:154`), plus `disconnectedErrorHandler` (`:148`). No timestamp or URI argument | nats.go per-event `ConnHandler` (`nats.go:1410-1467`, `:1686`) | all targets | Facade adapts these. |
| `Events.RESUBSCRIBED` | `ConnectionListener.java:34`; fired at `impl/NatsConnection.java:467` | **M**. Subscriptions are replayed on reconnect with no hook | jnats-only | none | |
| `ErrorListener.errorOccurred(conn, String)` for server `-ERR` | `ErrorListener.java:43`; `impl/NatsConnection.java:1986` | **P**. `-ERR` goes to `errorHandler` or `asyncErrorHandler` as an `NqException` (`Client.java:2010` then `report` `:639`) | nats.go `processErr` `nats.go:4385` → `AsyncErrorCB` | all targets | The facade must split server errors from exceptions by `NqException.code()`. |
| `ErrorListener.messageDiscarded(conn, msg)` | `ErrorListener.java:79`; `impl/NatsConnection.java:1886` | **M**. NQ never discards outgoing messages. A full outbound buffer applies backpressure, and a full reconnect buffer throws `reconnect_buf_exceeded` | jnats-only | none | Tied to the outgoing-queue gap below. |
| `ErrorListener.socketWriteTimeout(conn)` | `ErrorListener.java:151`; `impl/SocketDataPortWithWriteTimeout.java:59` | **P**. The write deadline is `Options.writeTimeout` (`Options.java:128`). A failure is reported to `errorHandler`, then reconnects when `reconnectOnFlusherError` is set (`:132`). No dedicated callback | nats.go `FlusherTimeout` `nats.go:1382`, `ReconnectOnFlusherError` `:1393` | all targets | Facade maps by error code. |
| `ErrorListener` default implementations (`ErrorListenerLoggerImpl`, `ConsoleImpl`) | `impl/ErrorListenerLoggerImpl.java` | **M**. No default logging. Errors are kept in `lastError` and passed to a handler only when one is set | jnats-only | none | |
| JetStream `ErrorListener` callbacks (`heartbeatAlarm`, `unhandledStatus`, `pullStatusWarning/Error`, `flowControlProcessed`) | `ErrorListener.java:90-144` | out of scope (JetStream) | — | — | Listed only for completeness. |
| `ReadListener` (every inbound protocol op and message) | `ReadListener.java`; `Options.java:2058` | **M** | jnats-only | none | |
| `TimeTraceLogger` / `traceConnection()` | `TimeTraceLogger.java`; `Options.java:2035,1545` | **M** | jnats-only | none | |

### Statistics

| jnats feature | jnats ref | NQ Java status + ref | nats.go ref / jnats-only | other NQ targets | notes |
|---|---|---|---|---|---|
| `Statistics` extra counters: `getPings`, `getDroppedCount`, `getOKs`, `getErrs`, `getExceptions`, `getRequestsSent`, `getRepliesReceived`, `getDuplicateRepliesReceived`, `getOrphanRepliesReceived`, `getFlushCounter`, `getOutstandingRequests` | `Statistics.java:28-122` | **P**. The `Client.Statistics` record (`Client.java:450`, via `stats()` `:182`) has only `inMessages`, `outMessages`, `inBytes`, `outBytes` and `reconnects` | nats.go `Statistics` `nats.go:1020` has the same 5; the extras are jnats-only | all targets have the 5 | The counters are an IR decision (`Core.record_outgoing_statistics`), so the extras need new IR counters. Request and reply counts are mostly advanced-stats only in jnats. |
| `StatisticsCollector` (pluggable) and `turnOnAdvancedStats()` | `StatisticsCollector.java`; `Options.java:2071,1535` | **M** | jnats-only | none | |

### Options / `Options.Builder`

| jnats feature | jnats ref | NQ Java status + ref | nats.go ref / jnats-only | other NQ targets | notes |
|---|---|---|---|---|---|
| Builder pattern and `build()`; `Builder(Properties)`, `Builder(String propsFile)`, `properties(Properties)`, `Builder(Options)` copy; about 70 `io.nats.client.*` property keys | `Options.java:1139-1174,2381,2522`; property keys `:401-787` | **M**. `Options` is a mutable class with public fields (`Options.java:9-178`) and a few fluent helpers. `copy()` is package-private | jnats-only | Python legacy facade precedent (`docs/NATS-PY-PAST-FACADE.md`) | Pure facade. |
| `hostnameResolveMode` (`ResolveToAll`, `ResolveToFirst`, `*IncludeIPV6`, `Unresolved`, `HappyEyeballs`); deprecated `noResolveHostnames()` and `enableFastFallback()` | `Options.java:1381,1360,1371`; enum `:336` | **P**. NQ resolves all addresses (IPv4 and IPv6), shuffles them unless `noRandomize`, and splits the timeout across them (`Client.java:855-868`). `skipHostLookup` (`Options.java:144`) is roughly `Unresolved`. No first-only, no IPv4-only, no Happy Eyeballs (jnats `support/HappyEyeballsConnector.java`) | nats.go `createConn` `nats.go:2460`, `SkipHostLookup` `:1746` | all targets nats.go-style | Policy decision, so it should be IR if added. |
| `subjectValidationType(None/Lenient/Strict)`; deprecated `noSubjectValidation()` and `strictSubjectValidation()` | `Options.java:1420,1396,1410` | **P**. Default is nats.go validation. `skipSubjectValidation` (`Options.java:135`) means None. No Strict | nats.go `SkipSubjectValidation` `nats.go:1808`; Strict is jnats-only | all targets | |
| `noHeaders()` (CONNECT `headers:false`) | `Options.java:1450` | **M**. CONNECT headers are decided by IR (`Core.ConnectFeatures`) with no option | jnats-only (nats.go always sends true, `connectProto` `nats.go:3089`) | none | Wire-level, so it needs IR. |
| `noNoResponders()` (CONNECT `no_responders:false`) | `Options.java:1459` | **M** | jnats-only | none | Wire-level, so it needs IR. |
| `clientSideLimitChecks(false)` | `Options.java:1469` | **P**. The max_payload check is always enforced (`leaf_max_payload_error` `Client.java:694`) | nats.go always checks (`publish` `nats.go:4631`) | all targets | Cannot be disabled. |
| `supportUTF8Subjects()` (jnats default is ASCII) | `Options.java:1481` | **P**. NQ always encodes UTF-8 (`Client.bytes` `Client.java:660`; `wtf8` for CONNECT) | jnats-only | all targets UTF-8 | A no-op in a facade. Only the default differs. |
| `maxControlLine(int)` (client-side outbound control-line check) | `Options.java:1734` | **M** | jnats-only (nats.go `MAX_CONTROL_LINE_SIZE` `parser.go:28` applies to inbound only) | none | Validation decision, so it needs IR. |
| `opentls()` (trust-all) | `Options.java:1565` | **M**. NQ always verifies the certificate and the hostname (`TlsTransport.java:34` sets endpoint identification to HTTPS) | jnats-only (Go: `TLSConfig.InsecureSkipVerify`) | none | A facade's trust-all `SSLContext` needs an `X509ExtendedTrustManager` to skip the hostname check. **jnats never does hostname verification** (`impl/SocketDataPort.java:149`), which is a real behaviour difference. |
| `secure()` (default `SSLContext`) | `Options.java:1555` | **P**. Equivalent to `tls://` or `options.sslContext = SSLContext.getDefault()`. NQ falls back to `getDefault` (`Client.java:1110`) | nats.go `Secure` `nats.go:1150` | all targets | Mapping. |
| `sslContextFactory(SSLContextFactory)` | `Options.java:1589`; `impl/SSLContextFactory.java` | **P**. `rootCAsHandler` and `clientCertHandler` (`Options.java:97,99`) re-supply material before each handshake | nats.go `ClientTLSConfig` `nats.go:1169` | all targets | |
| `keystorePath`, `keystorePassword`, `truststorePath`, `truststorePassword`, `tlsAlgorithm` | `Options.java:1599-1639` | **P**. `tlsKeyStore` and `tlsTrustStore` take KeyStore objects (`Options.java:109`). PEM files go through `rootCAs` / `clientCert` (`:101,103`). No path/password JKS or PKCS12 loading, and no algorithm choice | jnats-only (nats.go `RootCAs` / `ClientCert` PEM, `nats.go:1200,1230`) | all targets have PEM | The facade loads the KeyStore. |
| `socketReadTimeoutMillis` | `Options.java:1768` | **M**. Staleness is detected only by ping/maxPings | jnats-only | none | |
| `socketSoLinger` | `Options.java:1803` | **M** | jnats-only | none | |
| `receiveBufferSize` / `sendBufferSize` (`SO_RCVBUF` / `SO_SNDBUF`) | `Options.java:1815,1827` | **M**. `SO_RCVBUF` is hard-coded to 512 KiB with TCP_NODELAY (`Transport.java:45-46`) | jnats-only (Go would use `Dialer` / `CustomDialer`) | none | Workaround: `customDialer` (`Options.java:141-142`) can set socket options. |
| `bufferSize` (initial I/O buffer size) | `Options.java:1892` | **P**. `writeBufferSize` (`Options.java:131`, default 2 MiB) is the outbound batch limit. The read buffer is fixed at 256 KiB (`NioTransport.java:70`, `Transport.java:70`) | nats.go `WriteBufferSize` `nats.go:1365` | all targets | Different meaning. |
| `requestCleanupInterval` | `Options.java:1861` | **M** (not needed: each request removes its own mux entry on reply, timeout or failure) | jnats-only | n/a | No-op in a facade. |
| `writeQueuePushTimeout` | `Options.java:1871` | **M**. Publishes block inline on outbound backpressure | jnats-only | none | |
| `maxMessagesInOutgoingQueue` / `discardMessagesWhenOutgoingQueueFull` | `Options.java:2263,2275` | **M**. Byte-bounded buffer with backpressure | jnats-only | none | Policy, so it needs IR to be a decision. |
| `reconnectBufferSize` semantics | `Options.java:1908` | **P**. The field exists (`Options.java:131`), but nats.go semantics apply: a negative value disables, and overflow throws `reconnect_buf_exceeded`. jnats: 0 disables, a negative value is unlimited, and overflow silently drops and counts in `droppedCount` | nats.go `ReconnectBufSize` `nats.go:1352` | all targets nats.go | The facade must translate the values and the drop behaviour. |
| `reconnectDelayBehavior(BeforeSubsequentRounds/BeforeAllRounds)` | `Options.java:2013`; enum `:298` | **M**. `customReconnectDelayHandler` (`Options.java:122`) gets the completed pass count. When it is first invoked is fixed by Core | jnats-only | none | Policy, so it needs IR. |
| `executor`, `scheduledExecutor`, `connectExecutor`, `callbackExecutor`, `readerExecutor`, `writerExecutor`; `connect` / `callback` / `reader` / `writerThreadFactory` | `Options.java:2097-2204` | **M**. Daemon threads named `nq-*` are created in `Client.start` (`Client.java:655`). The only knobs are `profile` (`NIO`/`THREADED`) and the callback pool (`Options.java:123-127`) | jnats-only | none | Shell-only, with no protocol decisions. |
| `useDispatcherWithExecutor()` | `Options.java:2329` | **P**. Closest is `callbackPool` | jnats-only | none | |
| `dispatcherFactory(DispatcherFactory)` | `Options.java:2358` | **M** | jnats-only | none | |
| `httpRequestInterceptor(s)` (modify the WebSocket upgrade request) | `Options.java:2215,2229` | **P**. `websocketHeaders` and `websocketHeadersHandler` (`Options.java:89-90`) add headers. `proxyPath` (`:87`) sets the path | nats.go `WebSocketConnectionHeaders` `nats.go:1776,1791`, `ProxyPath` `:1718` | all targets | |
| `proxy(java.net.Proxy)` (SOCKS or HTTP) | `Options.java:2240` | **M**. `SocketChannel` cannot use `java.net.Proxy`. Only a hand-written tunnel through `customDialer` is possible | jnats-only (Go: `CustomDialer` `nats.go:1657`) | none | |
| `dataPortType` (pluggable `DataPort` transport) | `Options.java:2252`; `impl/DataPort.java` | **P**. `customDialer` returns a connected `SocketChannel` (`Options.java:141-142`). The `Transport` interface is package-private | nats.go `SetCustomDialer` `nats.go:1657` | all targets have a custom dialer | |
| `serverPool(ServerPool)` (pluggable: `initialize`, `acceptDiscoveredUrls`, `peek`/`nextServer`, `resolveHostToIps`, `connectSucceeded`/`Failed`, `getServerList`, `hasSecureServer`) | `Options.java:2348`; `ServerPool.java:31-101` | **P**. `reconnectToServerHandler` (`Options.java:120`), `serverPool()` / `setServerPool()` (`Client.java:507,517`), `ignoreDiscoveredServers`, `noRandomize` | nats.go `ReconnectToServer` `nats.go:1324`, `ServerPool` `:6597`, `SetServerPool` `:6629` | all targets | A full pluggable pool is jnats-only. Pool order is an IR decision (`Core.build_server_pool`). |
| `userInfo(char[],char[])`, `token(char[])`, `tokenSupplier(Supplier<char[]>)` | `Options.java:1938,1965,1977` | **P** (shape only). `user` / `password` / `token` are `String`s; `tokenHandler` is `Supplier<String>` (`Options.java:13,146`) | nats.go `UserInfo` / `Token` / `TokenHandler` `nats.go:1476-1507` | all targets | Conversion between `char[]` and `String`. |

### Subscription / Consumer / Message / Headers / utilities

| jnats feature | jnats ref | NQ Java status + ref | nats.go ref / jnats-only | other NQ targets | notes |
|---|---|---|---|---|---|
| `Subscription.getDispatcher()` | `Subscription.java:57` | **M** | jnats-only | none | |
| `nextMessage(long millis)`; `null` on timeout | `Subscription.java:93,75` | **P**. Only `nextMessage(Duration)` (`Subscription.java:115`), which throws `NqException(timeout)` | nats.go `NextMsg` `nats.go:5573` returns `ErrTimeout` | all targets | Facade overload and translation. |
| `Consumer.clearDroppedCount()` | `Consumer.java:103` | **M** | jnats-only (nats.go `ClearMaxPending` `nats.go:5840` resets the peak, not dropped) | none | |
| `Consumer.drain(Duration)` returning `CompletableFuture<Boolean>` | `Consumer.java:126` | **P**. `Subscription.drain()` returns void (`Subscription.java:122`) | nats.go `Subscription.Drain` `nats.go:5276` | all targets | Completion could come from `setClosedHandler` (`Subscription.java:96`). |
| `Message.getHeaders()` as a `Headers` class (`add`, `put`, `remove`, `getFirst`, `getLast`, `getIgnoreCase`, `containsKeyIgnoreCase`, `keySetIgnoreCase`, `readOnly`, `isDirty`, `serializedLength`, ...) | `impl/Headers.java:48-593` | **P**. `Map<String, List<String>>` (`Message.java:49`). Key and value validation happens at publish time | nats.go `Header` map `nats.go:4439` with `Add`/`Set`/`Get`/`Values`/`Del` `:4443-4474` (Java manifest: unsupported) | Rust `Header::add` etc., C `nq_msg_header_*` | The case-insensitive helpers and read-only flag are jnats-only. |
| `Message.isStatusMessage()` / `getStatus()` | `Message.java:68,74` | **M** publicly. Status lives in the package-private `Message.receiveStatus` / `receiveDescription` (`Message.java:19-22`). A 503 on a request or sync sub surfaces as `NqException(no_responders)` | nats.go puts status in `Msg.Header["Status"]` | — | Expose through headers or accessors. |
| `Message.getSubscription()` | `Message.java:94` | **M** | nats.go `Msg.Sub` field `nats.go:819` | not tracked in manifests | |
| `Message.getSID()` / `getConnection()` | `Message.java:101,107` | **M**. `sid` is package-private (`Subscription.java:53`) | jnats-only | none | |
| `NatsMessage.builder()` and the constructors taking `Headers` | `impl/NatsMessage.java:446-546,74-94` | **P**. Public constructor `Message(subject, reply, data, headers)` (`Message.java:25`) | nats.go `NewMsg` `nats.go:4479` (Java: unsupported) | Rust `Message::new`, C `nq_msg_create` | |
| `hasHeaders()` | `Message.java:56` | **P**. Check `headers()` for non-null and non-empty | — | — | Trivial. |
| `NUID` public class (`nextGlobal`, `next`, ...) | `NUID.java:74-137` | **M**. Internal `Core.NuidState`. Only `newInbox()` and `Client.defaultInbox()` are public | external `nuid` package in Go | none public | |
| `NKey` public class (`createUser`, `fromSeed`, `sign`, `verify`, `isValidPublic*Key`, ...) | `NKey.java:371-662` | **M**. `Credentials` is package-private (`Credentials.java:20`). Signing exists internally | external `nkeys` in Go | none public | |
| `support.JwtUtils.issueUserJWT` | `support/JwtUtils.java:87-249` | **P**. `Jwt.newUserClaims` / `encodeUserClaims` (`Jwt.java:163,180`) | external `nats-io/jwt` | Rust `jwt.rs` | |
| `support.Validator.validateSubject` / `QueueName` / `ReplyTo` | `support/Validator.java:101-121` | **M** publicly. Validation is internal in Core | jnats-only | none | |

## (c) PRESENT features (jnats → dev.nq)

Connection and connect:
- `Nats.connect()` → `Client.connect(new Options())`. Empty servers fall back to the default URL (`Client.java:470`).
- `Nats.connect(url)` → `Client.connect(String)` (`Client.java:548`). Comma lists go through `connect(String, Options)` (`:553`).
- `Nats.connect(options)` → `Client.connect(Options)` (`:164`) or `Options.connect()` (`Options.java:182`).
- `Nats.connectReconnectOnConnect(...)` → `Options.retryOnFailedConnect` (`Options.java:132`).
- `Nats.credentials(file)` and `credentials(jwt, nkey)` → `Options.userCredentials(file[, seed])` (`Options.java:65,67`).
- `Nats.staticCredentials(bytes)` and `staticCredentials(jwt, nkey)` → `userCredentialBytes` (`:69,71`) and `userJwtAndSeed` (`:73`).
- `publish(subj, body)` → `publish(String, byte[])` (`Client.java:561`).
- `publish(subj, headers, body)` and `publish(subj, reply, headers, body)` → `publish(subj, reply, data, Map)` (`:33`).
- `publish(subj, reply, body)` → `publishRequest` (`:27`).
- `publish(Message)` → `publishMsg` (`:566`).
- `request(subj, body, timeout)` → `request` (`:68`).
- `request(subj, headers, body, timeout)` → `request(..., headers)` (`:82`).
- `request(Message, timeout)` → `requestMsg` (`:95`). Throws instead of returning null.
- `subscribe(subj)` → `subscribe(String)` (`:562`).
- `subscribe(subj, queue)` → `subscribe(subj, queue, null)` (`:40`).
- Callback subscriptions (`Dispatcher.subscribe(subj[, queue], handler)`) → `subscribe(subj, queue, Consumer<Message>)` (`:40`).
- `flush(Duration)` → `flush(Duration)` (`:47`). `flush()` (`:57`) defaults to 10 s.
- `close()` → `close()` (`:138`). No checked `InterruptedException`.
- `getStatus()` / `Connection.Status` → `status()` (`:359`) returning `Core.ClientStatus` (`Core.java:580`), which also has `draining_subs` and `draining_pubs`. Also `isConnected` / `isClosed` / `isReconnecting` / `isDraining` (`:352,150,157,143`).
- `getMaxPayload()` → `maxPayload()` (`:313`).
- `getServers()` → `servers()` (`:189`). Also `discoveredServers()` (`:196`).
- `getConnectedUrl()` → `connectedUrl()` (`:587`). Returns `""`, not `null`, when disconnected.
- `getClientInetAddress()` → `getClientIp()` (`:366`).
- `createInbox()` → `newInbox()` (`:110`).
- `forceReconnect()` → `forceReconnect()` (`:177`).
- `RTT()` → `rtt()` (`:124`).
- `outgoingPendingBytes()` → `buffered()` (`:540`), nats.go `Buffered` `nats.go:6080`.
- `getStatistics()` (the in/out msgs/bytes and reconnects part) → `stats()` (`:182`), a `Statistics` record (`:450`).

Subscription and Consumer:
- `getSubject()` / `getQueueName()` → `subject()` / `queue()` (`Subscription.java:85,87`).
- `nextMessage(Duration)` → `nextMessage` (`:115`).
- `unsubscribe()` → `unsubscribe` (`:105`). `unsubscribe(after)` → `autoUnsubscribe(long)` (`:110`).
- `isActive()` → `isValid()` (`:134`).
- `setPendingLimits` → `setPendingLimits` (`:176`).
- `getPendingMessageLimit` / `getPendingByteLimit` → `pendingLimits()` (`:155`).
- `getPendingMessageCount` / `getPendingByteCount` → `pending()` (`:148`).
- `getDeliveredCount` → `delivered()` (`:141`).
- `getDroppedCount` → `dropped()` (`:169`).

Message:
- `getSubject` / `getReplyTo` / `getData` / `getHeaders` → `subject` / `reply` / `data` / `headers` (`Message.java:45-49`).

Listeners:
- `CONNECTED` → `connectedHandler`. `DISCONNECTED` → `disconnectedHandler` and `disconnectedErrorHandler`. `RECONNECTED` → `reconnectedHandler`. `CLOSED` → `closedHandler`. `DISCOVERED_SERVERS` → `discoveredServersHandler`. `LAME_DUCK` → `lameDuckHandler`. All are in `Options.java:148-154`.
- `ErrorListener.exceptionOccurred` → `errorHandler` and `asyncErrorHandler(sub, ex)` (`Options.java:148,153`). Reconnect failures go to `reconnectErrorHandler`.
- `ErrorListener.slowConsumerDetected(conn, consumer)` → `asyncErrorHandler(sub, NqException code slow_consumer)` (`Subscription.java:205`). The shape differs; the facade switches on `code()`.

Reconnect:
- `ReconnectDelayHandler.getWaitTime(totalTries)` → `customReconnectDelayHandler` (`LongFunction<Duration>`, `Options.java:122`).

`Options.Builder`, mapped to `dev.nq.Options` fields:
- `server` / `servers` → `url` / `servers` (`:176,178`).
- `oldRequestStyle` → `useOldRequestStyle`; `noRandomize`, `noEcho`, `verbose`, `pedantic` and `ignoreDiscoveredServers` keep their names (all `:133`).
- `connectionName` → `name` (`:146`).
- `inboxPrefix` → `inboxPrefix` (`:145`). jnats appends `.`; NQ's default is `_INBOX`.
- `sslContext` → `sslContext` (`:92`).
- `tlsFirst` → `tlsHandshakeFirst` (`:113`).
- `credentialPath` → `userCredentials` (`:65`).
- `noReconnect` → `allowReconnect = false` (`:132`). jnats implements it as `maxReconnects = 0`.
- `maxReconnects` → `maxReconnects` (`:131`).
- `reconnectWait`, `reconnectJitter`, `reconnectJitterTls` keep their names (`:128`).
- `connectionTimeout` → `timeout` (`:128`).
- `socketWriteTimeout` → `writeTimeout` (`:128`). The default is 1 min in both.
- `pingInterval` → `pingInterval` (`:128`).
- `maxPingsOut` → `maxPings` (`:131`).
- `userInfo(String, String)` → `user` / `password`, and `token(String)` → `token` (`:146`).
- `tokenSupplier` → `tokenHandler` (`:13`).
- `reconnectDelayHandler` → `customReconnectDelayHandler` (`:122`).

WebSocket:
- `ws://` and `wss://` URLs → built-in `WebSocketTransport` / `WebSocketUpgrade`.

Features NQ has that jnats lacks (nats.go-derived; useful extras in a facade):
- On `Client`: `barrier`, `newRespInbox`, `tlsConnectionState`, `connectedUrlRedacted`, `connectedAddr` / `localAddr`, `getClientId`, `isSystemAccount`, `connectedServerJetStream`, `numSubscriptions`.
- On `Message`: `respond`, `equal`, `size`.
- On `Subscription`: `setClosedHandler`, `maxPending` / `clearMaxPending`, `status()`.
- Options: `compression`, `proxyPath`, `websocketHeaders`, `tlsServerName`, `rootCAs` / `clientCert` PEM, `reconnectToServerHandler`, `customDialer`, `skipHostLookup`, `ignoreAuthErrorAbort`, `permissionErrOnSubscribe`, `noCallbacksAfterClose`, `reconnectOnFlusherError`, `drainTimeout`, `callbackPool`, `profile`.
- `AuthService` (auth callout) and `Jwt`.

## (d) API-shape differences for a jnats-compatible facade

1. **Exceptions.**
   - jnats uses checked exceptions: `IOException` and its subclass `AuthenticationException`, `InterruptedException`, `TimeoutException`. It also throws `IllegalArgumentException` / `IllegalStateException` for validation.
   - NQ throws only unchecked `NqException` (sealed; `code()` is a `Core.ErrorCode`, `NqException.java:4,22`) and `InfoDecodeException`. It also uses `IllegalArgumentException` for option validation.
   - NQ wraps interrupts in `NqException.interrupted`.
   - So the facade must rethrow by error code: auth codes become `AuthenticationException`, connection codes become `IOException`, `timeout` becomes `TimeoutException`.
2. **Sync vs async.** jnats offers `CompletableFuture` for request, drain and sub-drain. NQ is blocking for request and flush, and fire-and-forget for drain, with completion signalled through `closedHandler`.
3. **Request failure reporting.** In jnats, sync `request` returns `null`. Futures are cancelled (`CancellationException`), or complete exceptionally with `TimeoutException` when `useTimeoutException` is set, or with `JetStreamStatusException(503)` when `reportNoResponders` is set. `advancedRequestBehavior` returns a `RequestFailureMessage`. NQ throws `NqException(timeout | no_responders | connection_closed ...)`.
4. **`Headers` vs `Map`.**
   - jnats `Headers` is a class with ordered keys, case-insensitive helpers, read-only and dirty flags, and serialization.
   - NQ uses `Map<String, List<String>>`, which is null for "no headers" on outbound and lazily empty on inbound (`Message.java:49-56`).
   - `Message` is an interface in jnats (`NatsMessage` plus a builder) but a final class with a public constructor in NQ.
5. **Options model.**
   - jnats `Options` is immutable, built with `Options.Builder`, and readable through getters and `Properties` keys.
   - NQ `Options` is a mutable class with public fields, copied at connect (`Options.copy` `:184`).
   - Durations: jnats has `Duration` plus `long millis` overloads. NQ has only `Duration`.
   - Secrets: jnats uses `char[]`. NQ uses `String`.
6. **Listener model.**
   - jnats has one `ConnectionListener` (enum events, timestamp, URI) and an add/remove list, plus one `ErrorListener` with typed methods.
   - NQ has nats.go-style per-event `Runnable` / `Consumer<RuntimeException>` fields, with one setter per handler (`Client.java:573-584`). Async errors arrive as `BiConsumer<Subscription, RuntimeException>`.
   - Both run callbacks off-thread: NQ through one FIFO callback dispatcher, jnats on its callback executor.
7. **Subscriptions and threads.**
   - jnats groups callback subscriptions into `Dispatcher`s, each with one thread or the executor. A sync subscription is a `Subscription` without a dispatcher.
   - NQ gives each callback subscription its own worker by default, or an elastic pool. There is no dispatcher object, and a callback subscription is still a `Subscription`.
   - Message handlers: jnats `MessageHandler.onMessage` may throw `InterruptedException`. NQ uses a `Consumer<Message>`.
8. **Naming.** NQ follows nats.go names; jnats uses Java-bean names. Examples:
   - `stats()` vs `getStatistics()`
   - `status()` (lowercase enum) vs `getStatus()` (uppercase `Connection.Status`)
   - `rtt()` vs `RTT()`
   - `newInbox()` vs `createInbox()`
   - `reply()` vs `getReplyTo()`
   - `autoUnsubscribe(n)` vs `unsubscribe(n)`
   - `isValid()` vs `isActive()`
   - `Counts` records vs separate getters
9. **Defaults that differ.**
   - Inbox prefix: `_INBOX.` (jnats) vs `_INBOX` (NQ).
   - Async pending limit: 512Ki msgs (jnats) vs 500,000 (NQ, nats.go). Byte limit is 64 MiB in both.
   - Sync pending limit: 512Ki (jnats) vs 65,536 (NQ, nats.go `SubChanLen`).
   - Outbound buffer: 64 KiB initial (jnats) vs 2 MiB write batch (NQ).
   - Subject encoding: ASCII (jnats) vs UTF-8 (NQ).
   - TLS hostname verification: off (jnats) vs on (NQ).
   - Reconnect buffer overflow: drop and count (jnats) vs throw (NQ).
   - `noReconnect`: `maxReconnects=0` (jnats) vs `allowReconnect=false` (NQ).
   - `connectedUrl` when disconnected: `null` (jnats) vs `""` (NQ).
   - `lastError`: a String (jnats) vs a `RuntimeException` (NQ).
10. **Nullability.** jnats accepts a `null` body and `null` headers (`@Nullable`). NQ's API takes `byte[]`, and its examples always pass non-null data; null-payload tolerance was not verified. NQ `Message.reply()` may be `""` or `null`.
11. **Where gaps would land.** Pure shell work can live in a facade package, in the style of nats-py "past":
   - Dispatcher, futures, Builder and Properties
   - listener fan-out
   - Headers wrapper, exception translation
   - NUID/NKey wrappers
   - executors and thread factories

   Items that change wire bytes, statistics or policy must be IR, per nq.dev's `AGENTS.md` ("keep shared behavioral decisions in IR"):
   - `noHeaders` / `noNoResponders` (CONNECT)
   - extra statistics counters
   - outgoing-queue message limits and discard
   - `maxControlLine`
   - strict subject validation
   - `hostnameResolveMode`
   - `reconnectDelayBehavior`
   - `ForceReconnectOptions.forceClose` and flush-wait
   - jnats reconnect-buffer drop semantics

   None of these has a nats.go oracle, so each needs a recorded facade decision.
