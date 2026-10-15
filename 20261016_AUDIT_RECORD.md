# dureq Audit Record - 2026-10-16

## 1. 감사 기준과 결론

- 기준 커밋: `337a1d479e7d25fc86fe81b45e91a46460982277`.
- 감사 시작 시 worktree는 clean이었다. 제품 코드 수정 없이 이 기록만 추가했다.
- 범위: daemon HTTP/gRPC/WebSocket, webhook, worker claim/completion/reclaim, Redis dispatch/durability, workflow/batch progression, SDK operation identity, 관련 테스트와 보장 문서.
- 방법: CodeGraph로 호출 관계와 소스를 우선 탐색하고, 필요한 구간 및 rueidis `v1.0.72` 구현을 확인했다. 보안 검토에는 `security-best-practices`의 Go backend 지침을 적용했다.
- 결과: **17개 항목: High 13개, Medium 4개**. Critical로 확정한 항목은 없다. 아래의 "신규"는 이번 감사에서 새로 발견했다는 뜻이며, 최근 커밋에서 유입됐다는 뜻은 아니다.
- 실제 재현: webhook의 loopback 직접 접근 및 redirect 접근, loop 경로와 무관한 분기의 reset. 나머지는 코드 및 호출 순서로 확인했으며, 실제 Redis 장애 주입이나 운영 환경 침투로 재현했다고 주장하지 않는다.

주요 위험은 인증 없는 관리 API, 내부망으로 향하는 webhook 요청, 완료 ledger와 실제 결과/이벤트의 불일치, 장애 후 진행되지 않는 workflow다. ledger 한 필드의 최초 완료 CAS만으로 사용자에게 노출되는 결과와 후속 상태 전이까지 한 번만 반영된다고 볼 수 없다.

보안 노출도는 배포 경계에 따라 달라진다. 인증 프록시, 방화벽, egress 정책이 실제 운영에서 적용되는지는 이번 저장소 감사로 확인하지 않았다. TLS 종단 위치나 private webhook 사용 자체를 취약점으로 판정하지 않았다.

## 2. High: 보안

### S01. 기본 daemon 관리 API에 인증·인가 경계가 없음

- 분류: 신규 발견 / High / 인증·권한 통제, CWE-306.
- 위치: [daemon main:67](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/cmd/dureqd/main.go:67), [main:149](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/cmd/dureqd/main.go:149), [main:162](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/cmd/dureqd/main.go:162), [API.Handler:38](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/monitor/api.go:38), [관리 라우트:160](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/monitor/api.go:160).
- 근거: 기본 주소는 `:8080`, `:9090`; HTTP handler는 `corsMiddleware(a.mux)`, gRPC는 인증 interceptor 없이 `grpc.NewServer()`로 생성된다. 작업 조회·취소·재시도·삭제·payload 수정, queue pause, node drain, force unlock 등이 같은 API에 노출된다.
- 영향/조건: 해당 listener에 접근 가능한 사용자는 인증 없이 작업 데이터에 접근하고 실행을 방해하거나 수정할 수 있다. 단순 read-only monitoring API가 아니다.
- 추가 노출: [CORS:1000](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/monitor/api.go:1000)는 origin `*`를 허용하고, [WebSocket:21](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/monitor/ws_handler.go:21)는 origin 검사를 끈다. 브라우저의 실제 접근 허용 여부는 네트워크 정책에 좌우되지만, 애플리케이션의 origin 방어는 없다. 여기의 `InsecureSkipVerify`는 TLS 검증 해제가 아니라 WebSocket origin 검증 해제다.
- 수정: HTTP/gRPC/WS에 공통 인증을 적용하고 조회와 실행 변경 권한을 분리한다. 직접 노출의 기본값은 loopback 또는 명시적인 관리망으로 제한한다. 허용 origin을 설정으로 제한한다.
- 임시 완화/오탐 확인: 인증 프록시와 네트워크 ACL을 HTTP와 gRPC 양쪽에 적용하고, backend 포트에 대한 우회 접근이 막혔는지 확인한다.
- 회귀 검증: 인증 없는 read/write/WS 요청 거절, read-only 사용자의 cancel/unlock 거절, 보호되지 않은 gRPC 우회 금지.

### S02. webhook이 임의 내부 주소 및 redirect 목적지에 접근함

- 분류: 신규 발견 / High / `GO-SSRF-001`, CWE-918. payload를 신뢰할 수 없는 주체가 통제하는 배포에서 적용된다.
- 위치: [HTTPWebhookPayload:16](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/cmd/dureqd/handlers/http_webhook.go:16), [HTTP 요청:50](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/cmd/dureqd/handlers/http_webhook.go:50), [client.Do:61](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/cmd/dureqd/handlers/http_webhook.go:61), [기본 handler 등록:39](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/cmd/dureqd/handlers/registry.go:39).
- 근거: payload의 `URL`, `Method`, `Headers`를 그대로 HTTP 요청에 사용하며 destination allowlist, DNS/IP 제한, redirect 검증이 없다. HTTP client는 기본 redirect 정책을 사용한다. handler 목록이 비어 있으면 webhook을 포함한 모든 built-in handler가 등록된다.
- 재현: 실제 등록된 `http_webhook` handler를 호출했다. 로컬 fixture로의 직접 GET과 302 redirect 모두 대상에 도달했고, 내부 JSON 응답이 성공 결과에 포함됐다. metadata 서비스나 외부 시스템은 호출하지 않았다.
- 영향/조건: webhook payload 입력권자는 worker가 접근 가능한 내부 HTTP 서비스에 요청을 보내고 JSON 응답을 읽을 수 있다. S01과 결합하면 수정 가능한 scheduled webhook job의 payload를 바꿔 같은 경로에 도달할 수도 있다. API 자체에 임의 enqueue 기능이 있다고 가정하지 않았다.
- 수정: 업무상 필요한 목적지의 scheme/host/port allowlist를 도입하고, 실제 dial 주소와 모든 redirect hop을 검증한다. 내부 webhook이 필요하면 허용 목적지를 명시한다. 임의 목적지와 임의 header 권한을 별도로 제한한다.
- 임시 완화/오탐 확인: webhook handler 비활성화 또는 worker egress 제한. 모든 생산자가 관리자이고 arbitrary internal webhook이 의도된 기능이라면 위험 수용 범위와 권한 경계를 문서화한다.
- 회귀 검증: direct loopback, link-local, IPv6 loopback, DNS rebinding, 허용 host에서 비허용 목적지로 redirect, 정상 allowlist 목적지.

## 3. High: 실행과 복구

### R01. 완료 replay 및 repair가 durable 후속 작업을 유실할 수 있음

- 분류: 기존 문제 미해결 / High.
- 위치: [CompleteRun:303](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_streams.go:303), [firstCompletion 분기:359](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_streams.go:359), [repair:411](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_streams.go:411), [waiter:1050](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/client/client.go:1050).
- 근거: ledger를 먼저 done으로 만든 뒤 다른 key들의 pipeline을 실행한다. replay는 `firstCompletion=false`라 stats/event/notification을 생략한다. repair는 result와 `XACK`를 먼저 보내고 별도 `XADD`를 실행하며, 오류를 반환하지 않는다.
- 실패 순서: ledger commit → event shard의 쓰기 실패 → work shard의 ACK 성공 → 같은 CompleteRun 재시도 → event 생략. 또는 repair의 ACK 성공 직후 프로세스 종료 → completion event 없음. 둘 다 ACK된 work message라 XAUTOCLAIM이 복구 기회를 제공하지 않는다.
- 영향: workflow/batch 진행과 완료 통계가 누락되고, 결과가 저장돼도 한 번의 초기 poll 이후 Pub/Sub만 기다리는 `EnqueueAndWait`가 timeout될 수 있다. repair는 정상 history/active-run cleanup/job field 정리/워크플로 result TTL 연장도 재구성하지 못한다.
- 수정: ledger에 완전한 canonical completion과 replay 가능한 후속 작업을 함께 저장한다. 후속 projection/event 적용을 idempotent하게 추적하고, durable 복구 경로가 확보된 뒤 ACK한다. notification은 저장된 결과를 읽으라는 힌트로 사용하고 waiter에 재조회 경로를 둔다.
- 검증: commit 뒤 모든 단계에서 crash/응답 유실/일부 shard 실패를 주입하고, 재시작 후 결과·job/run/history·후속 dispatch가 수렴하는지 확인한다.

### R02. ledger CAS에서 패한 실행도 자기 결과로 공개 result를 덮어씀

- 분류: 기존 duplicate execution 문제에서 확장해 신규 확인 / High.
- 위치: [claim Lua:311](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_lua.go:311), [run lock:558](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/worker/worker.go:558), [완료 CAS:331](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_lua.go:331), [CompleteRun:292](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_streams.go:292), [result HSET:380](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_streams.go:380).
- 근거: 이미 claimed인 operation도 `ok`를 반환하고 lock은 RunID별이다. `CompleteOperation`이 false를 반환해도 phase 2의 result는 ledger의 확정 값이 아니라 호출자가 넘긴 `batch.Result`다.
- 실패 순서: 같은 operation/job의 R1과 R2가 동시에 시작 → R1이 결과 A로 ledger commit → R2의 결과 B commit은 거절 → R2가 공개 result hash를 B로 덮어씀. 이후 repair는 다시 A로 되돌릴 수도 있다.
- 영향: ledger, 조회 API, history, downstream이 서로 다른 성공 결과를 관측한다. ledger 필드의 CAS 성공 횟수만 검사하는 테스트로는 검출되지 않는다.
- 수정: commit 응답에 canonical 결과/commit identity를 반환하고 모든 projection을 그 값으로 수행한다. operation ownership/fencing으로 동시에 유효한 completer를 제한하고 claim 오류에서 실행을 계속하는 정책도 명시적으로 바꾼다.
- 검증: 같은 operation, 다른 RunID, 다른 output을 동시에 완료시켜 모든 읽기 경로가 동일한 winner를 반환하는지 검사한다. retry를 허용하는 시스템에서 handler가 평생 한 번만 시작된다는 보장을 목표로 삼지는 않는다.

### R03. completion event가 result보다 먼저 보이고, 결과 없는 완료가 고정됨

- 분류: 신규 발견 / High.
- 위치: [event XADD:362](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_streams.go:362), [result HSET:380](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_streams.go:380), [condition result:408](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:408), [ResultFrom fallback:906](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:906).
- 근거: event와 result는 다른 key이며 Cluster에서는 서로 다른 shard일 수 있다. DoMulti는 cross-shard transaction이 아니다. consumer는 result 조회 실패를 재시도 가능한 오류로 처리하지 않고 condition 처리를 중단하거나 원래 payload로 대체한다.
- 실패 순서: event shard에 XADD 도착 → orchestrator가 event 소비 → result shard에는 아직 HSET 미완료 → condition task를 completed로 저장하지만 route는 없음, 또는 downstream에 잘못된 payload 전송. 늦은 결과 저장 후에도 terminal guard가 재처리를 막는다.
- 영향: 정상적인 multi-shard 지연 차이만으로도 workflow가 멈추거나 잘못된 입력으로 진행할 수 있다.
- 수정: event가 가리키는 canonical result를 consumer가 반드시 확인하도록 한다. 결과가 아직 없으면 ACK/terminal transition을 보류하고 retry한다. projection 완료 확인 후 durable event 발행 또는 result를 포함한 outbox envelope를 사용한다.
- 검증: result shard만 지연시키고 event shard/consumer는 정상 동작시켜 condition, subflow, ResultFrom, batch onetime 결과를 검사한다.

### R04. FiringID pre-check가 dispatch 전에 소비되어 schedule firing이 유실됨

- 분류: 신규 발견 / High.
- 위치: [DispatchWork:49](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_streams.go:49), [atomic dispatch 호출:97](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_streams.go:97).
- 근거: `SET firingKey NX EX 1h`를 먼저 실행한다. 이후 Lua/XADD가 실행되지 않았어도 같은 FiringID 재시도는 `return "", nil`로 종료한다. 주석의 best-effort라는 표현과 달리 실제로는 dispatch를 차단하는 gate다.
- 실패 순서: firing marker 저장 → worker stream 기록 전 crash/연결 실패 → 같은 firing 재시도 → 이미 발송된 것으로 처리. durable work message가 한 번도 생성되지 않을 수 있다.
- 수정: FiringID dedup과 stream append를 같은 slot의 원자 연산으로 묶거나, durable dispatch intent를 저장하고 성공 전까지 재실행 가능한 상태로 관리한다. 중복 표식만으로 완료로 취급하지 않는다.
- 검증: pre-check 직후 중단한 뒤 동일 FiringID로 재시도하여 실제 stream entry가 정확히 하나 생성되는지 검사한다.

### R05. WAIT/WAITAOF가 원래 쓰기의 연결 및 shard에 묶이지 않음

- 분류: 기존 fail-open 문제에 추가로 신규 확인 / High.
- 위치: [waitDurability:2437](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_store.go:2437), [dispatch 호출:109](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_streams.go:109), [completion 호출:310](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_streams.go:310).
- 근거: 쓰기 script와 대기 명령을 각각 일반 `s.rdb`로 호출한다. pinned dependency rueidis v1.0.72의 `internal/cmds/gen_generic.go:1899,1926`은 두 대기 명령을 `blockTag`로 만들고, `mux.go:234`는 이를 일반 pipeline과 다른 blocking pool에 보낸다. `cluster.go:455`는 keyless InitSlot 명령의 node를 기존 쓰기 slot에 맞추지 않는다.
- 의미: 두 명령은 해당 연결의 선행 쓰기를 기준으로 확인한다. 다른 연결의 ACK count가 충분해도 방금 수행한 ledger/XADD의 내구성을 확인한 것이 아니다. 이 조건은 [Redis WAIT 공식 문서](https://redis.io/docs/latest/commands/wait/)와 [WAITAOF 공식 문서](https://redis.io/docs/latest/commands/waitaof/)에 명시되어 있다.
- 추가 문제: count 부족/unsupported command도 호출부에서 warn 후 성공으로 처리한다. count 파싱이 추가된 것만으로 durability 계약이 성립하지 않는다.
- 수정: 대상 shard에 고정된 dedicated connection에서 critical write와 durability wait를 순서대로 수행한다. count 검증 실패는 호출자에게 전파하되 이미 쓰기가 반영됐을 수 있는 ambiguous outcome으로 다루고, 안전한 idempotent retry를 제공한다.
- 검증: 연결 ID와 대상 node를 기록하는 테스트, replica 지연/연결 교체/Cluster failover 테스트. 이 감사에서는 dependency의 연결 선택 구현을 확인했으며 실제 failover를 주입하지 않았다.

### R06. reclaim 후 run lock 획득 실패 시 아직 실행 중인 메시지를 ACK함

- 분류: 신규 발견 / High.
- 위치: [claimStaleLoop:351](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/worker/worker.go:351), [lock 실패 처리:560](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/worker/worker.go:560), [heartbeat:400](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/worker/worker.go:400).
- 근거: heartbeat는 run/ledger를 갱신하지만 stream PEL idle 시간을 갱신하지 않는다. 5분 넘게 실행 중인 메시지가 reclaim되면 다른 worker는 같은 run lock을 획득하지 못하고 해당 stream message를 ACK한다.
- 실패 순서: W1의 장기 handler 실행 → W2가 같은 PEL entry를 reclaim → lock 실패 후 ACK → W1이 완료 전 crash. 해당 entry는 더 이상 pending이 아니므로 Streams 복구가 불가능하다.
- 영향: 별도의 scheduler orphan 복구가 없는 worker-only 구성에서는 특히 작업이 유실될 수 있다. 다른 복구 경로가 있더라도 Streams의 at-least-once 근거가 사라진다.
- 수정: lock이 사용 중이라는 사실만으로 ACK하지 않는다. 완료 ledger 또는 durable한 대체 delivery를 확인하기 전에는 pending으로 유지한다. 필요하면 ownership에 맞는 visibility 갱신을 추가한다.
- 검증: reclaim 기준을 넘는 handler 실행, 두 번째 worker의 lock 실패, 첫 worker 종료를 조합해 실제 재처리 가능성을 확인한다.

### R07. task 완료 guard가 crash 이후 successor dispatch 복구까지 차단함

- 분류: 기존 duplicate 방어의 불완전한 복구 의미 / High.
- 위치: [terminal guard:246](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:246), [완료 저장:329](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:329), [후속 dispatch:345](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:345).
- 실패 순서: A 완료를 workflow에 CAS 저장 → B dispatch 전 leader crash → unacked A event 재처리 → A가 terminal이라 즉시 return → event ACK. B는 pending으로 남는다.
- 영향: duplicate state mutation은 억제되지만, 완료 transition과 후속 task 생성 사이의 장애가 복구되지 않는다. 단일 연결·단일 shard에서도 가능한 실패다.
- 수정: "이 task의 완료를 반영했는가"와 "이 완료에서 파생되는 다음 작업을 모두 반영했는가"를 분리한다. 이미 완료된 event도 미반영 dispatch intent를 replay할 수 있어야 한다.
- 검증: A의 workflow CAS 직후 강제 종료하고 새 orchestrator만으로 B가 실행되는지 확인한다.

### R08. 실행 중 task 완료가 Suspended workflow를 자동으로 Running으로 바꿈

- 분류: 신규 발견 / High.
- 위치: [handleJobCompleted:327](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:327), [suspended 검사:340](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:340), [allow-failure 경로:717](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:717).
- 근거: task completion 저장 전에 `wf.Status = WorkflowStatusRunning`을 무조건 수행하고, 그 다음에 Suspended인지 검사한다. 성공 경로의 검사 조건은 도달할 때 이미 false다. allow-failure 진행 경로에도 같은 순서가 존재한다.
- 재현 조건: A 실행 중 workflow suspend, B는 A에 의존. A가 완료하면 명시적 resume 없이 B가 dispatch된다.
- 영향: 운영자가 일시정지한 workflow가 새 결제/외부 호출 등을 시작할 수 있다.
- 수정: 로드한 workflow의 suspended 상태를 task 결과 저장 시 보존한다. 진행 여부는 보존된 상태로 판단하고 명시적인 resume 처리에서만 dispatch한다.
- 검증: suspend → active task 성공/allow-failure → task 결과는 저장되지만 후속 task는 실행되지 않음 → resume 후 한 번 실행.

### R09. orchestrator의 처리 실패 event가 정상 동작 중 재시도되지 않음

- 분류: 신규 발견 / High.
- 위치: [eventLoop:94](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:94), [오류 처리:110](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:110), [drainPending:123](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:123).
- 근거: pending drain은 시작 시 한 번 실행된다. 이후 실패한 event는 ACK하지 않고 넘어가지만, read loop는 계속 `XREADGROUP ... >`만 사용한다. 이 경로에는 정기적인 pending retry/reclaim이 없다.
- 영향: 일시적인 GetJob/GetWorkflow/CAS 장애만으로도 복구 가능한 event가 다음 orchestrator 재시작까지 처리되지 않을 수 있다. 반대로 시작 시에는 영구 실패하는 pending entry를 backoff 없이 반복해 새 event 소비를 막을 수 있다.
- 수정: 새 event와 pending retry를 함께 처리하는 bounded retry 스케줄을 둔다. 실패 횟수/backoff와 poison-event 격리 절차를 추가하고, 나머지 정상 event가 진행하도록 한다.
- 검증: 첫 처리만 실패하는 event의 재시도, 영구 실패 event와 정상 event의 공존, 재시작 없이 backlog 회복.

### R10. batch 상태 저장 실패를 무시한 뒤 완료 event가 소비 완료됨

- 분류: 기존 문제 미해결 / High.
- 위치: [onetime 완료:1233](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:1233), [UpdateBatch:1243](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:1243), [item 완료 저장:1305](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:1305), [finalizeBatch:1553](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:1553).
- 근거: onetime completion은 event 발행과 item dispatch 후 `UpdateBatch` 오류를 버린다. finalizeBatch도 terminal event를 먼저 발행하고 마지막 `UpdateBatch`의 결과를 버린다. 상위 handler는 nil을 반환해 입력 event가 ACK된다.
- 실패 순서: batch cancel/retry와 CAS 경합 또는 저장 장애 → 외부에는 completed event, DB에는 running/cancelled 또는 이전 count → event ACK → 자동 재시도 근거 소실.
- 수정: 모든 critical Update 오류를 event consumer까지 전파하고 CAS 충돌 시 재조회한다. 파생 dispatch/event는 durable intent와 commit identity를 기준으로 replay한다. event-before-state를 단순히 state-before-event로 뒤집는 것만으로 crash gap이 사라지지는 않는다.
- 검증: batch 마지막 item 완료와 cancel의 경합, UpdateBatch 한 번 실패, onetime 저장 전 종료, hook UpdateWorkflow 오류 전파.

### R11. ResultReuseNever가 성공 ledger 때문에 재실행되지 않음

- 분류: 신규 발견 / High.
- 위치: [retryWorkflow:1614](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:1614), [task reset:1625](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:1625), [operation key 구성:865](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/orchestrator.go:865), [DONE skip:527](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/worker/worker.go:527).
- 근거: workflow retry는 ResultReuseNever인 completed task를 pending으로 되돌리지만 `Iteration` 또는 다른 execution generation을 증가시키지 않는다. operation key는 같은 `wfID:taskName:iteration`으로 남는다.
- 재현 조건: orchestrator가 dispatch한 non-root B가 성공하고, 다음 C가 실패해 workflow retry가 시작됨. B가 ResultReuseNever라 reset되지만 새 worker는 이전 DONE ledger를 읽고 B handler를 건너뛴다.
- 영향: 명시적으로 재계산/재실행해야 하는 task에 이전 결과를 재사용한다. Redis ledger뿐 아니라 같은 OperationKey를 쓰는 외부 dedup도 같은 결과를 고정할 수 있다.
- 수정: transport retry와 사용자가 요청한 새로운 logical execution을 구분한다. 성공 결과를 폐기하고 재실행하는 경우 generation을 올리고, 동일 logical execution의 장애 재시도에는 기존 key를 유지한다.
- 검증: ResultReuseNever, Always, OnSuccess 각각에 대해 실제 handler 호출 횟수와 operation key를 검사한다. SDK/monitor/자동 workflow retry를 모두 포함한다.

## 4. Medium: 보안과 기능 정확성

### S03. HTTP timeout과 요청 body 상한이 없음

- 분류: 신규 발견 / Medium / `GO-HTTP-001`, `GO-HTTP-002`, CWE-400.
- 위치: [http.Server:152](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/cmd/dureqd/main.go:152), [payload decoder:739](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/monitor/api.go:739), [bulk decoder:802](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/monitor/api.go:802).
- 근거: `http.Server{Addr: apiAddr, Handler: mux}`에 read-header/read/idle timeout이 없다. 요청 JSON은 MaxBytesReader 없이 decode한다. bulk ID 개수 절단은 전체 body를 파싱한 뒤 실행돼 메모리 제한이 되지 않는다. Go의 기본 header 크기 상한까지 없다고 주장하는 것은 아니다.
- 영향/조건: 직접 listener에 접근 가능한 사용자가 slow request나 큰 JSON body로 연결/메모리를 점유할 수 있다. load/DoS 공격 테스트는 실행하지 않았다.
- 수정: JSON route에 body byte 상한과 decode deadline을 적용하고 ReadHeaderTimeout/IdleTimeout 등을 설정한다. WS/streaming 경로에 일괄 WriteTimeout을 적용해 정상 연결을 끊지 않도록 분리한다.
- 임시 완화/오탐 확인: ingress의 timeout/body limit이 실제 backend 직접 접근까지 방어하는지 확인한다.
- 검증: 제한 초과 JSON은 413, 느린 header는 deadline 종료, 정상 WS는 유지.

### R12. resetLoopBody가 loop와 무관한 side branch까지 reset함

- 분류: 신규 발견 / Medium. 해당 분기에 비멱등 외부 효과가 있으면 업무 영향은 더 클 수 있다.
- 위치: [resetLoopBody:451](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/workflow/dag.go:451).
- 근거: startTask에서 forward BFS를 하며 endTask만 제외한다. 도달한 노드가 실제로 endTask의 선행 노드인지 검사하지 않는다.
- 재현 fixture: `start -> body -> condition`, 별도로 `start -> side`. `side`는 완료된 독립 분기다. `resetLoopBody(start, condition)` 호출 후 side가 `pending`, 빈 JobID, `Iteration=1`로 바뀌었다.
- 영향: loop가 다시 돌 때 무관한 완료 task도 다시 실행되고, OperationKey까지 바뀌어 ledger가 중복 효과를 억제하지 못한다.
- 수정: start에서 도달 가능하고 동시에 end에 도달 가능한 노드의 교집합만 reset한다. start/end 자체와 외부 선행 dependency 처리도 명시한다.
- 검증: 독립 side branch, diamond, loop 외부 dependency가 있는 body, 이미 실행 중인 분기를 포함한다.

### R13. effect ledger의 빈 결과 및 claimant 소유권 처리 오류

- 분류: 신규 발견 및 기존 "CAS 수정 완료" 판정 보정 / Medium.
- 위치: [ClaimOperationEffect:2524](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_store.go:2524), [completion Lua:366](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/store/redis_lua.go:366).
- 빈 결과 문제: `CompleteOperationEffect(..., "")` 후 claim 응답은 정확히 `done:`다. `len(result) > 5` 조건을 통과하지 않아 `("", false, nil)`로 반환되고 호출자는 신규 claim처럼 외부 효과를 재실행할 수 있다. API는 빈 결과를 금지하지 않는다.
- 소유권 문제: completion Lua는 status가 done인지 확인할 뿐 claimant token/generation을 검사하지 않는다. A의 pending TTL 만료 → B가 새로 claim → 오래된 A가 먼저 complete하면 B의 claim을 A가 완료 처리할 수 있다. 이미 done인 값을 덮어쓰지 않는다는 것과 stale owner를 거절한다는 것은 다르다.
- 수정: 문자열 길이 대신 명시적인 status/result 응답을 사용한다. claim token을 반환하고 완료·갱신 시 token을 비교하며, 완료 tombstone의 보존 기간을 별도로 정한다. 외부 API의 idempotency 계약은 여전히 필요하다.
- 검증: 빈 문자열 결과, pending TTL 만료 후 재claim, 이전 claimant의 늦은 complete, 완료 후 TTL/retention.

### R14. SDK가 생성하는 workflow/batch job의 identity가 orchestrator와 다름

- 분류: 이전 SDK 수정의 잔여 문제 / Medium.
- 위치: [SDK root dispatch:585](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/client/client.go:585), [SDK workflow helper:1208](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/client/client.go:1208), [onetime helper:1238](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/client/client.go:1238), [item helper:1283](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/client/client.go:1283), [fallback:1137](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/internal/client/client.go:1137).
- 근거: SDK의 세 helper는 job을 만들면서 stable operation header를 설정하지 않는다. dispatchJob에 generic OperationKey 전파가 생겼어도 이 경로는 임의 JobID로 fallback한다. orchestrator의 동일 task/item은 workflow/batch 기반 key를 사용한다.
- 영향: 같은 logical task/item이 SDK와 orchestrator의 재생성 경로를 오가면 dedup identity가 바뀐다. batch 첫 chunk와 이후 chunk의 계약도 달라진다. 이전 "batch/onetime stable key 수정 완료" 판정은 orchestrator 경로에만 해당한다.
- 추가 불일치: explicit operation-key header가 있으면 SDK가 firing-id header를 별도 필드로 복사하지 않는다. handler Version도 SDK에는 설정 경로가 없으므로, versioned dispatch가 필요한 사용 시 계약을 정해야 한다.
- 수정: operation identity 생성과 WorkMessage 구성을 공통화하고 SDK 및 orchestrator가 동일 규칙을 사용하게 한다. 재실행 generation 정책은 R11과 함께 설계한다.
- 검증: 실제 EnqueueWorkflow/EnqueueBatch 호출, 첫 dispatch와 재생성 시 OperationKey, 두 header 동시 사용. 임의 WorkMessage에 테스트가 직접 key를 넣는 것만으로는 검증되지 않는다.

## 5. 이전 감사 항목의 현재 판정

| 이전 항목 | 현재 판정 | 이번 확인 내용 |
|---|---|---|
| Repair payload가 WorkResult였음 | 해당 오류 수정됨 | 현재 JobEvent를 직렬화하며 EventsPubSub도 발행한다. 전체 repair 내구성은 R01로 남음. |
| stale completion/failed event | 부분 수정 | task/item/onetime/hook에 JobID guard, retrying helper에 동일 JobID 검사가 있다. 다만 `state.JobID != ""` 조건 때문에 reset 직후 빈 JobID 상태는 보호하지 못한다. |
| guard 이전 WorkflowTaskCompleted 발행 | 해당 순서 수정됨 | guard 뒤로 이동했다. 여전히 workflow CAS 전에 발행하므로 저장 실패 시의 event consistency까지 해결된 것은 아니다. |
| loop OperationKey 충돌 | iteration 증가는 확인 | target, condition, intermediate reset에 증가가 있다. side branch reset과 ResultReuseNever는 R12/R11에 남음. |
| operation effect pending을 done으로 반환 | 해당 오류 수정됨 | ErrEffectPending 반환. owner fencing과 빈 결과 문제는 R13에 남음. |
| operation effect CAS로 stale owner 방지 | 이전 완료 판정 철회 | non-done→done 조건만으로 소유권 교체 뒤 stale completer를 구분할 수 없다. |
| heartbeat의 ledger TTL refresh | 코드 반영됨 | RefreshOperationLedger 호출은 존재. 완료 retention/retry horizon 정합성이나 refresh 실패까지 보장하지는 않는다. |
| WAIT/WAITAOF count 검사 | 부분 수정 | reply count 검사 있음. 연결/shard 결합 및 실패 전파는 R05 미해결. |
| duplicate handler start | operation 단위 배타성 없음 | docs/guarantees.md는 per-RunID duplicate suppression으로 범위를 제한한다. handler 재시작 자체보다 canonical 결과 오염 R02를 구체 버그로 판단했다. |
| SDK OperationKey 전파 | 부분 수정 | 일반 job에는 key가 생겼지만 logical workflow/batch identity는 R14 미해결. |
| replay event/notify 누락 | 미해결 | firstCompletion gating과 repair ACK 순서의 복구 공백, R01. |
| critical Update 오류 무시 | 미해결 | batch, hook, repair 등에서 상태 저장 오류를 버리는 경로가 남아 있음, R10. |

이전 보고서의 "repair가 PublishEvent를 우회해서 workflow event index가 누락된다"는 설명은 그대로 재사용하지 않는다. 현재 `isWorkflowEventType`은 일반 `EventJobCompleted`를 index 대상으로 분류하지 않으므로, helper 우회만으로 정상 경로 대비 별도 index 회귀가 성립하지 않는다. run/node metadata 차이와 durable repair 부재는 별개다.

## 6. 실행한 검증과 한계

| 명령/검사 | 결과 |
|---|---|
| `go vet ./internal/... ./pkg/... ./cmd/...` | exit 0. |
| `go test ./internal/... ./pkg/... ./cmd/... -count=1 -timeout=90s` | exit 1. internal/ratelimit 3개 테스트가 Redis 연결 거부로 실패. workflow/scheduler/types/sideeffect 등은 pass. |
| `go test -race ./internal/workflow ./pkg/types ./pkg/sideeffect -count=1 -timeout=60s` | 세 패키지 모두 exit 0. 이 범위에서 race report 없음. |
| `go test ./tests/integration -run 'TestPhase3_OperationLedger_ClaimAndComplete\|TestFixH_CompleteRun_Replay_EventSkippedOnReplay' -count=1 -timeout=20s` | 두 테스트 모두 Redis cluster localhost:7005/7006 연결 거부로 실패. 코드 assertion 검증까지 도달하지 못함. |
| 임시 overlay 재현 테스트 | workflow 1개, webhook direct/redirect 2개 case에서 안전한 기대 조건 위반을 재현. 아래 결과 참조. |
| `git diff --check` | 기록 작성 전 제품 worktree에 오류 없음. 기록 작성 후에도 확인. |

`internal/client`의 Redis 의존 테스트 helper는 Redis가 없으면 skip한다. 따라서 해당 패키지의 `ok` 출력은 실제 enqueue/wait 통합 동작의 성공 증거로 사용하지 않았다. 전체 `go test ./...`, PostgreSQL integration, 실제 Cluster failover/resharding, 실서비스 공격 테스트는 완료하지 않았다. govulncheck 실행 파일이 없어 dependency CVE 진단은 수행하지 않았다.

사용한 Go는 `go1.27.1`, go.mod는 `go 1.25.5`다. 재현 테스트 실행 시 Sonic이 지원 범위 밖이라 encoding/json fallback 경고를 출력했다. 이 경고를 제품 취약점으로 집계하지 않았으며 다른 Go/Sonic 조합에서의 race/performance까지 검증한 것은 아니다.

### 재현 artifact

제품 테스트 파일을 수정하지 않고 Go `-overlay`로 임시 파일을 주입했다. artifact는 다음 로컬 경로에 남겨 두었다. 임시 디렉터리 삭제 후에도 재현 조건을 알 수 있도록 fixture와 관측값을 이 기록에 포함한다.

- `/private/tmp/dureq-audit-20261016.OvTBtq/workflow_probe_test.go`
- `/private/tmp/dureq-audit-20261016.OvTBtq/webhook_probe_test.go`
- `/private/tmp/dureq-audit-20261016.OvTBtq/overlay.json`

```sh
go test -overlay /private/tmp/dureq-audit-20261016.OvTBtq/overlay.json \
  ./internal/workflow ./cmd/dureqd/handlers \
  -run TestAudit20261016 -v -count=1 -timeout=30s
```

관측 결과:

```text
TestAudit20261016ResetLoopExcludesUnrelatedBranch:
  unrelated branch reset: status=pending jobID="" iteration=1
TestAudit20261016WebhookRejectsPrivateTargets/direct:
  private target reached: hits=1 err=<nil>
  result={"status_code":200,"body":{"internal":"audit-fixture"}}
TestAudit20261016WebhookRejectsPrivateTargets/redirect:
  private target reached: hits=1 err=<nil>
  result={"status_code":200,"body":{"internal":"audit-fixture"}}
```

이 테스트의 FAIL은 수정된 제품이 실패했다는 뜻이 아니라, 현 코드가 안전한 기대 조건을 위반한다는 재현 결과다. S02는 loopback fixture만 사용했고 실제 내부망을 탐색하지 않았다.

### 기존 테스트가 놓치는 지점

- [TestFixA:44](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/tests/integration/exactly_once_fix_test.go:44)는 CompleteRun 전에 ledger를 완료시킨다. 이후 두 stats count가 모두 0이어도 동일성 assertion이 통과하므로 "실제 완료 통계가 정확히 1"을 검증하지 않는다.
- [TestFixH:163](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/tests/integration/exactly_once_fix_test.go:163)는 성공한 첫 호출 뒤 replay의 event 생략만 확인한다. 첫 호출이 부분 실패했을 때 eventual completion을 보장하는 테스트가 아니다.
- [TestBatchItem_StableOperationKey:568](/Users/fdk/Documents/dev/fdk/scheduler/dureq/dureq/tests/integration/exactly_once_fix_test.go:568)는 테스트가 직접 OperationKey를 넣은 WorkMessage를 DispatchWork에 전달한다. R14의 실제 SDK helper 경로를 실행하지 않는다.
- stale-event 테스트는 임의 sleep 후 상태가 변하지 않았는지를 확인한다. 해당 event가 consumer에서 실제 처리/ACK됐는지를 동기화하고 정상 event positive control을 함께 검증해야 미처리로 인한 거짓 통과를 줄일 수 있다.

## 7. 수정 우선순위

1. S01/S02: 관리 API 접근 경계와 webhook 목적지 정책을 먼저 확정하고 적용한다.
2. R01~R06: canonical completion/result, durable outbox 및 repair, dispatch marker 원자성, 연결에 묶인 durability, ACK 조건을 함께 정리한다.
3. R07~R11: workflow/batch에 완료 사실과 후속 작업 적용을 분리하고, pending retry와 CAS 실패 전파를 추가한다. suspend 및 재실행 generation 의미를 보존한다.
4. S03/R12~R14: request resource limit, loop reset 범위, effect claim token, SDK/orchestrator identity 공통화를 반영한다.
5. 각 수정은 성공 경로 assertion보다 commit 경계에서의 crash, reply loss, stale generation, cross-shard 지연을 우선 테스트한다.

이 기록은 구현 감사 결과다. 외부 효과의 exactly-once outcome은 외부 시스템의 idempotency key 또는 업무 transaction/outbox 계약을 계속 필요로 한다.
