
## 역할 나누기 (2026-11-15)

AI(`SafeWatch_AI`)는 판단해서 이벤트를 만드는 데까지, HW(이 저장소)는 영상 클립·큐·서버 전송을 맡는다.

```
AI  검출 → 추적 → 차선 → 판단 ──envelope──▶ HW  큐 적재 → 클립 저장 → 서버 JSON 조립 → 전송
```

AI 가 넘기는 형식(envelope)과 서버로 보내는 형식은 `edge/event.py` 맨 위에 적어 두었다.

- `event` : 서버로 그대로 나가는 부분. AI 책임, HW 는 값을 고치지 않는다
- `trigger` : 클립 자를 기준 (판정 프레임 시각, 카메라/영상 파일)
- `target` : 대상 차량 박스 등 참고 정보. 서버로 안 나감
- 서버 JSON = `event` + `location`(GPS 미장착이라 null) + `clip`

### 이벤트 큐 (`edge/upload/queue.py`)

메타데이터(JSON)와 클립(영상) 상태를 따로 관리한다.

| 상태 | 흐름 |
| --- | --- |
| metadata | `pending` → `sent`, 4xx 가 5번이면 `failed` |
| clip | `waiting` → `ready` → `sent` / 저장 실패·시간 초과면 `missing` / 요청 안 하면 `none` |

- 메타데이터는 클립 저장을 기다렸다가 clip 정보(길이·해상도)를 채워서 보낸다. 클립이 `missing` 이면 `clip: null` 로 보낸다
- 클립 파일은 메타데이터가 서버에 등록된 뒤에 보낸다. **업로드 방식이 서버와 아직 안 정해져서** 지금은 `ready` 로 대기한다 (`UploadSender(clip_uploader=...)` 자리)
- 실패하면 다음 시도를 10초, 20초, 40초 … 최대 5분 뒤로 미룬다. 실패한 건이 큐 맨 앞을 막지 않는다
- 5xx·네트워크 오류는 거절 횟수에 넣지 않는다 (서버가 살아나면 다시 보냄)
- 같은 `event_id` 는 한 번만 들어간다
- 예전 형식 `/data/queue.db` 를 열면 기존 테이블을 `events_legacy_<시각>` 으로 옮겨 두고 새로 만든다 (지우지 않음)

```bash
python scripts/flush_queue.py --status          # 상태별 건수
python scripts/flush_queue.py --show <event_id> # 한 건 자세히 + 서버로 나갈 JSON
python scripts/flush_queue.py --watch 30        # 30초마다 전송
python -m unittest tests.test_event_flow -v     # 테스트 (카메라 없이, ffmpeg 필요)
```

### 실행 (영상 파일로 전체 테스트)

AI 저장소(`~/SafeWatch_AI`, dev)가 같은 Pi 에 있어야 한다. `run_pipeline.py` 가 AI 의 `src.main.run()` 을
부르고 `event_sink` 로 `edge/sink.py` 의 `EdgeEventSink` 를 넘긴다.

```bash
source ~/yolo-env/bin/activate
cd ~/SafeWatch_HW
python run_pipeline.py --source ~/models/sim_weave/sim_weave_a40.mp4 --data ~/edge_test
python scripts/flush_queue.py --db ~/edge_test/queue.db --status
python scripts/flush_queue.py --db ~/edge_test/queue.db --events-dir ~/edge_test/events   # 서버로 전송
```

- `--config` 기본값은 AI 의 `configs/pi5_ufld_hailo.yaml` (차선 NPU, 차량 CPU)
- 이벤트가 나면 `<data>/events/<event_id>/` 에 서버 JSON 과 클립(mp4)이 생긴다
- 영상 파일 입력은 `trigger.video_sec` 앞 10초·뒤 5초를 원본에서 잘라낸다 (`edge/storage/file_clip.py`)
- 카메라 입력은 링버퍼가 자른다. 단, 카메라 공유가 아직 없어서 `--source camera` 는 막아 두었다

### 남은 일

- 카메라 공유: 카메라를 한 번 열어 H.264 는 링버퍼로, RGB 프레임은 AI 의 `frame_source` 로 (`edge/storage/ring.py` 수정)
- 클립 업로드 (서버 방식 확정 후), 디스크 정리 (`purge_done` 결과로 파일 삭제)
- 서버가 새 이벤트 JSON(event + location + clip)을 받는지 확인 (주민규)

## 엣지 파이프라인 (2026-10-27)

카메라 입력부터 서버 등록까지 전 구간이 동작한다. 판단 로직은 아직 임시 규칙이다.

### 구성

| 파일 | 역할 |
| --- | --- |
| `edge/schema.py` | TrackFrame / Event 스키마. 모듈 간 주고받는 dict를 이 함수들로만 생성 |
| `edge/storage/ring.py` | 순환 영상 버퍼. picamera2 CircularOutput으로 이전 10초 보유, 이벤트 시 전후 구간 추출 |
| `edge/judge/rules.py` | 임시 판단 규칙. 대상이 화면 면적 비율 이상으로 N초 지속되면 이벤트 |
| `edge/upload/queue.py` | SQLite 업로드 큐. 통신 단절에도 이벤트가 사라지지 않게 영속화 |
| `edge/upload/sender.py` | 서버 전송. reason code → riskType 매핑 포함 |
| `run_pipeline.py` | 전체 실행기 (2026-11-15 교체: AI `run()` + `EdgeEventSink`) |
| `scripts/flush_queue.py` | 큐를 서버로 비우는 CLI |
| `scripts/extract_tracks.py` | 영상에서 궤적만 추출(tracks.jsonl). 판단 로직 오프라인 실험용 |

### 실행

```bash
source ~/yolo-env/bin/activate

# 감지
python run_pipeline.py

# 전송 (30초마다)
python scripts/flush_queue.py --watch 30
```

환경변수로 대상 서버와 장치를 바꾼다.

```bash
export SAFEWATCH_SERVER=http://3.34.130.6:8080
export SAFEWATCH_DEVICE_ID=aaaaaaaa-0000-0000-0000-000000000001
```

### 검증된 것 (2026-10-27)

- Camera Module 3 Wide 듀얼 스트림 — 추론용 640x384 + 저장용 1280x720, 센서 2304x1296 전체 화각, 동일 프레임에서 30 FPS
- 탐지 → 클립 추출 → 큐 적재 → `POST /api/events` 서버 등록까지 관통
- 링버퍼 사전 구간 동작 확인 (시작 직후 트리거 시 버퍼가 덜 차 클립이 짧아지는 것은 정상)

### 미해결

- **riskType 매핑 불일치.** 서버 허용값은 `drunk_suspect` / `lane_departure` / `sudden_accel` / `swerving` 4개인데, 엣지 reason code 중 `TAILGATING` · `ABRUPT_CUTIN` · `SPEED_VARIANCE`는 대응값이 없어 임시로 `swerving`에 매핑했다. enum 확장 협의 필요
- **`send_events.py`와 `sender.py`가 역할이 겹친다.** 전자는 파일 기반 대기열, 후자는 SQLite 큐 연동. 하나로 합치거나 역할을 나눠야 한다
- `videoUrl`은 항상 null. S3 미구축으로 클립이 장치에만 있다
- 클립 삭제·용량 관리 미구현. `/data`가 계속 찬다
- 판단 로직은 임시 규칙. 실제 모델은 `SimpleJudge`를 교체하면 되고, 계약은 `push(TrackFrame) -> Event | None` 하나다
