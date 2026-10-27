
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
| `run_pipeline.py` | 전체 실행기 |
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
