#!/usr/bin/env python3
"""
SafeWatch 장치 시뮬레이터 — 라즈베리파이가 할 일을 흉내낸다.

실제로는 AI 가 위험을 감지하면 이 코드가 서버로 올린다. 지금은 AI 파트가 완성 전이라
감지 결과를 무작위로 만들어 보낸다. 나중에 라즈베리파이가 생기면 이 파일을 그대로 올리고
make_event() 안만 실제 감지 결과로 바꾸면 된다.

차량 환경을 전제로 만든 것:
  - id 는 장치가 만든다. 통신이 끊겼다 몰아서 올라와도 번호가 꼬이지 않는다.
  - 전송에 실패하면 버리지 않고 쌓아둔다. 연결되면 쌓인 것부터 다시 보낸다.
  - 같은 이벤트를 다시 보내도 서버가 중복을 만들지 않는다(서버가 200 으로 받아준다).

사용법:
    python3 send_events.py --server http://172.21.35.104:8080
    python3 send_events.py --server http://172.21.35.104:8080 --interval 3 --count 10

표준 라이브러리만 쓴다. 라즈베리파이에 따로 설치할 게 없다.
"""

import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections import deque
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# 장치 설정
# ---------------------------------------------------------------------------

# 이 장치의 id. 서버의 devices 테이블에 등록된 값이어야 한다.
# 없는 값을 보내면 서버가 400 으로 거절한다.
DEFAULT_DEVICE_ID = "aaaaaaaa-0000-0000-0000-000000000001"

# schema.sql 의 CHECK 제약과 같은 값이어야 한다.
RISK_TYPES = ["drunk_suspect", "lane_departure", "sudden_accel", "swerving"]

# 아주대 근처 좌표. 실제로는 GPS 모듈에서 읽는다.
BASE_LAT, BASE_LON = 37.2820, 127.0450

PLATES = ["12가 3456", "34나 7890", "56다 1234", "78라 5678", None]


def make_event(device_id):
    """감지 결과 하나를 만든다. 실제 장치에서는 이 함수가 AI 출력을 받아 채운다."""
    return {
        # 서버가 아니라 장치가 만든다. 재전송해도 같은 id 라 중복이 안 생긴다.
        "id": str(uuid.uuid4()),
        "deviceId": device_id,
        # 감지한 시각(장치 시계). 서버가 받은 시각은 서버가 따로 적는다.
        "detectedAt": datetime.now(timezone.utc).isoformat(),
        "riskType": random.choice(RISK_TYPES),
        "riskScore": random.randint(55, 99),
        "latitude": round(BASE_LAT + random.uniform(-0.01, 0.01), 6),
        "longitude": round(BASE_LON + random.uniform(-0.01, 0.01), 6),
        "plateNumber": random.choice(PLATES),
        # 영상은 파일이 아니라 링크만 보낸다. 실제로는 스토리지에 올린 주소.
        "videoUrl": None,
    }


def post_event(server, event, timeout):
    """이벤트 하나를 서버로 보낸다. (성공여부, 상태코드, 메시지) 를 돌려준다."""
    url = server.rstrip("/") + "/api/events"
    body = json.dumps(event).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as res:
            return True, res.status, ""
    except urllib.error.HTTPError as e:
        # 서버까지는 닿았지만 거절당한 경우. 보통 우리가 보낸 값이 잘못된 것이라
        # 다시 보내도 결과가 같다. 쌓아두지 않고 버린다.
        detail = ""
        try:
            detail = json.loads(e.read().decode("utf-8")).get("detail", "")
        except Exception:
            pass
        return False, e.code, detail
    except urllib.error.URLError as e:
        # 서버에 닿지도 못한 경우(터널, 지하주차장, 서버 꺼짐).
        # 이건 우리 잘못이 아니므로 쌓아뒀다가 다시 보낸다.
        return False, 0, str(e.reason)


def main():
    p = argparse.ArgumentParser(description="SafeWatch 장치 시뮬레이터")
    p.add_argument("--server", required=True,
                   help="서버 주소. 예: http://172.21.35.104:8080")
    p.add_argument("--device", default=DEFAULT_DEVICE_ID, help="장치 id(UUID)")
    p.add_argument("--interval", type=float, default=5.0, help="전송 간격(초)")
    p.add_argument("--count", type=int, default=0, help="보낼 개수. 0 이면 멈출 때까지")
    p.add_argument("--timeout", type=float, default=5.0, help="응답 대기(초)")
    args = p.parse_args()

    print(f"서버   : {args.server}")
    print(f"장치   : {args.device}")
    print(f"간격   : {args.interval}초 / 개수: {'무제한' if args.count == 0 else args.count}")
    print("중지하려면 Ctrl+C\n")

    pending = deque()   # 보내지 못하고 쌓아둔 이벤트
    sent = failed = 0
    n = 0

    try:
        while args.count == 0 or n < args.count:
            n += 1

            # 쌓인 것이 있으면 새 이벤트보다 먼저 보낸다(감지된 순서를 지킨다).
            pending.append(make_event(args.device))

            while pending:
                event = pending[0]
                ok, code, msg = post_event(args.server, event, args.timeout)
                stamp = datetime.now().strftime("%H:%M:%S")

                if ok:
                    pending.popleft()
                    sent += 1
                    label = "등록" if code == 201 else "이미 있음"
                    print(f"[{stamp}] {code} {label:8s} "
                          f"{event['riskType']:15s} 위험도 {event['riskScore']:3d}  "
                          f"{event['plateNumber'] or '번호판 미인식'}")
                elif code == 0:
                    # 서버에 못 닿음. 쌓아두고 다음 차례에 다시 시도한다.
                    print(f"[{stamp}] --- 전송 실패({msg}) · 쌓아둠 {len(pending)}건")
                    break
                else:
                    # 서버가 거절. 다시 보내도 같으므로 버린다.
                    pending.popleft()
                    failed += 1
                    print(f"[{stamp}] {code} 거절     {msg}")

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print()

    print(f"\n보냄 {sent}건 · 거절 {failed}건 · 못 보내고 남은 것 {len(pending)}건")
    if pending:
        print("남은 것은 프로그램을 끄면 사라진다. 실제 장치라면 파일에 저장해 두어야 한다.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
