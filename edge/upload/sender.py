"""업로드 큐 → 서버 전송.

서버는 같은 id 재전송을 200으로 돌려주므로(멱등) 재시도가 안전하다.
네트워크가 없으면 조용히 넘어가고 다음 주기에 다시 시도한다.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone

# 우리 reason code → 서버 riskType
# 서버 허용값: drunk_suspect, lane_departure, sudden_accel, swerving
RISK_TYPE_MAP = {
    "LANE_DEPARTURE": "lane_departure",
    "LANE_WEAVING":   "swerving",
    "SUDDEN_BRAKE":   "sudden_accel",
    "RAPID_ACCEL":    "sudden_accel",
    # 아래 셋은 서버 enum에 대응값이 없어 임시 매핑. 확장 협의 필요.
    "TAILGATING":     "swerving",
    "ABRUPT_CUTIN":   "swerving",
    "SPEED_VARIANCE": "sudden_accel",
}

SERVER_URL = os.environ.get("SAFEWATCH_SERVER", "http://3.34.130.6:8080")
DEVICE_ID  = os.environ.get("SAFEWATCH_DEVICE_ID",
                            "aaaaaaaa-0000-0000-0000-000000000001")


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(
        timespec="milliseconds").replace("+00:00", "Z")


def to_server_payload(ev: dict) -> dict:
    """엣지 Event 스키마 → 서버 EventCreateRequest."""
    code = ev["reasons"][0]["code"] if ev.get("reasons") else "TAILGATING"
    loc = ev.get("location") or {}
    return {
        "id":        ev["event_id"],
        "deviceId":  DEVICE_ID,
        "detectedAt": _iso(ev["window"]["start"]),
        "riskType":  RISK_TYPE_MAP.get(code, "swerving"),
        "riskScore": ev["score"],
        "latitude":  loc.get("lat"),
        "longitude": loc.get("lon"),
        "plateNumber": (ev.get("target") or {}).get("plate"),
        "videoUrl":  None,          # S3 미구축. 추후 업로드 후 URL 기입
    }


class UploadSender:
    def __init__(self, queue, server_url=SERVER_URL, timeout=10):
        self.queue = queue
        self.url = server_url.rstrip("/") + "/api/events"
        self.timeout = timeout

    def _post(self, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.url, data=body, method="POST",
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")

    def flush(self, limit=20, verbose=True):
        """pending 항목을 전송한다. (성공, 실패) 건수를 돌려준다."""
        ok = fail = 0
        for event_id, meta_json, _clip in self.queue.pending(limit):
            ev = json.loads(meta_json)
            try:
                status, _ = self._post(to_server_payload(ev))
                if 200 <= status < 300:
                    self.queue.mark_uploaded(event_id)
                    ok += 1
                    if verbose:
                        print(f"[SEND ] {event_id[:8]} -> {status}", flush=True)
                else:
                    fail += 1
                    if verbose:
                        print(f"[SEND ] {event_id[:8]} 거절 {status}", flush=True)
            except urllib.error.HTTPError as e:
                # 400류는 재시도해도 소용없다. 사유를 남긴다.
                detail = e.read().decode("utf-8", "replace")[:300]
                fail += 1
                if verbose:
                    print(f"[SEND ] {event_id[:8]} HTTP {e.code}: {detail}",
                          flush=True)
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                # 네트워크 문제. 다음 주기에 다시 시도한다.
                fail += 1
                if verbose:
                    print(f"[SEND ] 네트워크 대기: {e}", flush=True)
                break
        return ok, fail
