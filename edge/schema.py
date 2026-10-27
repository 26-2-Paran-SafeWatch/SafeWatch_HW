"""SafeWatch 인터페이스 스키마 v1.0

모든 모듈이 이 파일의 함수로 메시지를 생성한다.
직접 dict를 만들지 말 것 - 필드가 어긋나면 통합에서 깨진다.
"""
from __future__ import annotations

import time
import uuid
from typing import Any

TRACKFRAME_SCHEMA = "trackframe/1.0"
EVENT_SCHEMA = "event/1.0"

REASON_CODES = (
    "TAILGATING",       # 안전거리 미확보
    "LANE_WEAVING",     # 사행 운전
    "ABRUPT_CUTIN",     # 급끼어들기
    "SUDDEN_BRAKE",     # 급제동
    "RAPID_ACCEL",      # 급가속
    "LANE_DEPARTURE",   # 차선 이탈
    "SPEED_VARIANCE",   # 속도 불안정
)

VEHICLE_CLASSES = {2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}


def _f(v) -> float | None:
    """numpy 타입을 JSON 직렬화 가능한 float으로."""
    return None if v is None else float(v)


def _i(v) -> int | None:
    return None if v is None else int(v)


def make_track(
    tid, cls, conf, box,
    lane_rel=None, dist_m=None, ttc_s=None, age=0, lost=0,
) -> dict[str, Any]:
    return {
        "id": _i(tid),
        "cls": str(cls),
        "conf": round(_f(conf), 3),
        "box": [round(_f(v), 1) for v in box],
        "lane_rel": _i(lane_rel),
        "dist_m": None if dist_m is None else round(_f(dist_m), 1),
        "ttc_s": None if ttc_s is None else round(_f(ttc_s), 2),
        "age": _i(age),
        "lost": _i(lost),
    }


def make_ego(speed_kph=None, accel_x_g=None, accel_y_g=None,
             yaw_rate_dps=None, lat=None, lon=None, gps_fix=False):
    return {
        "speed_kph": _f(speed_kph),
        "accel_x_g": _f(accel_x_g),
        "accel_y_g": _f(accel_y_g),
        "yaw_rate_dps": _f(yaw_rate_dps),
        "lat": _f(lat),
        "lon": _f(lon),
        "gps_fix": bool(gps_fix),
    }


def make_lanes(detected=False, ego_left_x=None, ego_right_x=None,
               offset_m=None, curvature=None):
    return {
        "detected": bool(detected),
        "ego_left_x": _f(ego_left_x),
        "ego_right_x": _f(ego_right_x),
        "offset_m": _f(offset_m),
        "curvature": _f(curvature),
    }


def make_trackframe(ts, frame_id, src_w, src_h, fps, tracks,
                    ego=None, lanes=None) -> dict[str, Any]:
    return {
        "schema": TRACKFRAME_SCHEMA,
        "ts": round(_f(ts), 3),
        "frame_id": _i(frame_id),
        "src": {"w": _i(src_w), "h": _i(src_h), "fps": round(_f(fps), 2)},
        "ego": ego or make_ego(),
        "lanes": lanes or make_lanes(),
        "tracks": tracks,
    }


def level_of(score: int) -> str:
    if score >= 70:
        return "high"
    if score >= 40:
        return "mid"
    return "low"


def make_reason(code: str, weight: int, **detail) -> dict[str, Any]:
    if code not in REASON_CODES:
        raise ValueError(f"알 수 없는 reason code: {code}")
    return {"code": code, "weight": _i(weight), "detail": detail}


def make_event(device_id, ts_start, ts_end, track_id, cls, score, reasons,
               location=None, clip=None, retention_days=7) -> dict[str, Any]:
    now = time.time()
    return {
        "schema": EVENT_SCHEMA,
        "event_id": str(uuid.uuid4()),
        "device_id": str(device_id),
        "created_at": round(now, 3),
        "window": {"start": round(_f(ts_start), 3), "end": round(_f(ts_end), 3)},
        "target": {"track_id": _i(track_id), "cls": str(cls), "plate": None},
        "score": _i(score),
        "level": level_of(score),
        "reasons": reasons,
        "location": location,
        "clip": clip,
        "state": "pending",
        "expires_at": round(now + retention_days * 86400, 3),
    }


def validate_trackframe(tf: dict) -> list[str]:
    """형식 오류 목록을 돌려준다. 빈 리스트면 정상."""
    errs = []
    if tf.get("schema") != TRACKFRAME_SCHEMA:
        errs.append(f"schema 불일치: {tf.get('schema')}")
    for key in ("ts", "frame_id", "src", "ego", "lanes", "tracks"):
        if key not in tf:
            errs.append(f"필수 필드 누락: {key}")
    for t in tf.get("tracks", []):
        if len(t.get("box", [])) != 4:
            errs.append(f"box 형식 오류: track {t.get('id')}")
    return errs
