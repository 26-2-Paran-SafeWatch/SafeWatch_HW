"""서버 이벤트 스키마.

서버가 받는 JSON 과 엣지가 큐·디스크에 저장하는 JSON 을 같은 모양으로 둔다.
그래서 sender 는 변환 없이 저장된 JSON 을 그대로 보낸다.

    {
      "event_id": "evt_20261103_143022_001",
      "device_id": "safewatch-dev-001",
      "timestamp": "2026-11-03T14:30:22.451+09:00",
      "risk": {"score": 78, "level": "high", "types": ["lane_departure", "weaving"]},
      "indicators": {"observation_window_sec": 10.0, ...},
      "location": {"latitude": .., "longitude": .., "speed_kmh": .., "heading": ..},
      "clip": {"filename": "evt_..._001.mp4", "duration_sec": 15,
               "resolution": "1280x720", "blurred": false},
      "meta": {"model_version": "yolov8n", "rule_version": "risk-v1"}
    }

역할 분담
  - 판단 모델(AI)  : Judgment = {"score", "types", "indicators"} 만 만든다.
  - 엣지(HW)       : event_id, device_id, timestamp, level, location, clip, meta 를 채운다.
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import Callable, Optional
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

# 점수 → 등급. [임시] 서버·앱과 같은 기준인지 확인 필요.
LEVELS = ((70, "high"), (40, "medium"), (0, "low"))

TOP_KEYS = ("event_id", "device_id", "timestamp", "risk", "indicators",
            "location", "clip", "meta")


def level_of(score: int) -> str:
    for threshold, name in LEVELS:
        if score >= threshold:
            return name
    return "low"


def now_kst() -> datetime:
    return datetime.now(KST)


def iso_kst(dt: datetime) -> str:
    """2026-11-03T14:30:22.451+09:00"""
    return dt.astimezone(KST).isoformat(timespec="milliseconds")


def new_event_id(dt: datetime, exists: Callable[[str], bool] = lambda _: False) -> str:
    """evt_YYYYMMDD_HHMMSS_NNN. 같은 초에 여러 건이면 NNN 을 올린다."""
    stem = dt.astimezone(KST).strftime("evt_%Y%m%d_%H%M%S")
    for seq in range(1, 1000):
        eid = f"{stem}_{seq:03d}"
        if not exists(eid):
            return eid
    raise RuntimeError(f"같은 초에 이벤트가 999건을 넘었다: {stem}")


def _num(v):
    """numpy 타입 등을 json 직렬화 가능한 int/float 로. NaN/inf 는 None."""
    if hasattr(v, "item") and callable(v.item):   # numpy 스칼라 → 파이썬 기본형
        v = v.item()
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def validate_judgment(j: dict) -> dict:
    """판단 모델 출력 검사. 문제가 있으면 ValueError."""
    if not isinstance(j, dict):
        raise ValueError("judgment 는 dict 여야 한다")
    score = j.get("score")
    if not isinstance(score, (int, float)) or not 0 <= score <= 100:
        raise ValueError(f"score 는 0~100 숫자여야 한다: {score!r}")
    types = j.get("types")
    if not types or not all(isinstance(t, str) and t for t in types):
        raise ValueError(f"types 는 비어 있지 않은 문자열 리스트여야 한다: {types!r}")
    ind = j.get("indicators", {})
    if not isinstance(ind, dict):
        raise ValueError("indicators 는 dict 여야 한다")
    return j


def build_event(
    judgment: dict,
    *,
    device_id: str,
    model_version: str,
    rule_version: str,
    resolution: str = "1280x720",
    detected_at: Optional[datetime] = None,
    location: Optional[dict] = None,
    exists: Callable[[str], bool] = lambda _: False,
) -> dict:
    """판단 결과 + 엣지 정보 → 서버 스키마 이벤트.

    clip.duration_sec 은 클립 저장이 끝나야 알 수 있으므로 여기서는 None.
    링버퍼 저장이 끝나면 큐가 실제 길이로 채운다.
    """
    validate_judgment(judgment)
    dt = detected_at or now_kst()
    eid = new_event_id(dt, exists)
    score = int(round(judgment["score"]))
    loc = location or {}
    return {
        "event_id": eid,
        "device_id": device_id,
        "timestamp": iso_kst(dt),
        "risk": {
            "score": score,
            "level": level_of(score),
            "types": list(judgment["types"]),
        },
        "indicators": {k: _num(v) for k, v in judgment.get("indicators", {}).items()},
        "location": {
            # GPS 미장착. 붙이기 전까지 null.
            "latitude": loc.get("latitude"),
            "longitude": loc.get("longitude"),
            "speed_kmh": loc.get("speed_kmh"),
            "heading": loc.get("heading"),
        },
        "clip": {
            "filename": f"{eid}.mp4",
            "duration_sec": None,
            "resolution": resolution,
            # 번호판·얼굴 블러 미구현. 구현 전까지 반드시 false.
            "blurred": False,
        },
        "meta": {"model_version": model_version, "rule_version": rule_version},
    }


def validate_event(ev: dict) -> dict:
    """저장·전송 직전 모양 검사."""
    missing = [k for k in TOP_KEYS if k not in ev]
    if missing:
        raise ValueError(f"이벤트 필드 누락: {missing}")
    extra = [k for k in ev if k not in TOP_KEYS]
    if extra:
        raise ValueError(f"서버 스키마에 없는 필드: {extra}")
    if ev["clip"] is not None:
        for k in ("filename", "duration_sec", "resolution", "blurred"):
            if k not in ev["clip"]:
                raise ValueError(f"clip.{k} 누락")
    return ev
