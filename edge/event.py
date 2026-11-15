"""AI → HW 이벤트 전달 형식(envelope)과 서버로 보내는 JSON.

역할
  - AI(SafeWatch_AI) : 판단 후 envelope 하나를 넘긴다. 서버로 나갈 event 는 AI 책임.
  - HW(SafeWatch_HW) : 영상 클립을 자르고 location·clip 을 붙여 서버로 보낸다.
                       event 안의 값은 고치지 않는다.

AI 가 넘기는 envelope
    {
      "event": {                                    ← 서버로 그대로 나감
        "event_id": "evt_20261115_143022_001",
        "device_id": "safewatch-dev-001",
        "timestamp": "2026-11-15T14:30:22.451+09:00",
        "risk": {"score": 60, "level": "medium", "types": ["weaving", "swerving"]},
        "indicators": {"observation_window_sec": 10.0, ...},
        "meta": {"model_version": "...", "rule_version": "..."}
      },
      "trigger": {                                  ← 클립 자를 기준 (서버로 안 나감)
        "epoch": 1794725422.451,      판정이 켜진 프레임 시각 (time.time() 기준)
        "frame_id": 1234,
        "source": "camera",           카메라면 "camera", 영상 파일이면 그 경로
        "video_sec": null             영상 파일일 때만 영상 안 시각(초)
      },
      "target": {                                   ← 참고 정보 (서버로 안 나감)
        "track_id": 3, "box": [x1, y1, x2, y2], "frame_size": [w, h],
        "tracked_duration_sec": 12.4, "avg_detection_confidence": 0.81
      }
    }

서버로 보내는 JSON = event + location + clip (TOP_KEYS 순서)
    location : GPS 미장착이라 값은 전부 null
    clip     : {"filename", "duration_sec", "resolution", "blurred"}. 클립이 없으면 null
"""
from __future__ import annotations

import copy
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

TOP_KEYS = ("event_id", "device_id", "timestamp", "risk", "indicators",
            "location", "clip", "meta")
AI_KEYS = ("event_id", "device_id", "timestamp", "risk", "indicators", "meta")
HW_KEYS = ("location", "clip")
CLIP_KEYS = ("filename", "duration_sec", "resolution", "blurred")
LOCATION_KEYS = ("latitude", "longitude", "speed_kmh", "heading")

EVENT_ID_RE = re.compile(r"^evt_\d{8}_\d{6}_\d{3}$")


def iso_kst(dt: datetime) -> str:
    """2026-11-03T14:30:22.451+09:00"""
    return dt.astimezone(KST).isoformat(timespec="milliseconds")


def clean(v: Any) -> Any:
    """numpy 값 등을 json 직렬화 가능한 기본형으로. NaN/inf 는 None."""
    if isinstance(v, dict):
        return {str(k): clean(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [clean(x) for x in v]
    if hasattr(v, "item") and callable(v.item):   # numpy 스칼라
        v = v.item()
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


# ---------------------------------------------------------------- envelope (AI → HW)

def validate_envelope(env: dict) -> dict:
    """AI 가 넘긴 envelope 검사. 문제가 있으면 ValueError."""
    if not isinstance(env, dict):
        raise ValueError("envelope 는 dict 여야 한다")
    ev = env.get("event")
    if not isinstance(ev, dict):
        raise ValueError("envelope.event 가 없다")
    missing = [k for k in AI_KEYS if k not in ev]
    if missing:
        raise ValueError(f"event 필드 누락: {missing}")
    if not isinstance(ev["event_id"], str) or not EVENT_ID_RE.match(ev["event_id"]):
        raise ValueError(f"event_id 형식이 evt_YYYYMMDD_HHMMSS_NNN 이 아니다: {ev['event_id']!r}")
    risk = ev["risk"]
    if not isinstance(risk, dict):
        raise ValueError("event.risk 는 dict 여야 한다")
    score = risk.get("score")
    if not isinstance(score, (int, float)) or isinstance(score, bool) or not 0 <= score <= 100:
        raise ValueError(f"risk.score 는 0~100 숫자여야 한다: {score!r}")
    types = risk.get("types")
    if not types or not all(isinstance(t, str) and t for t in types):
        raise ValueError(f"risk.types 는 비어 있지 않은 문자열 리스트여야 한다: {types!r}")
    if not isinstance(ev["indicators"], dict):
        raise ValueError("event.indicators 는 dict 여야 한다")

    tr = env.get("trigger")
    if not isinstance(tr, dict):
        raise ValueError("envelope.trigger 가 없다")
    epoch = tr.get("epoch")
    if not isinstance(epoch, (int, float)) or isinstance(epoch, bool) or epoch <= 0:
        raise ValueError(f"trigger.epoch 는 양수(초)여야 한다: {epoch!r}")
    src = tr.get("source", "camera")
    if not isinstance(src, str) or not src:
        raise ValueError(f"trigger.source 가 잘못됐다: {src!r}")
    if src != "camera" and not isinstance(tr.get("video_sec"), (int, float)):
        raise ValueError("영상 파일 입력이면 trigger.video_sec 이 있어야 한다")
    return env


def normalize_envelope(env: dict) -> dict:
    """저장용 사본. 숫자 정리 + AI 가 아직 event 안에 두는 값 정리.

    - target_vehicle 은 event 밖 target 으로 옮긴다 (서버와 합의 전까지 안 보냄).
    - location·clip 은 HW 몫이라 event 안에 있으면 버린다.
    """
    env = clean(copy.deepcopy(env))
    ev = env["event"]
    tv = ev.pop("target_vehicle", None)
    if tv is not None and not env.get("target"):
        env["target"] = tv
    for k in HW_KEYS:
        ev.pop(k, None)
    env.setdefault("target", None)
    env["trigger"].setdefault("source", "camera")
    env["trigger"].setdefault("video_sec", None)
    return env


# ---------------------------------------------------------------- 서버 JSON (HW → 서버)

def empty_location() -> dict:
    # GPS 미장착. 붙이기 전까지 값은 null.
    return {k: None for k in LOCATION_KEYS}


def clip_field(event_id: str, info: Optional[dict]) -> Optional[dict]:
    """클립 저장 결과(probe_clip 결과) → 서버 clip 필드."""
    if info is None:
        return None
    return {
        "filename": f"{event_id}.mp4",
        "duration_sec": int(info["duration_sec"]),
        "resolution": str(info["resolution"]),
        # 번호판·얼굴 블러 미구현. 구현 전까지 반드시 false.
        "blurred": bool(info.get("blurred", False)),
    }


def assemble(event: dict, location: Optional[dict], clip: Optional[dict]) -> dict:
    """event + location + clip → 서버 JSON. 키 순서는 TOP_KEYS."""
    loc = empty_location()
    if location:
        loc.update({k: location.get(k) for k in LOCATION_KEYS})
    out = {k: event[k] for k in AI_KEYS}
    out["location"] = loc
    out["clip"] = clip
    return validate_event({k: out[k] for k in TOP_KEYS})


def validate_event(ev: dict) -> dict:
    """서버로 보내기 직전 모양 검사."""
    missing = [k for k in TOP_KEYS if k not in ev]
    if missing:
        raise ValueError(f"이벤트 필드 누락: {missing}")
    extra = [k for k in ev if k not in TOP_KEYS]
    if extra:
        raise ValueError(f"서버 스키마에 없는 필드: {extra}")
    if ev["clip"] is not None:
        for k in CLIP_KEYS:
            if k not in ev["clip"]:
                raise ValueError(f"clip.{k} 누락")
    return ev


def clip_path_for(events_dir: Path, event_id: str) -> Path:
    """디스크 구조: <events_dir>/<event_id>/<event_id>.mp4"""
    return Path(events_dir) / event_id / f"{event_id}.mp4"
