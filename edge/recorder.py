"""판단 결과 → 이벤트 기록.

새 파이프라인은 판단 모델이 Judgment 를 내면 recorder.record(judgment) 만 부르면 된다.
이벤트 JSON 생성, 디스크 저장, 큐 적재, 링버퍼 클립 저장, 클립 정보 반영을
여기서 한 번에 처리한다.

디스크 구조
    /data/events/<event_id>/<event_id>.json   ← 큐에 들어간 것과 같은 JSON
    /data/events/<event_id>/<event_id>.mp4    ← 이벤트 전 10초 + 후 5초
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from edge.event import build_event

DEVICE_ID = os.environ.get("SAFEWATCH_DEVICE_ID", "safewatch-dev-001")


class EventRecorder:
    def __init__(self, ring, queue, events_dir="/data/events", *,
                 device_id: str = DEVICE_ID, model_version: str,
                 rule_version: str, resolution: str = "1280x720",
                 log=print):
        self.ring = ring
        self.queue = queue
        self.dir = Path(events_dir)
        self.device_id = device_id
        self.model_version = model_version
        self.rule_version = rule_version
        self.resolution = resolution
        self.log = log

    def _write_json(self, ev: dict):
        d = self.dir / ev["event_id"]
        d.mkdir(parents=True, exist_ok=True)
        tmp = d / f"{ev['event_id']}.json.tmp"
        tmp.write_text(json.dumps(ev, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(d / f"{ev['event_id']}.json")   # 쓰다 꺼져도 깨진 파일이 남지 않게

    def record(self, judgment: dict, location: Optional[dict] = None) -> dict:
        ev = build_event(
            judgment,
            device_id=self.device_id,
            model_version=self.model_version,
            rule_version=self.rule_version,
            resolution=self.resolution,
            location=location,
            exists=self.queue.exists,
        )
        eid = ev["event_id"]
        self._write_json(ev)
        self.queue.enqueue(ev, wait_clip=True)
        self.log(f"[EVENT] {eid} score={ev['risk']['score']} "
                 f"{ev['risk']['level']} {','.join(ev['risk']['types'])}")

        def on_done(info):
            done = self.queue.set_clip(eid, info)
            self._write_json(done)
            if info:
                self.log(f"[CLIP ] {info['path']} {info['duration_sec']}s")
            else:
                self.log(f"[CLIP ] {eid} 클립 없음 (저장 실패)")

        started = self.ring.save(self.dir / eid, on_done=on_done, stem=eid)
        if not started:
            # 직전 이벤트 클립을 아직 저장 중. 이 이벤트는 클립 없이 보낸다.
            on_done(None)
        return ev
