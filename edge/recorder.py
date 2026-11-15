"""AI envelope → 큐 적재 + 링버퍼 클립 저장 (카메라 입력용).

AI 가 이벤트를 넘기면 record(envelope) 만 부르면 된다.

    1. envelope 검사 → 큐에 넣음 (클립 waiting)
    2. 서버 JSON 을 디스크에 저장 (clip=null 상태)
    3. 링버퍼에 클립 저장 요청 (백그라운드, 이벤트 뒤 post 초 기다림)
    4. 저장이 끝나면 큐에 클립을 붙이고(ready / missing) 디스크 JSON 도 갱신

디스크 구조
    /data/events/<event_id>/<event_id>.json   ← 서버로 나갈 JSON (클립 정보까지 채워진 최신본)
    /data/events/<event_id>/<event_id>.mp4    ← 이벤트 전 pre 초 + 후 post 초

영상 파일 입력(trigger.source 가 파일 경로)은 링버퍼 대신 원본에서 잘라야 한다 → 별도 모듈(예정).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from edge.event import validate_envelope


class EventRecorder:
    def __init__(self, ring, queue, events_dir="/data/events", log=print):
        self.ring = ring
        self.queue = queue
        self.dir = Path(events_dir)
        self.log = log

    def _write_json(self, event_id: str):
        d = self.dir / event_id
        d.mkdir(parents=True, exist_ok=True)
        ev = self.queue.server_event(event_id)
        tmp = d / f"{event_id}.json.tmp"
        tmp.write_text(json.dumps(ev, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(d / f"{event_id}.json")   # 쓰다 꺼져도 깨진 파일이 남지 않게

    def record(self, envelope: dict, location: Optional[dict] = None) -> str:
        """envelope 을 기록하고 event_id 를 돌려준다. 이미 받은 event_id 면 다시 처리하지 않는다."""
        validate_envelope(envelope)
        ev = envelope["event"]
        eid = ev["event_id"]
        if not self.queue.enqueue(envelope, location=location, wait_clip=True):
            self.log(f"[EVENT] {eid} 이미 받은 이벤트, 건너뜀")
            return eid
        self._write_json(eid)
        self.log(f"[EVENT] {eid} score={ev['risk']['score']} "
                 f"{ev['risk'].get('level', '')} {','.join(ev['risk']['types'])}")

        def on_done(info):
            status = self.queue.attach_clip(eid, info)
            self._write_json(eid)
            if info:
                self.log(f"[CLIP ] {info['path']} {info['duration_sec']}s")
            else:
                self.log(f"[CLIP ] {eid} 클립 없음 ({status})")

        started = self.ring.save(self.dir / eid, on_done=on_done, stem=eid)
        if not started:
            # 직전 이벤트 클립을 아직 저장 중. 이 이벤트는 클립 없이 보낸다.
            on_done(None)
        return eid
