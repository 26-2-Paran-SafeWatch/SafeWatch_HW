"""AI → HW 이벤트 입구 (EventSink).

AI 파이프라인(SafeWatch_AI src.main.run)이 이벤트를 확정하면 publish(envelope) 를 부른다.
AI 쪽 계약(src/event/sink.py EventSink): publish 는 바로 돌아와야 하고, 끝날 때 close() 한 번.

publish 에서 하는 일
    1. envelope 검사 → 이벤트 큐에 넣음 (클립 waiting)
    2. 서버 JSON 을 디스크에 저장 (/data/events/<id>/<id>.json, clip=null 상태)
    3. 클립 요청
         trigger.source == "camera" → 링버퍼 저장 (백그라운드, 이벤트 뒤 post 초 기다림)
         trigger.source == 파일 경로 → 작업 스레드에서 ffmpeg 로 원본을 잘라냄
    4. 클립이 끝나면 큐에 붙이고(ready / missing) 디스크 JSON 갱신

예외는 밖으로 내지 않는다 (이벤트 하나 때문에 파이프라인이 멈추면 안 된다). 로그만 남긴다.
전송은 여기서 하지 않는다 — scripts/flush_queue.py (별도 프로세스)가 큐를 비운다.
"""
from __future__ import annotations

import json
import queue as _queue
import threading
from pathlib import Path
from typing import Callable, Optional

from edge.event import clip_path_for, validate_envelope
from edge.storage.file_clip import cut_clip


class EdgeEventSink:
    def __init__(self, event_queue, events_dir="/data/events", *, ring=None,
                 pre_s: float = 10.0, post_s: float = 5.0, location: Optional[Callable[[], dict]] = None,
                 clipper: Callable = cut_clip, log=print):
        """
        event_queue : edge.upload.queue.EventQueue
        ring        : 카메라 입력일 때 쓰는 RingBuffer (없으면 카메라 이벤트는 클립 없이 보냄)
        location    : 지금 위치를 돌려주는 함수 (GPS 붙이기 전까지 None)
        """
        self.q = event_queue
        self.dir = Path(events_dir)
        self.ring = ring
        self.pre_s, self.post_s = pre_s, post_s
        self.location = location
        self.clipper = clipper
        self.log = log
        self.published = 0
        self._jobs: _queue.Queue = _queue.Queue()
        self._worker = threading.Thread(target=self._work, name="clip-worker", daemon=True)
        self._worker.start()
        self._closed = False

    # ------------------------------------------------------------ AI 가 부르는 것
    def publish(self, envelope: dict) -> None:
        try:
            self._publish(envelope)
        except Exception as e:      # noqa: BLE001
            eid = (envelope.get("event") or {}).get("event_id") if isinstance(envelope, dict) else None
            self.log(f"[SINK ] 이벤트 처리 실패 ({eid}): {e}")

    def close(self, timeout: float = 300.0) -> None:
        """남은 클립 작업을 끝까지 기다린다 (영상 파일 테스트가 끝날 때 클립이 다 만들어지게)."""
        if self._closed:
            return
        self._closed = True
        self._jobs.put(None)
        self._worker.join(timeout)
        if self._worker.is_alive():
            self.log(f"[SINK ] 클립 작업이 {timeout:.0f}초 안에 안 끝남 — 남은 건 다음 실행 때 recover 로 정리")

    # ------------------------------------------------------------ 내부
    def _publish(self, envelope: dict) -> None:
        validate_envelope(envelope)
        ev, tr = envelope["event"], envelope["trigger"]
        eid = ev["event_id"]
        loc = self.location() if self.location else None
        if not self.q.enqueue(envelope, location=loc, wait_clip=True):
            self.log(f"[EVENT] {eid} 이미 받은 이벤트, 건너뜀")
            return
        self.published += 1
        self._write_json(eid)
        self.log(f"[EVENT] {eid} score={ev['risk']['score']} {ev['risk'].get('level', '')} "
                 f"{','.join(ev['risk']['types'])} | source={tr.get('source')} t={tr.get('video_sec')}")

        if tr.get("source", "camera") == "camera":
            if self.ring is None:
                self._done(eid, None, "링버퍼 없음")
                return
            started = self.ring.save(self.dir / eid, on_done=lambda info: self._done(eid, info), stem=eid)
            if not started:
                self._done(eid, None, "링버퍼가 직전 클립 저장 중")
        else:
            self._jobs.put((eid, Path(tr["source"]), float(tr["video_sec"])))

    def _work(self):
        while True:
            job = self._jobs.get()
            if job is None:
                return
            eid, src, t = job
            try:
                info = self.clipper(src, t, clip_path_for(self.dir, eid), pre_s=self.pre_s, post_s=self.post_s)
                self._done(eid, info)
            except Exception as e:  # noqa: BLE001
                self._done(eid, None, f"자르기 실패: {e}")

    def _done(self, eid: str, info: Optional[dict], why: str = ""):
        try:
            status = self.q.attach_clip(eid, info)
            self._write_json(eid)
            if info:
                self.log(f"[CLIP ] {eid} {info['duration_sec']}s {info['resolution']} → {info['path']}")
            else:
                self.log(f"[CLIP ] {eid} 클립 없음 ({status}) {why}")
        except Exception as e:  # noqa: BLE001
            self.log(f"[CLIP ] {eid} 큐 반영 실패: {e}")

    def _write_json(self, eid: str):
        d = self.dir / eid
        d.mkdir(parents=True, exist_ok=True)
        tmp = d / f"{eid}.json.tmp"
        tmp.write_text(json.dumps(self.q.server_event(eid), ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(d / f"{eid}.json")   # 쓰다 꺼져도 깨진 파일이 남지 않게
