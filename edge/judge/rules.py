"""임시 위험 판단 규칙.

실제 판단 모델이 나오기 전까지 파이프라인을 관통시키기 위한 최소 구현.
'대상이 화면에서 일정 크기 이상으로 N초 이상 지속되면' 이벤트로 본다.
실내에서도 사람으로 시연이 가능하다.
"""
from __future__ import annotations

import sys
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from edge.schema import make_event, make_reason


class SimpleJudge:
    def __init__(self, device_id="safewatch-001", fps=30,
                 area_ratio=0.06, hold_s=2.0, min_age=10, cooldown_s=20.0):
        self.device_id = device_id
        self.fps = fps
        self.area_ratio = area_ratio
        self.hold_s = hold_s
        self.min_age = min_age
        self.cooldown_s = cooldown_s
        self.hist = defaultdict(lambda: deque(maxlen=max(2, int(fps * hold_s))))
        self.last_fire = {}

    def push(self, tf: dict):
        area_px = tf["src"]["w"] * tf["src"]["h"]
        ts = tf["ts"]
        seen = set()
        event = None

        for t in tf["tracks"]:
            tid = t["id"]
            seen.add(tid)
            x1, y1, x2, y2 = t["box"]
            ratio = max(0.0, x2 - x1) * max(0.0, y2 - y1) / area_px
            self.hist[tid].append((ts, ratio))

            if event is not None:
                continue
            if t["age"] < self.min_age:
                continue
            if len(self.hist[tid]) < self.hist[tid].maxlen:
                continue
            if ts - self.last_fire.get(tid, -1e9) < self.cooldown_s:
                continue

            close = [r for _, r in self.hist[tid] if r >= self.area_ratio]
            if len(close) < self.hist[tid].maxlen * 0.8:
                continue

            self.last_fire[tid] = ts
            peak = max(r for _, r in self.hist[tid])
            event = make_event(
                device_id=self.device_id,
                ts_start=self.hist[tid][0][0], ts_end=ts,
                track_id=tid, cls=t["cls"],
                score=int(min(100, 40 + peak * 300)),
                reasons=[make_reason("TAILGATING", 30,
                                     max_area_ratio=round(peak, 3),
                                     duration_s=round(self.hold_s, 1))],
            )

        for tid in [k for k in self.hist if k not in seen]:
            self.hist.pop(tid, None)
        return event
