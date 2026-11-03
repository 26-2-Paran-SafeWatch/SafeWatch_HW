"""전송 대기 큐.

터널·주차장·전원 차단에도 이벤트가 사라지지 않도록 디스크(SQLite)에 영속화한다.
저장하는 JSON 은 서버 스키마(edge/event.py)와 같은 모양이다.

상태 흐름
    wait_clip ──(클립 저장 완료 / 실패 / 시간초과)──▶ ready ──(전송 성공)──▶ done
                                                       └──(4xx 반복)──▶ failed

클립 길이(clip.duration_sec)는 저장이 끝나야 알 수 있어서, 이벤트는 wait_clip 으로
들어왔다가 클립 정보가 채워진 뒤에 ready 가 된다. sender 는 ready 만 보낸다.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

from edge.event import validate_event

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
  event_id     TEXT PRIMARY KEY,
  event_json   TEXT NOT NULL,
  clip_path    TEXT,
  state        TEXT NOT NULL,
  retry        INTEGER NOT NULL DEFAULT 0,
  last_error   TEXT,
  created_at   REAL NOT NULL,
  uploaded_at  REAL
);
CREATE INDEX IF NOT EXISTS idx_events_state ON events(state, created_at);
"""

MAX_RETRY = 5   # 4xx 가 이만큼 반복되면 failed 로 빼서 큐가 막히지 않게 한다


class UploadQueue:
    def __init__(self, db_path="/data/queue.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = threading.Lock()   # 링버퍼 콜백 스레드와 메인 루프가 같이 씀
        with self._lock:
            self.con.executescript(SCHEMA)
            self.con.commit()

    # ------------------------------------------------------------ 쓰기
    def enqueue(self, event: dict, wait_clip: bool = True):
        validate_event(event)
        state = "wait_clip" if wait_clip else "ready"
        with self._lock:
            self.con.execute(
                "INSERT INTO events (event_id, event_json, state, created_at)"
                " VALUES (?,?,?,?)",
                (event["event_id"], json.dumps(event, ensure_ascii=False),
                 state, time.time()),
            )
            self.con.commit()

    def set_clip(self, event_id: str, clip_info: Optional[dict]) -> dict:
        """클립 저장 결과를 이벤트 JSON 에 반영하고 ready 로 바꾼다.

        clip_info 가 None 이면(저장 실패·링버퍼 사용 중) clip 을 null 로 두고 보낸다.
        이벤트 자체는 클립이 없어도 의미가 있기 때문.
        """
        with self._lock:
            row = self.con.execute(
                "SELECT event_json FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
            if row is None:
                raise KeyError(event_id)
            ev = json.loads(row[0])
            if clip_info is None:
                ev["clip"] = None
                path = None
            else:
                if ev.get("clip") is None:
                    ev["clip"] = {"filename": f"{event_id}.mp4", "blurred": False}
                ev["clip"]["duration_sec"] = clip_info["duration_sec"]
                ev["clip"]["resolution"] = clip_info["resolution"]
                path = str(clip_info["path"])
            self.con.execute(
                "UPDATE events SET event_json=?, clip_path=?, state='ready'"
                " WHERE event_id=?",
                (json.dumps(ev, ensure_ascii=False), path, event_id),
            )
            self.con.commit()
        return ev

    def mark_done(self, event_id: str):
        with self._lock:
            self.con.execute(
                "UPDATE events SET state='done', uploaded_at=?, last_error=NULL"
                " WHERE event_id=?", (time.time(), event_id))
            self.con.commit()

    def record_error(self, event_id: str, error: str) -> str:
        """서버가 거절했을 때. 재시도 횟수를 올리고, 한도를 넘으면 failed."""
        with self._lock:
            self.con.execute(
                "UPDATE events SET retry=retry+1, last_error=?,"
                " state=CASE WHEN retry+1 >= ? THEN 'failed' ELSE state END"
                " WHERE event_id=?", (error[:500], MAX_RETRY, event_id))
            self.con.commit()
            return self.con.execute(
                "SELECT state FROM events WHERE event_id=?", (event_id,)
            ).fetchone()[0]

    def recover(self, max_wait_s: float = 60.0, clip_dir: Optional[Path] = None,
                probe=None) -> int:
        """전원이 꺼져 wait_clip 에 멈춘 이벤트를 ready 로 풀어준다.

        mp4 가 남아 있으면 길이를 재서 채우고, 없으면 clip=null 로 보낸다.
        파이프라인 시작 시 한 번 부르면 된다.
        """
        cutoff = time.time() - max_wait_s
        with self._lock:
            rows = self.con.execute(
                "SELECT event_id FROM events WHERE state='wait_clip' AND created_at < ?",
                (cutoff,)).fetchall()
        n = 0
        for (eid,) in rows:
            info = None
            if clip_dir is not None and probe is not None:
                mp4 = Path(clip_dir) / eid / f"{eid}.mp4"
                if mp4.exists():
                    try:
                        info = probe(mp4)
                    except Exception:
                        info = None
            self.set_clip(eid, info)
            n += 1
        return n

    def close(self):
        with self._lock:
            self.con.close()

    # ------------------------------------------------------------ 읽기
    def exists(self, event_id: str) -> bool:
        with self._lock:
            return self.con.execute(
                "SELECT 1 FROM events WHERE event_id=?", (event_id,)
            ).fetchone() is not None

    def get(self, event_id: str) -> Optional[dict]:
        with self._lock:
            row = self.con.execute(
                "SELECT event_json FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
        return json.loads(row[0]) if row else None

    def ready(self, limit=50):
        """전송할 차례인 이벤트. [(event_id, event_dict, clip_path), ...]"""
        with self._lock:
            rows = self.con.execute(
                "SELECT event_id, event_json, clip_path FROM events"
                " WHERE state='ready' ORDER BY created_at LIMIT ?", (limit,)
            ).fetchall()
        return [(eid, json.loads(js), cp) for eid, js, cp in rows]

    def count(self):
        with self._lock:
            rows = self.con.execute(
                "SELECT state, COUNT(*) FROM events GROUP BY state").fetchall()
        return dict(rows)
