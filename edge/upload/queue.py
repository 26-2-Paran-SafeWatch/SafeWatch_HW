"""전송 대기 큐.

터널·주차장·전원 차단에도 이벤트가 사라지지 않도록 디스크에 영속화한다.
서버가 준비되면 pending 항목을 읽어 전송하는 워커만 추가하면 된다.
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS upload_queue (
  event_id     TEXT PRIMARY KEY,
  meta_json    TEXT NOT NULL,
  clip_path    TEXT,
  state        TEXT NOT NULL DEFAULT 'pending',
  retry        INTEGER NOT NULL DEFAULT 0,
  created_at   REAL NOT NULL,
  uploaded_at  REAL,
  clip_deleted INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_state ON upload_queue(state, created_at);
"""


class UploadQueue:
    def __init__(self, db_path="/data/queue.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = sqlite3.connect(db_path, check_same_thread=False)
        self.con.executescript(SCHEMA)
        self.con.commit()

    def enqueue(self, event: dict):
        self.con.execute(
            "INSERT OR REPLACE INTO upload_queue"
            " (event_id, meta_json, clip_path, state, retry, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (event["event_id"], json.dumps(event, ensure_ascii=False),
             None, "pending", 0, time.time()),
        )
        self.con.commit()

    def attach_clip(self, event_id: str, clip_path):
        self.con.execute("UPDATE upload_queue SET clip_path=? WHERE event_id=?",
                         (str(clip_path), event_id))
        self.con.commit()

    def mark_uploaded(self, event_id: str):
        self.con.execute(
            "UPDATE upload_queue SET state='done', uploaded_at=? WHERE event_id=?",
            (time.time(), event_id))
        self.con.commit()

    def pending(self, limit=50):
        return self.con.execute(
            "SELECT event_id, meta_json, clip_path FROM upload_queue"
            " WHERE state='pending' ORDER BY created_at LIMIT ?", (limit,)
        ).fetchall()

    def count(self):
        rows = self.con.execute(
            "SELECT state, COUNT(*) FROM upload_queue GROUP BY state").fetchall()
        return dict(rows)
