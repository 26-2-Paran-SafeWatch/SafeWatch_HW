"""이벤트 큐 (Store-and-Forward).

AI 가 넘긴 envelope 을 받아서, 클립이 준비되고 서버로 보낼 때까지 디스크(SQLite)에 둔다.
터널·주차장·전원 차단에도 이벤트가 사라지지 않게 하는 게 목적이다.

이벤트 하나에 상태가 두 줄이다. 메타데이터(JSON)와 클립(영상)은 보내는 시점과 경로가 달라서.

    metadata : pending ──▶ sent
                   └──(4xx 반복)──▶ failed
    clip     : none                         클립을 요청하지 않음
               waiting ──▶ ready ──▶ sent   링버퍼·영상 자르기가 끝나면 ready
                   │          └──(4xx 반복)──▶ failed
                   └──(저장 실패·시간 초과)──▶ missing   클립 없이 메타데이터만 보냄

규칙
  - 메타데이터는 기본적으로 클립을 기다린다(meta_waits_for_clip=True). 서버 JSON 의
    clip 필드(길이·해상도)를 채워서 보내야 하기 때문. 클립이 missing 이면 clip=null 로 보낸다.
  - 클립 파일은 메타데이터가 서버에 등록된 뒤에만 보낸다 (서버가 이벤트에 붙일 수 있게).
  - 실패하면 다음 시도 시각을 늦춘다(10초, 20초, 40초 … 최대 5분). 실패한 건이 큐 맨 앞을 막지 않는다.
  - 4xx(데이터 문제)는 MAX_REJECTS 번이면 failed 로 뺀다. 5xx·네트워크는 횟수에 넣지 않는다.
  - 같은 event_id 가 다시 들어오면 무시한다 (AI 쪽 재시도·중복 방지).

여러 프로세스(파이프라인, 전송 서비스)가 같은 DB 를 쓰도록 WAL 모드와 busy_timeout 을 켠다.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable, Optional

from edge.event import assemble, clip_field, clip_path_for, normalize_envelope, validate_envelope

MAX_REJECTS = 5          # 4xx 가 이만큼 반복되면 failed
BACKOFF_BASE_S = 10.0    # 첫 재시도 대기
BACKOFF_MAX_S = 300.0    # 재시도 대기 상한
KINDS = ("metadata", "clip")

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
  event_id       TEXT PRIMARY KEY,
  created_at     REAL NOT NULL,
  updated_at     REAL NOT NULL,
  envelope_json  TEXT NOT NULL,   -- AI 가 넘긴 그대로 (event / trigger / target)
  location_json  TEXT,            -- HW 가 채움. 없으면 NULL (서버에는 값이 전부 null 인 객체)
  clip_json      TEXT,            -- 서버로 나갈 clip 필드. 없으면 NULL
  clip_path      TEXT,

  meta_status    TEXT NOT NULL,   -- pending | sent | failed
  meta_attempts  INTEGER NOT NULL DEFAULT 0,
  meta_rejects   INTEGER NOT NULL DEFAULT 0,
  meta_next_at   REAL NOT NULL DEFAULT 0,
  meta_error     TEXT,
  meta_sent_at   REAL,

  clip_status    TEXT NOT NULL,   -- none | waiting | ready | sent | failed | missing
  clip_attempts  INTEGER NOT NULL DEFAULT 0,
  clip_rejects   INTEGER NOT NULL DEFAULT 0,
  clip_next_at   REAL NOT NULL DEFAULT 0,
  clip_error     TEXT,
  clip_sent_at   REAL
);
CREATE INDEX IF NOT EXISTS idx_meta ON events(meta_status, meta_next_at, created_at);
CREATE INDEX IF NOT EXISTS idx_clip ON events(clip_status, clip_next_at, created_at);
"""

META_STATES = ("pending", "sent", "failed")
CLIP_STATES = ("none", "waiting", "ready", "sent", "failed", "missing")


def backoff(attempts: int) -> float:
    return min(BACKOFF_MAX_S, BACKOFF_BASE_S * 2 ** max(0, attempts - 1))


class EventQueue:
    def __init__(self, db_path="/data/queue.db", *, meta_waits_for_clip: bool = True,
                 clock: Callable[[], float] = time.time):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.meta_waits_for_clip = meta_waits_for_clip
        self._now = clock
        self.con = sqlite3.connect(str(db_path), check_same_thread=False, timeout=10)
        self._lock = threading.Lock()   # 링버퍼 콜백 스레드와 메인 루프가 같이 씀
        with self._lock:
            self.con.execute("PRAGMA journal_mode=WAL")
            self.con.execute("PRAGMA busy_timeout=5000")
            self.legacy_table = self._move_legacy_table()
            self.con.executescript(SCHEMA)
            self.con.commit()

    def _move_legacy_table(self) -> Optional[str]:
        """예전 큐(state 한 줄짜리) DB 를 열었을 때. 지우지 않고 이름만 바꿔 둔다."""
        cols = [r[1] for r in self.con.execute("PRAGMA table_info(events)")]
        if not cols or "meta_status" in cols:
            return None
        name = f"events_legacy_{int(self._now())}"
        self.con.execute(f"ALTER TABLE events RENAME TO {name}")
        self.con.execute("DROP INDEX IF EXISTS idx_events_state")
        return name

    # ------------------------------------------------------------ 쓰기
    def enqueue(self, envelope: dict, *, location: Optional[dict] = None,
                wait_clip: bool = True) -> bool:
        """AI 가 넘긴 envelope 을 넣는다. 이미 있는 event_id 면 False (아무것도 안 바뀜)."""
        validate_envelope(envelope)
        env = normalize_envelope(envelope)
        eid = env["event"]["event_id"]
        now = self._now()
        with self._lock:
            cur = self.con.execute(
                "INSERT OR IGNORE INTO events (event_id, created_at, updated_at, envelope_json,"
                " location_json, meta_status, clip_status) VALUES (?,?,?,?,?,?,?)",
                (eid, now, now, json.dumps(env, ensure_ascii=False),
                 json.dumps(location, ensure_ascii=False) if location else None,
                 "pending", "waiting" if wait_clip else "none"))
            self.con.commit()
            return cur.rowcount == 1

    def attach_clip(self, event_id: str, info: Optional[dict]) -> str:
        """클립 저장 결과를 붙인다. info 는 probe_clip() 결과({path, duration_sec, resolution}).

        info 가 None 이면 클립을 못 만든 것 → missing (메타데이터는 clip=null 로 나간다).
        돌려주는 값은 바뀐 클립 상태.
        """
        clip = clip_field(event_id, info)
        status = "ready" if clip else "missing"
        with self._lock:
            cur = self.con.execute(
                "UPDATE events SET clip_json=?, clip_path=?, clip_status=?, clip_attempts=0,"
                " clip_rejects=0, clip_next_at=0, clip_error=NULL, updated_at=? WHERE event_id=?",
                (json.dumps(clip, ensure_ascii=False) if clip else None,
                 str(info["path"]) if clip else None, status, self._now(), event_id))
            self.con.commit()
        if cur.rowcount == 0:
            raise KeyError(event_id)
        return status

    def set_location(self, event_id: str, location: Optional[dict]):
        with self._lock:
            self.con.execute(
                "UPDATE events SET location_json=?, updated_at=? WHERE event_id=?",
                (json.dumps(location, ensure_ascii=False) if location else None,
                 self._now(), event_id))
            self.con.commit()

    def mark_sent(self, event_id: str, kind: str):
        p = self._prefix(kind)
        now = self._now()
        with self._lock:
            self.con.execute(
                f"UPDATE events SET {p}_status='sent', {p}_sent_at=?, {p}_error=NULL,"
                f" updated_at=? WHERE event_id=?", (now, now, event_id))
            self.con.commit()

    def mark_failed(self, event_id: str, kind: str, error: str, *, retryable: bool) -> str:
        """보내기 실패. retryable=True 는 5xx·네트워크(횟수에 안 넣음), False 는 4xx·형식 오류.

        다음 시도 시각을 늦추고, 4xx 가 MAX_REJECTS 번이면 failed. 바뀐 상태를 돌려준다.
        """
        p = self._prefix(kind)
        now = self._now()
        with self._lock:
            row = self.con.execute(
                f"SELECT {p}_attempts, {p}_rejects, {p}_status FROM events WHERE event_id=?",
                (event_id,)).fetchone()
            if row is None:
                raise KeyError(event_id)
            attempts, rejects, status = row[0] + 1, row[1] + (0 if retryable else 1), row[2]
            if rejects >= MAX_REJECTS:
                status = "failed"
            self.con.execute(
                f"UPDATE events SET {p}_attempts=?, {p}_rejects=?, {p}_status=?, {p}_next_at=?,"
                f" {p}_error=?, updated_at=? WHERE event_id=?",
                (attempts, rejects, status, now + backoff(attempts), str(error)[:500], now, event_id))
            self.con.commit()
        return status

    def recover(self, *, clip_timeout_s: float = 60.0, events_dir: Optional[Path] = None,
                probe: Optional[Callable[[Path], dict]] = None) -> int:
        """전원이 꺼져 waiting 에 멈춘 클립을 정리한다. 전송 서비스 시작 시 한 번 부르면 된다.

        clip_timeout_s 보다 오래 waiting 인 것 중 mp4 가 남아 있으면 길이를 재서 ready,
        없으면 missing. 정리한 건수를 돌려준다.
        """
        cutoff = self._now() - clip_timeout_s
        with self._lock:
            rows = self.con.execute(
                "SELECT event_id FROM events WHERE clip_status='waiting' AND created_at < ?",
                (cutoff,)).fetchall()
        for (eid,) in rows:
            info = None
            if events_dir is not None and probe is not None:
                mp4 = clip_path_for(events_dir, eid)
                if mp4.exists():
                    try:
                        info = probe(mp4)
                    except Exception:
                        info = None
            self.attach_clip(eid, info)
        return len(rows)

    def purge_done(self, older_than_s: float) -> list[tuple[str, Optional[str]]]:
        """다 끝난 이벤트(메타 sent/failed, 클립 none/sent/failed/missing)를 큐에서 지운다.

        지운 (event_id, clip_path) 목록을 돌려준다. 디스크 파일 정리는 호출한 쪽이 한다.
        """
        cutoff = self._now() - older_than_s
        cond = ("meta_status IN ('sent','failed') AND clip_status IN ('none','sent','failed','missing')"
                " AND updated_at < ?")
        with self._lock:
            rows = self.con.execute(
                f"SELECT event_id, clip_path FROM events WHERE {cond}", (cutoff,)).fetchall()
            self.con.execute(f"DELETE FROM events WHERE {cond}", (cutoff,))
            self.con.commit()
        return [(r[0], r[1]) for r in rows]

    def close(self):
        with self._lock:
            self.con.close()

    # ------------------------------------------------------------ 읽기
    def pending(self, kind: str, limit: int = 50) -> list[dict]:
        """지금 보낼 차례인 것. 생성 순서대로.

        metadata → [{"event_id", "payload"(서버 JSON)}]
        clip     → [{"event_id", "clip_path", "payload"(서버 JSON)}]
        """
        self._prefix(kind)
        now = self._now()
        if kind == "metadata":
            wait = "AND clip_status != 'waiting'" if self.meta_waits_for_clip else ""
            sql = (f"SELECT event_id FROM events WHERE meta_status='pending' AND meta_next_at <= ?"
                   f" {wait} ORDER BY created_at LIMIT ?")
        else:
            sql = ("SELECT event_id FROM events WHERE clip_status='ready' AND meta_status='sent'"
                   " AND clip_next_at <= ? ORDER BY created_at LIMIT ?")
        with self._lock:
            ids = [r[0] for r in self.con.execute(sql, (now, limit)).fetchall()]
        out = []
        for eid in ids:
            row = self.get(eid)
            item = {"event_id": eid, "payload": self.server_event(eid)}
            if kind == "clip":
                item["clip_path"] = row["clip_path"]
            out.append(item)
        return out

    def get(self, event_id: str) -> Optional[dict]:
        """한 건의 전체 상태 (디버그·도구용)."""
        with self._lock:
            cur = self.con.execute("SELECT * FROM events WHERE event_id=?", (event_id,))
            row = cur.fetchone()
            names = [d[0] for d in cur.description]
        if row is None:
            return None
        d = dict(zip(names, row))
        d["envelope"] = json.loads(d.pop("envelope_json"))
        d["location"] = json.loads(d.pop("location_json")) if d["location_json"] else None
        d["clip"] = json.loads(d.pop("clip_json")) if d["clip_json"] else None
        return d

    def exists(self, event_id: str) -> bool:
        with self._lock:
            return self.con.execute(
                "SELECT 1 FROM events WHERE event_id=?", (event_id,)).fetchone() is not None

    def server_event(self, event_id: str) -> dict:
        """서버로 보낼 JSON (event + location + clip)."""
        d = self.get(event_id)
        if d is None:
            raise KeyError(event_id)
        return assemble(d["envelope"]["event"], d["location"], d["clip"])

    def envelope(self, event_id: str) -> dict:
        d = self.get(event_id)
        if d is None:
            raise KeyError(event_id)
        return d["envelope"]

    def stats(self) -> dict:
        """{"metadata": {상태: 건수}, "clip": {상태: 건수}, "total": n}"""
        with self._lock:
            meta = dict(self.con.execute(
                "SELECT meta_status, COUNT(*) FROM events GROUP BY meta_status").fetchall())
            clip = dict(self.con.execute(
                "SELECT clip_status, COUNT(*) FROM events GROUP BY clip_status").fetchall())
            total = self.con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        return {"metadata": meta, "clip": clip, "total": total}

    @staticmethod
    def _prefix(kind: str) -> str:
        if kind not in KINDS:
            raise ValueError(f"kind 는 {KINDS} 중 하나: {kind!r}")
        return "meta" if kind == "metadata" else "clip"
