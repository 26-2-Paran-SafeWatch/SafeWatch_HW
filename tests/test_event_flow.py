"""envelope · 이벤트 큐 · 전송 흐름 테스트 (카메라 없이 실행 가능, ffmpeg 필요).

    python -m unittest tests.test_event_flow -v
"""
from __future__ import annotations

import copy
import json
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import unittest
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from edge.event import TOP_KEYS, assemble, normalize_envelope, validate_envelope
from edge.recorder import EventRecorder
from edge.storage.ring import probe_clip
from edge.upload.queue import BACKOFF_BASE_S, MAX_REJECTS, EventQueue
from edge.upload.sender import UploadSender

# 서버 스키마 예시 (주민규 님 제공)
SERVER_EXAMPLE = {
    "event_id": "evt_20261103_143022_001",
    "device_id": "safewatch-dev-001",
    "timestamp": "2026-11-03T14:30:22.451+09:00",
    "risk": {"score": 78, "level": "high", "types": ["lane_departure", "weaving"]},
    "indicators": {"observation_window_sec": 10.0, "lane_departure_duration_sec": 2.8,
                   "direction_changes_count": 4},
    "location": {"latitude": 37.32, "longitude": 126.83, "speed_kmh": 62.5, "heading": 271.3},
    "clip": {"filename": "evt_20261103_143022_001.mp4", "duration_sec": 15,
             "resolution": "1280x720", "blurred": True},
    "meta": {"model_version": "yolov8n", "rule_version": "risk-v1"},
}


def envelope(eid="evt_20261115_143022_001", **trigger):
    """AI 가 넘길 envelope (계약 예시와 같은 모양)."""
    tr = {"epoch": 1794725422.451, "frame_id": 1234, "source": "camera", "video_sec": None}
    tr.update(trigger)
    return {
        "event": {
            "event_id": eid,
            "device_id": "safewatch-dev-001",
            "timestamp": "2026-11-15T14:30:22.451+09:00",
            "risk": {"score": 60, "level": "medium", "types": ["weaving", "swerving"]},
            "indicators": {"observation_window_sec": 10.0, "weaving": 0.82},
            "meta": {"model_version": "yolov8n-safewatch", "rule_version": "risk-v1"},
        },
        "trigger": tr,
        "target": {"track_id": 3, "box": [252, 164, 348, 238], "frame_size": [1280, 720],
                   "tracked_duration_sec": 12.4, "avg_detection_confidence": 0.81},
    }


def shape(d):
    """값은 버리고 키 구조만."""
    if isinstance(d, dict):
        return {k: shape(v) for k, v in d.items()}
    return None


def make_mp4(path: Path, seconds=3, size="1280x720"):
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "lavfi",
                    "-i", f"testsrc=size={size}:rate=30:duration={seconds}",
                    "-c:v", "libx264", "-pix_fmt", "yuv420p", str(path)], check=True)


class Clock:
    def __init__(self, t=1_800_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


class FakeRing:
    """링버퍼 대역. 즉시 mp4 를 만들고 on_done 을 부른다."""
    def __init__(self, seconds=3, busy=False, fail=False):
        self.seconds, self.busy, self.fail = seconds, busy, fail

    def save(self, out_dir, on_done=None, stem="clip"):
        if self.busy:
            return False
        out_dir.mkdir(parents=True, exist_ok=True)
        info = None
        if not self.fail:
            mp4 = out_dir / f"{stem}.mp4"
            make_mp4(mp4, self.seconds)
            info = probe_clip(mp4)
        on_done(info)
        return True


class Server:
    """POST /api/events 받는 가짜 서버."""
    def __init__(self, status=200):
        self.received, self.status = [], status
        outer = self

        class H(BaseHTTPRequestHandler):
            def do_POST(self):
                n = int(self.headers["Content-Length"])
                outer.received.append((self.path, json.loads(self.rfile.read(n))))
                self.send_response(outer.status)
                self.end_headers()
                self.wfile.write(b'{"status":"new"}')

            def log_message(self, *a):
                pass

        self.httpd = HTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.httpd.server_port}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.clock = Clock()
        self.q = EventQueue(self.tmp / "queue.db", clock=self.clock)

    def tearDown(self):
        self.q.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def clip_info(self, eid, seconds=3):
        mp4 = self.tmp / "events" / eid / f"{eid}.mp4"
        mp4.parent.mkdir(parents=True, exist_ok=True)
        make_mp4(mp4, seconds)
        return probe_clip(mp4)


class TestEnvelope(Base):
    def test_contract_example_passes(self):
        validate_envelope(envelope())

    def test_rejects_bad_input(self):
        bad = []
        e = envelope(); del e["trigger"]; bad.append(e)
        e = envelope(); e["event"]["event_id"] = "evt-123"; bad.append(e)
        e = envelope(); e["event"]["risk"]["score"] = 120; bad.append(e)
        e = envelope(); e["event"]["risk"]["types"] = []; bad.append(e)
        e = envelope(); del e["event"]["meta"]; bad.append(e)
        bad.append(envelope(source="/home/safewatch/models/a30.mp4"))   # 파일인데 video_sec 없음
        for b in bad:
            with self.assertRaises(ValueError):
                validate_envelope(b)

    def test_file_source_with_video_sec(self):
        validate_envelope(envelope(source="/home/safewatch/models/a30.mp4", video_sec=22.1))

    def test_target_vehicle_moved_out_and_hw_fields_dropped(self):
        e = envelope()
        del e["target"]
        e["event"]["target_vehicle"] = {"track_id": 7}
        e["event"]["clip"] = {"filename": "x.mp4"}
        e["event"]["location"] = {"latitude": 1}
        n = normalize_envelope(e)
        self.assertEqual(n["target"], {"track_id": 7})
        for k in ("target_vehicle", "clip", "location"):
            self.assertNotIn(k, n["event"])
        self.assertIn("target_vehicle", e["event"])      # 원본은 그대로

    def test_numpy_values_serializable(self):
        import numpy as np
        e = envelope()
        e["event"]["indicators"] = {"a": np.float32(1.5), "b": np.int64(3), "c": float("nan")}
        e["trigger"]["epoch"] = float(np.float64(1794725422.4))
        self.q.enqueue(e)
        ind = self.q.server_event(e["event"]["event_id"])["indicators"]
        self.assertEqual(ind, {"a": 1.5, "b": 3, "c": None})


class TestServerJson(Base):
    def test_same_shape_as_server_example(self):
        info = {"path": "x.mp4", "duration_sec": 15, "resolution": "1280x720"}
        e = envelope()
        self.q.enqueue(e, location={"latitude": 37.3, "longitude": 126.8,
                                    "speed_kmh": 60, "heading": 270})
        self.q.attach_clip(e["event"]["event_id"], info)
        out = self.q.server_event(e["event"]["event_id"])
        self.assertEqual(list(out), list(TOP_KEYS))
        self.assertEqual(shape(out["clip"]), shape(SERVER_EXAMPLE["clip"]))
        self.assertEqual(shape(out["location"]), shape(SERVER_EXAMPLE["location"]))
        self.assertEqual(shape(out["risk"]), shape(SERVER_EXAMPLE["risk"]))
        self.assertIs(out["clip"]["blurred"], False)          # 블러 미구현
        self.assertEqual(out["clip"]["filename"], "evt_20261115_143022_001.mp4")

    def test_ai_values_unchanged(self):
        e = envelope()
        self.q.enqueue(e)
        out = self.q.server_event(e["event"]["event_id"])
        for k in ("event_id", "device_id", "timestamp", "risk", "indicators", "meta"):
            self.assertEqual(out[k], e["event"][k])

    def test_no_gps_no_clip(self):
        out = assemble(envelope()["event"], None, None)
        self.assertEqual(out["location"], {"latitude": None, "longitude": None,
                                           "speed_kmh": None, "heading": None})
        self.assertIsNone(out["clip"])


class TestQueue(Base):
    eid = "evt_20261115_143022_001"

    def test_duplicate_ignored(self):
        self.assertTrue(self.q.enqueue(envelope()))
        self.assertFalse(self.q.enqueue(envelope()))
        self.assertEqual(self.q.stats()["total"], 1)

    def test_metadata_waits_for_clip(self):
        self.q.enqueue(envelope())
        self.assertEqual(self.q.pending("metadata"), [])
        self.q.attach_clip(self.eid, {"path": "x.mp4", "duration_sec": 15, "resolution": "1280x720"})
        items = self.q.pending("metadata")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["payload"]["clip"]["duration_sec"], 15)

    def test_metadata_without_waiting_option(self):
        q = EventQueue(self.tmp / "q2.db", meta_waits_for_clip=False, clock=self.clock)
        q.enqueue(envelope())
        self.assertEqual(len(q.pending("metadata")), 1)
        q.close()

    def test_missing_clip_sends_null(self):
        self.q.enqueue(envelope())
        self.assertEqual(self.q.attach_clip(self.eid, None), "missing")
        items = self.q.pending("metadata")
        self.assertIsNone(items[0]["payload"]["clip"])
        self.assertEqual(self.q.pending("clip"), [])

    def test_no_clip_requested(self):
        self.q.enqueue(envelope(), wait_clip=False)
        self.assertEqual(len(self.q.pending("metadata")), 1)
        self.assertEqual(self.q.get(self.eid)["clip_status"], "none")

    def test_clip_only_after_metadata_sent(self):
        self.q.enqueue(envelope())
        self.q.attach_clip(self.eid, {"path": "x.mp4", "duration_sec": 15, "resolution": "1280x720"})
        self.assertEqual(self.q.pending("clip"), [])
        self.q.mark_sent(self.eid, "metadata")
        clips = self.q.pending("clip")
        self.assertEqual([c["clip_path"] for c in clips], ["x.mp4"])
        self.q.mark_sent(self.eid, "clip")
        self.assertEqual(self.q.pending("clip"), [])

    def test_retry_backoff_and_failed(self):
        self.q.enqueue(envelope(), wait_clip=False)
        st = self.q.mark_failed(self.eid, "metadata", "net", retryable=True)
        self.assertEqual(st, "pending")
        self.assertEqual(self.q.pending("metadata"), [])          # 아직 대기 중
        self.clock.t += BACKOFF_BASE_S
        self.assertEqual(len(self.q.pending("metadata")), 1)      # 시간이 되면 다시
        for i in range(MAX_REJECTS):
            st = self.q.mark_failed(self.eid, "metadata", "HTTP 400", retryable=False)
        self.assertEqual(st, "failed")
        self.assertEqual(self.q.get(self.eid)["meta_rejects"], MAX_REJECTS)

    def test_failed_one_does_not_block_others(self):
        self.q.enqueue(envelope("evt_20261115_143022_001"), wait_clip=False)
        self.q.enqueue(envelope("evt_20261115_143023_001"), wait_clip=False)
        self.q.mark_failed("evt_20261115_143022_001", "metadata", "x", retryable=True)
        ids = [i["event_id"] for i in self.q.pending("metadata")]
        self.assertEqual(ids, ["evt_20261115_143023_001"])

    def test_recover_after_power_loss(self):
        a, b = "evt_20261115_143022_001", "evt_20261115_143023_001"
        self.q.enqueue(envelope(a))
        self.q.enqueue(envelope(b))
        make_dir = self.tmp / "events" / a
        make_dir.mkdir(parents=True)
        make_mp4(make_dir / f"{a}.mp4", 2)
        self.assertEqual(self.q.recover(events_dir=self.tmp / "events", probe=probe_clip), 0)  # 아직 안 늦음
        self.clock.t += 61
        n = self.q.recover(events_dir=self.tmp / "events", probe=probe_clip)
        self.assertEqual(n, 2)
        self.assertEqual(self.q.get(a)["clip_status"], "ready")
        self.assertEqual(self.q.get(a)["clip"]["duration_sec"], 2)
        self.assertEqual(self.q.get(b)["clip_status"], "missing")

    def test_survives_reopen(self):
        self.q.enqueue(envelope())
        self.q.close()
        self.q = EventQueue(self.tmp / "queue.db", clock=self.clock)
        self.assertTrue(self.q.exists(self.eid))
        self.assertEqual(self.q.get(self.eid)["clip_status"], "waiting")

    def test_legacy_db_moved_aside(self):
        db = self.tmp / "old.db"
        con = sqlite3.connect(db)
        con.executescript("CREATE TABLE events (event_id TEXT PRIMARY KEY, event_json TEXT, state TEXT);"
                          "INSERT INTO events VALUES ('evt_old', '{}', 'done');")
        con.commit()
        con.close()
        q = EventQueue(db, clock=self.clock)
        self.assertTrue(q.legacy_table.startswith("events_legacy_"))
        self.assertTrue(q.enqueue(envelope()))
        old = q.con.execute(f"SELECT COUNT(*) FROM {q.legacy_table}").fetchone()[0]
        self.assertEqual(old, 1)                                  # 예전 데이터는 지우지 않음
        q.close()

    def test_stats_and_purge(self):
        self.q.enqueue(envelope("evt_20261115_143022_001"), wait_clip=False)
        self.q.enqueue(envelope("evt_20261115_143023_001"))
        self.q.mark_sent("evt_20261115_143022_001", "metadata")
        s = self.q.stats()
        self.assertEqual(s["metadata"], {"sent": 1, "pending": 1})
        self.assertEqual(s["clip"], {"none": 1, "waiting": 1})
        self.assertEqual(self.q.purge_done(older_than_s=60), [])   # 아직 최근
        self.clock.t += 61
        self.assertEqual(self.q.purge_done(older_than_s=60), [("evt_20261115_143022_001", None)])
        self.assertEqual(self.q.stats()["total"], 1)


class TestRecorder(Base):
    def recorder(self, ring):
        return EventRecorder(ring, self.q, self.tmp / "events", log=lambda *_: None)

    def test_record_fills_clip_and_writes_json(self):
        eid = self.recorder(FakeRing(seconds=3)).record(envelope())
        d = self.q.get(eid)
        self.assertEqual(d["clip_status"], "ready")
        self.assertEqual(d["clip"]["duration_sec"], 3)
        on_disk = json.loads((self.tmp / "events" / eid / f"{eid}.json").read_text(encoding="utf-8"))
        self.assertEqual(on_disk, self.q.server_event(eid))
        self.assertEqual(list(on_disk), list(TOP_KEYS))

    def test_ring_busy_or_failure_sends_without_clip(self):
        for ring, eid in ((FakeRing(busy=True), "evt_20261115_143022_001"),
                          (FakeRing(fail=True), "evt_20261115_143023_001")):
            self.recorder(ring).record(envelope(eid))
            self.assertEqual(self.q.get(eid)["clip_status"], "missing")
            self.assertIsNone(self.q.server_event(eid)["clip"])

    def test_duplicate_not_recorded_twice(self):
        calls = []

        class CountRing(FakeRing):
            def save(self, *a, **k):
                calls.append(1)
                return super().save(*a, **k)

        rec = self.recorder(CountRing())
        rec.record(envelope())
        rec.record(copy.deepcopy(envelope()))
        self.assertEqual(len(calls), 1)


class TestSender(Base):
    def setUp(self):
        super().setUp()
        self.srv = Server()

    def tearDown(self):
        self.srv.close()
        super().tearDown()

    def sender(self, url=None, uploader=None):
        return UploadSender(self.q, server_url=url or self.srv.url, clip_uploader=uploader,
                            log=lambda *_: None)

    def test_sends_assembled_json(self):
        eid = "evt_20261115_143022_001"
        self.q.enqueue(envelope())
        self.q.attach_clip(eid, self.clip_info(eid))
        r = self.sender().flush()
        self.assertEqual(r["metadata_ok"], 1)
        path, body = self.srv.received[0]
        self.assertEqual(path, "/api/events")
        self.assertEqual(body, self.q.server_event(eid))
        self.assertNotIn("target", body)
        self.assertNotIn("trigger", body)
        self.assertEqual(self.q.get(eid)["meta_status"], "sent")

    def test_clip_upload_after_metadata(self):
        eid = "evt_20261115_143022_001"
        self.q.enqueue(envelope())
        self.q.attach_clip(eid, self.clip_info(eid))
        up = []
        r = self.sender(uploader=lambda e, p, ev: up.append((e, p.name))).flush()
        self.assertEqual((r["metadata_ok"], r["clip_ok"]), (1, 1))
        self.assertEqual(up, [(eid, f"{eid}.mp4")])
        self.assertEqual(self.q.get(eid)["clip_status"], "sent")

    def test_clip_waits_when_no_uploader(self):
        eid = "evt_20261115_143022_001"
        self.q.enqueue(envelope())
        self.q.attach_clip(eid, self.clip_info(eid))
        r = self.sender().flush()
        self.assertEqual(r["clip_waiting_uploader"], 1)
        self.assertEqual(self.q.get(eid)["clip_status"], "ready")

    def test_4xx_eventually_failed_not_stuck(self):
        self.srv.status = 400
        self.q.enqueue(envelope("evt_20261115_143022_001"), wait_clip=False)
        for _ in range(MAX_REJECTS):
            self.sender().flush()
            self.clock.t += 1000
        self.assertEqual(self.q.get("evt_20261115_143022_001")["meta_status"], "failed")
        self.srv.status = 200
        self.q.enqueue(envelope("evt_20261115_143023_001"), wait_clip=False)
        self.assertEqual(self.sender().flush()["metadata_ok"], 1)

    def test_5xx_stops_round_keeps_pending(self):
        self.srv.status = 503
        for eid in ("evt_20261115_143022_001", "evt_20261115_143023_001"):
            self.q.enqueue(envelope(eid), wait_clip=False)
        r = self.sender().flush()
        self.assertEqual((r["metadata_ok"], r["metadata_fail"]), (0, 1))   # 첫 건에서 멈춤
        self.assertEqual(len(self.srv.received), 1)
        d = self.q.get("evt_20261115_143022_001")
        self.assertEqual((d["meta_status"], d["meta_rejects"]), ("pending", 0))

    def test_network_down_keeps_pending(self):
        self.q.enqueue(envelope(), wait_clip=False)
        r = self.sender(url="http://127.0.0.1:1").flush()
        self.assertEqual(r["metadata_fail"], 1)
        d = self.q.get("evt_20261115_143022_001")
        self.assertEqual((d["meta_status"], d["meta_rejects"]), ("pending", 0))
        self.assertIn("network", d["meta_error"])

    def test_clip_upload_error_retries(self):
        eid = "evt_20261115_143022_001"
        self.q.enqueue(envelope())
        self.q.attach_clip(eid, self.clip_info(eid))

        def broken(*_):
            raise urllib.error.URLError("down")

        r = self.sender(uploader=broken).flush()
        self.assertEqual((r["metadata_ok"], r["clip_fail"]), (1, 1))
        self.assertEqual(self.q.get(eid)["clip_status"], "ready")


if __name__ == "__main__":
    unittest.main()
