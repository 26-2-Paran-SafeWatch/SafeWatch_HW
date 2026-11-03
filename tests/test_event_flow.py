"""이벤트 스키마 · 큐 · 전송 흐름 테스트 (카메라 없이 실행 가능).

    python -m unittest tests.test_event_flow -v
"""
from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import threading
import unittest
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from edge.event import KST, build_event, level_of, new_event_id, validate_event
from edge.recorder import EventRecorder
from edge.storage.ring import probe_clip
from edge.upload.queue import MAX_RETRY, UploadQueue
from edge.upload.sender import UploadSender

# 사용자 제공 서버 스키마 예시
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

JUDGMENT = {"score": 78, "types": ["lane_departure", "weaving"],
            "indicators": {"observation_window_sec": 10.0,
                           "lane_departure_duration_sec": 2.8,
                           "direction_changes_count": 4}}


def shape(d):
    """값은 버리고 키 구조만."""
    if isinstance(d, dict):
        return {k: shape(v) for k, v in d.items()}
    return None


def make_mp4(path: Path, seconds=3, size="1280x720"):
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "lavfi",
                    "-i", f"testsrc=size={size}:rate=30:duration={seconds}",
                    "-c:v", "libx264", "-pix_fmt", "yuv420p", str(path)], check=True)


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
        self.q = UploadQueue(self.tmp / "queue.db")

    def tearDown(self):
        self.q.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def recorder(self, ring):
        return EventRecorder(ring, self.q, self.tmp / "events",
                             device_id="safewatch-dev-001",
                             model_version="yolov8n", rule_version="risk-v1",
                             log=lambda *_: None)


class TestSchema(Base):
    def test_same_shape_as_server_example(self):
        ev = build_event(JUDGMENT, device_id="safewatch-dev-001",
                         model_version="yolov8n", rule_version="risk-v1")
        self.assertEqual(shape(ev), shape(SERVER_EXAMPLE))
        validate_event(ev)

    def test_id_and_timestamp_format(self):
        dt = datetime(2026, 11, 3, 14, 30, 22, 451000, tzinfo=KST)
        ev = build_event(JUDGMENT, device_id="d", model_version="m",
                         rule_version="r", detected_at=dt)
        self.assertEqual(ev["event_id"], "evt_20261103_143022_001")
        self.assertEqual(ev["timestamp"], "2026-11-03T14:30:22.451+09:00")
        self.assertEqual(ev["clip"]["filename"], "evt_20261103_143022_001.mp4")

    def test_same_second_bumps_sequence(self):
        dt = datetime(2026, 11, 3, 14, 30, 22, tzinfo=KST)
        taken = {"evt_20261103_143022_001", "evt_20261103_143022_002"}
        self.assertEqual(new_event_id(dt, taken.__contains__), "evt_20261103_143022_003")

    def test_level(self):
        self.assertEqual(level_of(78), "high")
        self.assertEqual(level_of(55), "medium")
        self.assertEqual(level_of(10), "low")

    def test_bad_judgment_rejected(self):
        for bad in ({"score": 120, "types": ["x"]}, {"score": 50, "types": []},
                    {"score": 50}, {"types": ["x"]}):
            with self.assertRaises(ValueError):
                build_event(bad, device_id="d", model_version="m", rule_version="r")

    def test_blurred_false_and_location_null(self):
        ev = build_event(JUDGMENT, device_id="d", model_version="m", rule_version="r")
        self.assertIs(ev["clip"]["blurred"], False)
        self.assertEqual(set(ev["location"].values()), {None})

    def test_numpy_values_serializable(self):
        try:
            import numpy as np
        except ImportError:
            self.skipTest("numpy 없음")
        j = dict(JUDGMENT, indicators={"a": np.float32(2.5), "b": np.int64(4),
                                       "c": float("nan")})
        ev = build_event(j, device_id="d", model_version="m", rule_version="r")
        self.assertEqual(json.loads(json.dumps(ev))["indicators"], {"a": 2.5, "b": 4, "c": None})


class TestRecorderQueue(Base):
    def test_record_fills_clip_and_becomes_ready(self):
        ev = self.recorder(FakeRing(seconds=3)).record(JUDGMENT)
        eid = ev["event_id"]
        self.assertEqual(self.q.count(), {"ready": 1})
        stored = self.q.get(eid)
        self.assertEqual(stored["clip"]["duration_sec"], 3)
        self.assertEqual(stored["clip"]["resolution"], "1280x720")
        on_disk = json.loads((self.tmp / "events" / eid / f"{eid}.json").read_text())
        self.assertEqual(on_disk, stored)                     # 디스크 = 큐
        self.assertTrue((self.tmp / "events" / eid / f"{eid}.mp4").exists())

    def test_ring_busy_sends_without_clip(self):
        ev = self.recorder(FakeRing(busy=True)).record(JUDGMENT)
        self.assertIsNone(self.q.get(ev["event_id"])["clip"])
        self.assertEqual(self.q.count(), {"ready": 1})

    def test_ring_failure_sends_without_clip(self):
        ev = self.recorder(FakeRing(fail=True)).record(JUDGMENT)
        self.assertIsNone(self.q.get(ev["event_id"])["clip"])

    def test_recover_after_power_loss(self):
        ev = build_event(JUDGMENT, device_id="d", model_version="m", rule_version="r")
        eid = ev["event_id"]
        self.q.enqueue(ev, wait_clip=True)
        d = self.tmp / "events" / eid
        d.mkdir(parents=True)
        make_mp4(d / f"{eid}.mp4", 2)
        self.assertEqual(self.q.recover(max_wait_s=0, clip_dir=self.tmp / "events",
                                        probe=probe_clip), 1)
        self.assertEqual(self.q.get(eid)["clip"]["duration_sec"], 2)
        self.assertEqual(self.q.count(), {"ready": 1})

    def test_wait_clip_not_sent(self):
        ev = build_event(JUDGMENT, device_id="d", model_version="m", rule_version="r")
        self.q.enqueue(ev, wait_clip=True)
        self.assertEqual(self.q.ready(), [])


class TestSender(Base):
    def test_sends_stored_json_as_is(self):
        srv = Server(200)
        try:
            ev = self.recorder(FakeRing(seconds=2)).record(JUDGMENT)
            ok, fail = UploadSender(self.q, srv.url).flush(verbose=False)
            self.assertEqual((ok, fail), (1, 0))
            path, body = srv.received[0]
            self.assertEqual(path, "/api/events")
            self.assertEqual(body, self.q.get(ev["event_id"]))
            self.assertEqual(shape(body), shape(SERVER_EXAMPLE))
            self.assertEqual(self.q.count(), {"done": 1})
        finally:
            srv.close()

    def test_4xx_eventually_failed_not_stuck(self):
        srv = Server(400)
        try:
            self.recorder(FakeRing(seconds=1)).record(JUDGMENT)
            s = UploadSender(self.q, srv.url)
            for _ in range(MAX_RETRY):
                s.flush(verbose=False)
            self.assertEqual(self.q.count(), {"failed": 1})
        finally:
            srv.close()

    def test_network_down_keeps_ready(self):
        self.recorder(FakeRing(seconds=1)).record(JUDGMENT)
        ok, fail = UploadSender(self.q, "http://127.0.0.1:9", timeout=1).flush(verbose=False)
        self.assertEqual(ok, 0)
        self.assertEqual(self.q.count(), {"ready": 1})


if __name__ == "__main__":
    unittest.main()
