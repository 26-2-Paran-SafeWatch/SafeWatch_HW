"""카메라 → 추론 → 추적 → 판단 → 클립 저장 → 큐 적재.

서버 전송만 비어 있는 상태의 전 구간 관통 파이프라인.
    python run_pipeline.py
"""
from __future__ import annotations

import json
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import cv2
from picamera2 import Picamera2
from ultralytics import YOLO

sys.path.insert(0, str(Path(__file__).resolve().parent))
from edge.schema import VEHICLE_CLASSES, make_track, make_trackframe
from edge.judge.rules import SimpleJudge
from edge.storage.ring import RingBuffer
from edge.upload.queue import UploadQueue

MODEL   = str(Path.home() / "models/n320/yolo11n_ncnn_model")
IMGSZ   = 320
FPS     = 30
EVENTS  = Path("/data/events")
CLASSES = list(VEHICLE_CLASSES) + [0]      # 차량 + 사람(실내 시연용)

running = True


def _stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def main():
    model = YOLO(MODEL)
    picam = Picamera2()
    picam.configure(picam.create_video_configuration(
        main={"size": (1280, 720), "format": "RGB888"},
        lores={"size": (640, 384), "format": "YUV420"},
        raw={"size": (2304, 1296)},
        buffer_count=4,
        controls={"FrameRate": FPS, "AfMode": 0, "LensPosition": 0.0},
    ))
    picam.start()
    time.sleep(1)

    ring = RingBuffer(picam, fps=FPS, pre_s=10, post_s=5)
    ring.start()
    judge = SimpleJudge(fps=FPS)
    queue = UploadQueue()

    ages = {}
    n = 0
    t_log = time.time()
    print("파이프라인 시작. Ctrl+C로 종료", flush=True)

    while running:
        req = picam.capture_request()
        lo = req.make_array("lores")
        req.release()
        frame = cv2.cvtColor(lo, cv2.COLOR_YUV2BGR_I420)

        res = model.track(frame, imgsz=IMGSZ, persist=True,
                          tracker="bytetrack.yaml", classes=CLASSES,
                          verbose=False)[0]

        tracks, seen = [], set()
        sx = 1280 / frame.shape[1]
        sy = 720 / frame.shape[0]
        if res.boxes is not None and res.boxes.id is not None:
            for b in res.boxes:
                tid = int(b.id)
                seen.add(tid)
                ages[tid] = ages.get(tid, 0) + 1
                x1, y1, x2, y2 = (float(v) for v in b.xyxy[0])
                tracks.append(make_track(
                    tid=tid, cls=res.names[int(b.cls)], conf=float(b.conf),
                    box=[x1 * sx, y1 * sy, x2 * sx, y2 * sy], age=ages[tid]))
        for tid in [k for k in ages if k not in seen]:
            ages.pop(tid)

        tf = make_trackframe(time.time(), n, 1280, 720, FPS, tracks)
        ev = judge.push(tf)

        if ev:
            stamp = datetime.fromtimestamp(
                ev["created_at"], timezone.utc).strftime("%Y%m%dT%H%M%S")
            out_dir = EVENTS / f"{stamp}_{ev['event_id'][:8]}"
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "event.json").write_text(
                json.dumps(ev, ensure_ascii=False, indent=2))
            queue.enqueue(ev)
            codes = ",".join(r["code"] for r in ev["reasons"])
            print(f"[EVENT] score={ev['score']} {codes} "
                  f"track={ev['target']['track_id']}", flush=True)
            ring.save(out_dir, on_done=lambda p, eid=ev["event_id"]: (
                queue.attach_clip(eid, p),
                print(f"[CLIP ] {p}", flush=True)))

        n += 1
        if time.time() - t_log >= 5:
            print(f"  {n / (time.time() - t_log):5.1f} FPS  "
                  f"tracks={len(tracks)}  queue={queue.count()}", flush=True)
            n, t_log = 0, time.time()

    print("\n종료 중...", flush=True)
    ring.stop()
    picam.stop()
    print("큐 상태:", queue.count())


if __name__ == "__main__":
    main()
