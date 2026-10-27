"""영상에서 추적 결과를 뽑아 tracks.jsonl로 저장한다.

한 번 뽑아두면 판단 로직을 몇 번이든 재실험할 수 있다.
    python scripts/extract_tracks.py --source test30.mp4 --out /data/tracks/test30.jsonl
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import cv2

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from edge.schema import (VEHICLE_CLASSES, make_track, make_trackframe,
                         validate_trackframe)

# 거리 추정용 임시 상수 (카메라 캘리브레이션 전)
# TODO: 실제 카메라로 보정할 것
FOCAL_PX = 900.0
VEHICLE_HEIGHT_M = 1.5


def estimate_distance(box) -> float | None:
    """박스 높이로 거리를 추정한다. 보정 전이라 상대 비교용으로만 쓸 것."""
    h = box[3] - box[1]
    if h <= 1:
        return None
    return (VEHICLE_HEIGHT_M * FOCAL_PX) / h


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--model", default="~/models/n640/yolo11n_ncnn_model")
    p.add_argument("--imgsz", type=int, default=640)
    p.add_argument("--tracker", default="bytetrack.yaml")
    p.add_argument("--conf", type=float, default=0.25)
    p.add_argument("--out", default="/data/tracks/out.jsonl")
    p.add_argument("--limit", type=int, default=0)
    args = p.parse_args()

    from ultralytics import YOLO

    model = YOLO(str(Path(args.model).expanduser()))
    cap = cv2.VideoCapture(args.source)
    if not cap.isOpened():
        sys.exit(f"영상을 열 수 없음: {args.source}")

    w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0

    out_path = Path(args.out).expanduser()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    ages: dict[int, int] = {}
    n = 0
    t0 = time.time()
    base_ts = time.time()

    with out_path.open("w") as f:
        while True:
            ok, frame = cap.read()
            if not ok:
                break

            res = model.track(frame, imgsz=args.imgsz, conf=args.conf,
                              tracker=args.tracker, persist=True,
                              classes=list(VEHICLE_CLASSES), verbose=False)[0]

            tracks = []
            seen = set()
            if res.boxes is not None and res.boxes.id is not None:
                for b in res.boxes:
                    tid = int(b.id)
                    seen.add(tid)
                    ages[tid] = ages.get(tid, 0) + 1
                    box = [float(v) for v in b.xyxy[0]]
                    tracks.append(make_track(
                        tid=tid,
                        cls=res.names[int(b.cls)],
                        conf=float(b.conf),
                        box=box,
                        dist_m=estimate_distance(box),
                        age=ages[tid],
                    ))
            for tid in list(ages):
                if tid not in seen:
                    ages.pop(tid)

            tf = make_trackframe(
                ts=base_ts + n / fps,
                frame_id=n, src_w=w, src_h=h, fps=fps, tracks=tracks,
            )

            if n == 0:
                errs = validate_trackframe(tf)
                if errs:
                    sys.exit(f"스키마 오류: {errs}")

            f.write(json.dumps(tf, ensure_ascii=False) + "\n")
            n += 1
            if args.limit and n >= args.limit:
                break
            if n % 100 == 0:
                print(f"  {n}프레임 처리", flush=True)

    dt = time.time() - t0
    cap.release()
    print(f"\n{n}프레임 -> {out_path}")
    print(f"{dt:.1f}초, {n/dt:.2f} FPS, 파일 크기 {out_path.stat().st_size/1e6:.1f}MB")


if __name__ == "__main__":
    main()
