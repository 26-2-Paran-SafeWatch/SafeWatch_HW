"""엣지 실행기: AI 파이프라인을 돌리고, 나온 이벤트를 HW 가 받아 클립·큐까지 처리한다.

    AI (SafeWatch_AI src.main.run)      검출 → 추적 → 차선 → 판단 → envelope
      └─ event_sink = EdgeEventSink      큐 적재 → 클립 → 서버 JSON 저장   (이 저장소)
    전송은 따로:  python scripts/flush_queue.py --watch 30

사용법 (Pi, yolo-env)
    cd ~/SafeWatch_HW
    python run_pipeline.py --source ~/models/sim_weave/sim_weave_a40.mp4
    python run_pipeline.py --config configs/pi5_ufld_hailo.yaml --source <영상> --data ~/edge_test

    --config 는 AI 저장소 기준 경로. 결과는 <data>/queue.db, <data>/events/<event_id>/
    카메라 입력(--source camera)은 카메라 공유(링버퍼 + AI 프레임) 구현 전이라 아직 안 된다.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from edge.sink import EdgeEventSink          # noqa: E402
from edge.storage.ring import probe_clip     # noqa: E402
from edge.upload.queue import EventQueue     # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", required=True, help="영상 파일 경로 (camera 는 아직 미지원)")
    p.add_argument("--config", default="configs/pi5_ufld_hailo.yaml", help="AI 저장소 기준 config")
    p.add_argument("--ai-dir", default=str(Path.home() / "SafeWatch_AI"))
    p.add_argument("--data", default="/data", help="큐 DB·이벤트 폴더를 둘 곳")
    p.add_argument("--pre", type=float, default=10.0, help="이벤트 앞 클립 길이(초)")
    p.add_argument("--post", type=float, default=5.0, help="이벤트 뒤 클립 길이(초)")
    args = p.parse_args()

    if args.source == "camera":
        sys.exit("카메라 입력은 아직 안 된다: 카메라를 링버퍼와 AI 가 같이 쓰는 부분(README 남은 일) 구현 후 사용")
    source = str(Path(args.source).expanduser().resolve())
    if not Path(source).exists():
        sys.exit(f"영상이 없다: {source}")
    ai_dir = Path(args.ai_dir).expanduser().resolve()
    if not (ai_dir / "src" / "main.py").exists():
        sys.exit(f"AI 저장소가 없다: {ai_dir}")
    data = Path(args.data).expanduser().resolve()
    events_dir = data / "events"

    q = EventQueue(data / "queue.db")
    n = q.recover(events_dir=events_dir, probe=probe_clip)
    if n:
        print(f"[QUEUE] 지난 실행에서 클립 waiting 으로 남은 {n}건 정리")
    sink = EdgeEventSink(q, events_dir, pre_s=args.pre, post_s=args.post,
                         log=lambda m: print(m, flush=True))

    # AI 저장소의 config 안 상대 경로(data/models/...)가 AI 저장소 기준이라 그 폴더에서 돈다
    os.chdir(ai_dir)
    sys.path.insert(0, str(ai_dir))
    from src.main import run   # noqa: E402

    print(f"[RUN  ] AI {ai_dir} | config {args.config} | source {source}", flush=True)
    print(f"[RUN  ] 큐 {data / 'queue.db'} | 이벤트 {events_dir} | 클립 앞 {args.pre:g}초 뒤 {args.post:g}초",
          flush=True)
    try:
        run(config_path=args.config, source=source, event_sink=sink)   # 끝날 때 run 이 sink.close()
    finally:
        sink.close()          # 이미 닫혔으면 아무 일도 안 함. 남은 클립 작업을 기다린다
        s = q.stats()
        print(f"[DONE ] 이번 실행 이벤트 {sink.published}건 | 큐 전체 {s['total']}건 "
              f"메타 {s['metadata']} 클립 {s['clip']}", flush=True)
        print("        자세히: python scripts/flush_queue.py --db "
              f"{data / 'queue.db'} --status   /   전송: ... --events-dir {events_dir} --watch 30", flush=True)
        q.close()


if __name__ == "__main__":
    main()
