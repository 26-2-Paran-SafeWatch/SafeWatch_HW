"""업로드 큐를 서버로 비운다.

    python scripts/flush_queue.py            # 1회 전송
    python scripts/flush_queue.py --watch 30 # 30초마다 반복
    python scripts/flush_queue.py --status   # 큐 상태만 보기
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from edge.storage.ring import probe_clip
from edge.upload.queue import UploadQueue
from edge.upload.sender import SERVER_URL, UploadSender


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="/data/queue.db")
    p.add_argument("--events-dir", default="/data/events")
    p.add_argument("--server", default=SERVER_URL)
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--watch", type=int, default=0, help="초 단위 반복 간격")
    p.add_argument("--status", action="store_true")
    args = p.parse_args()

    q = UploadQueue(args.db)
    if args.status:
        print(q.count())
        return

    n = q.recover(clip_dir=Path(args.events_dir), probe=probe_clip)
    if n:
        print(f"wait_clip 에 멈춰 있던 {n}건을 ready 로 복구")

    sender = UploadSender(q, server_url=args.server)
    print(f"서버 {args.server}")
    while True:
        ok, fail = sender.flush(limit=args.limit)
        print(f"  성공 {ok}  실패 {fail}  큐 {q.count()}", flush=True)
        if not args.watch:
            break
        time.sleep(args.watch)


if __name__ == "__main__":
    main()
