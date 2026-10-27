"""업로드 큐를 서버로 비운다.

    python scripts/flush_queue.py            # 1회 전송
    python scripts/flush_queue.py --watch 30 # 30초마다 반복
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from edge.upload.queue import UploadQueue
from edge.upload.sender import SERVER_URL, DEVICE_ID, UploadSender


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="/data/queue.db")
    p.add_argument("--server", default=SERVER_URL)
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--watch", type=int, default=0, help="초 단위 반복 간격")
    args = p.parse_args()

    q = UploadQueue(args.db)
    sender = UploadSender(q, server_url=args.server)
    print(f"서버 {args.server}  장치 {DEVICE_ID}")

    while True:
        ok, fail = sender.flush(limit=args.limit)
        print(f"성공 {ok}  실패 {fail}  큐 {q.count()}", flush=True)
        if not args.watch:
            break
        time.sleep(args.watch)


if __name__ == "__main__":
    main()
