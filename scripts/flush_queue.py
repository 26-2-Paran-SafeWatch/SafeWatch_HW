"""이벤트 큐를 서버로 비운다.

    python scripts/flush_queue.py            # 1회 전송
    python scripts/flush_queue.py --watch 30 # 30초마다 반복
    python scripts/flush_queue.py --status   # 큐 상태만 보기
    python scripts/flush_queue.py --show <event_id>   # 한 건 자세히 (서버로 나갈 JSON 포함)

서버 주소: --server > 환경변수 SAFEWATCH_SERVER > https://safewatch.co.kr
장치 이름: --device > 환경변수 SAFEWATCH_DEVICE_ID > AI 이벤트의 device_id (지금 safewatch-dev-001)
클립 영상도 PUT /api/events/clip 으로 올린다. 메타데이터만 보내려면 --no-clip
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from edge.storage.ring import probe_clip
from edge.upload.queue import EventQueue
from edge.upload.sender import DEVICE_ID, SERVER_URL, UploadSender, make_clip_uploader


def print_status(q: EventQueue):
    s = q.stats()
    print(f"전체 {s['total']}건")
    print("  메타데이터 : " + (", ".join(f"{k} {v}" for k, v in sorted(s["metadata"].items())) or "-"))
    print("  클립       : " + (", ".join(f"{k} {v}" for k, v in sorted(s["clip"].items())) or "-"))
    if q.legacy_table:
        print(f"  (예전 형식 큐를 {q.legacy_table} 테이블로 옮겨 둠)")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="/data/queue.db")
    p.add_argument("--events-dir", default="/data/events")
    p.add_argument("--server", default=SERVER_URL)
    p.add_argument("--device", default=DEVICE_ID, help="서버에 등록된 장치 이름 (예: safewatch-dev-001)")
    p.add_argument("--no-clip", action="store_true", help="클립 영상은 보내지 않음 (ready 로 남김)")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--watch", type=int, default=0, help="초 단위 반복 간격")
    p.add_argument("--status", action="store_true")
    p.add_argument("--show", metavar="EVENT_ID")
    args = p.parse_args()

    q = EventQueue(args.db)
    if args.status:
        print_status(q)
        return
    if args.show:
        d = q.get(args.show)
        if d is None:
            sys.exit(f"없는 이벤트: {args.show}")
        print(json.dumps({k: v for k, v in d.items() if k != "envelope"}, ensure_ascii=False, indent=2))
        print("--- 서버로 나갈 JSON")
        print(json.dumps(q.server_event(args.show), ensure_ascii=False, indent=2))
        return

    n = q.recover(events_dir=Path(args.events_dir), probe=probe_clip)
    if n:
        print(f"클립 waiting 에 멈춰 있던 {n}건 정리 (mp4 있으면 ready, 없으면 missing)")

    uploader = None if args.no_clip else make_clip_uploader(args.server)
    sender = UploadSender(q, server_url=args.server, clip_uploader=uploader, device_id=args.device,
                          log=lambda m: print(m, flush=True))
    print(f"서버 {args.server} | 장치 {args.device or 'AI 이벤트 값'} | 클립 {'안 보냄' if args.no_clip else '보냄'}")
    while True:
        r = sender.flush(limit=args.limit)
        line = f"  메타 성공 {r['metadata_ok']} 실패 {r['metadata_fail']}"
        if r["clip_waiting_uploader"]:
            line += f" | 클립 {r['clip_waiting_uploader']}건 대기 (--no-clip)"
        else:
            line += f" | 클립 성공 {r['clip_ok']} 실패 {r['clip_fail']}"
        print(line, flush=True)
        if not args.watch:
            break
        time.sleep(args.watch)
    print_status(q)


if __name__ == "__main__":
    main()
