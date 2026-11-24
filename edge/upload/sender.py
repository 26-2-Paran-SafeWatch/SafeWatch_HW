"""이벤트 큐 → 서버 전송.

1) 메타데이터: 큐가 조립한 서버 JSON(event + location + clip)을 POST /api/events.
2) 클립 파일: 메타데이터가 등록된 이벤트만. 서버의 영상 업로드 방식이 아직 정해지지 않아서
   clip_uploader 를 넘겨야 보낸다. 안 넘기면 클립은 ready 로 남아 기다린다.

결과 처리
  - 2xx              : sent
  - 4xx · 형식 오류  : 거절 횟수 증가, MAX_REJECTS 번이면 failed (큐가 막히지 않게)
  - 5xx · 네트워크   : 다음 시도 시각만 늦추고 이번 회차 중단
같은 event_id 재전송은 서버가 멱등으로 처리한다는 전제(재시도 안전).
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable, Optional

SERVER_URL = os.environ.get("SAFEWATCH_SERVER", "https://safewatch.co.kr")

# clip_uploader(event_id, clip_path, server_event) — 실패하면 urllib.error.HTTPError /
# URLError / OSError 를 그대로 던진다 (POST 와 같은 규칙으로 처리).
ClipUploader = Callable[[str, Path, dict], None]


class UploadSender:
    def __init__(self, queue, server_url: str = SERVER_URL, timeout: float = 10,
                 clip_uploader: Optional[ClipUploader] = None, log=print):
        self.queue = queue
        self.url = server_url.rstrip("/") + "/api/events"
        self.timeout = timeout
        self.clip_uploader = clip_uploader
        self.log = log

    def _post(self, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            self.url, data=body, method="POST",
            headers={"Content-Type": "application/json; charset=utf-8"})
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")

    def _handle(self, event_id: str, kind: str, send: Callable[[], object]) -> str:
        """한 건 보내기. 'ok' | 'rejected' | 'stop'(이번 회차 중단)"""
        try:
            send()
        except ValueError as e:                      # 조립·형식 오류: 보내봐야 거절된다
            st = self.queue.mark_failed(event_id, kind, f"invalid: {e}", retryable=False)
            self.log(f"[SEND ] {kind} {event_id} 형식 오류({st}): {e}")
            return "rejected"
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "replace")[:300]
            if 400 <= e.code < 500:
                st = self.queue.mark_failed(event_id, kind, f"HTTP {e.code}: {detail}",
                                            retryable=False)
                self.log(f"[SEND ] {kind} {event_id} 거절 HTTP {e.code}({st}): {detail}")
                return "rejected"
            self.queue.mark_failed(event_id, kind, f"HTTP {e.code}: {detail}", retryable=True)
            self.log(f"[SEND ] 서버 오류 HTTP {e.code}, 나중에 재시도")
            return "stop"
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            self.queue.mark_failed(event_id, kind, f"network: {e}", retryable=True)
            self.log(f"[SEND ] 네트워크 대기: {e}")
            return "stop"
        self.queue.mark_sent(event_id, kind)
        return "ok"

    def flush(self, limit: int = 20) -> dict:
        """보낼 차례인 메타데이터 → 클립 순서로 보낸다. 건수를 돌려준다."""
        r = {"metadata_ok": 0, "metadata_fail": 0, "clip_ok": 0, "clip_fail": 0,
             "clip_waiting_uploader": 0}
        for item in self.queue.pending("metadata", limit):
            eid, payload = item["event_id"], item["payload"]

            def send(payload=payload):
                status, _ = self._post(payload)
                self.log(f"[SEND ] metadata {eid} -> {status}"
                         f"{'' if payload['clip'] else ' (clip 없음)'}")

            res = self._handle(eid, "metadata", send)
            r["metadata_ok" if res == "ok" else "metadata_fail"] += 1
            if res == "stop":
                return r

        clips = self.queue.pending("clip", limit)
        if self.clip_uploader is None:
            r["clip_waiting_uploader"] = len(clips)
            return r
        for item in clips:
            eid, path, payload = item["event_id"], Path(item["clip_path"]), item["payload"]

            def send(eid=eid, path=path, payload=payload):
                if not path.exists():
                    raise ValueError(f"클립 파일이 없다: {path}")
                self.clip_uploader(eid, path, payload)
                self.log(f"[SEND ] clip {eid} -> ok ({path.name})")

            res = self._handle(eid, "clip", send)
            r["clip_ok" if res == "ok" else "clip_fail"] += 1
            if res == "stop":
                break
        return r
