"""업로드 큐 → 서버 전송.

큐에 저장된 JSON 이 곧 서버 스키마라서 변환 없이 그대로 보낸다.
같은 event_id 재전송은 서버가 멱등으로 처리한다는 전제(재시도 안전).

  - 2xx              : done
  - 4xx              : 재시도 횟수 증가, MAX_RETRY 넘으면 failed (큐가 막히지 않게)
  - 5xx / 네트워크   : 이번 회차 중단, 다음 회차에 다시
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

from edge.event import validate_event

SERVER_URL = os.environ.get("SAFEWATCH_SERVER", "http://3.34.130.6:8080")


class UploadSender:
    def __init__(self, queue, server_url=SERVER_URL, timeout=10):
        self.queue = queue
        self.url = server_url.rstrip("/") + "/api/events"
        self.timeout = timeout

    def _post(self, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            self.url, data=body, method="POST",
            headers={"Content-Type": "application/json; charset=utf-8"})
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")

    def flush(self, limit=20, verbose=True):
        """ready 항목을 전송한다. (성공, 실패) 건수를 돌려준다."""
        ok = fail = 0
        for event_id, ev, _clip in self.queue.ready(limit):
            try:
                validate_event(ev)
                status, _ = self._post(ev)
                self.queue.mark_done(event_id)
                ok += 1
                if verbose:
                    print(f"[SEND ] {event_id} -> {status}", flush=True)
            except ValueError as e:
                # 엣지에서 만든 JSON 자체가 잘못됨. 보내봐야 거절된다.
                fail += 1
                state = self.queue.record_error(event_id, f"invalid: {e}")
                if verbose:
                    print(f"[SEND ] {event_id} 형식 오류({state}): {e}", flush=True)
            except urllib.error.HTTPError as e:
                detail = e.read().decode("utf-8", "replace")[:300]
                fail += 1
                if 400 <= e.code < 500:
                    state = self.queue.record_error(event_id, f"HTTP {e.code}: {detail}")
                    if verbose:
                        print(f"[SEND ] {event_id} 거절 HTTP {e.code}({state}): {detail}",
                              flush=True)
                    continue
                if verbose:
                    print(f"[SEND ] 서버 오류 HTTP {e.code}, 다음 회차에 재시도", flush=True)
                break
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                fail += 1
                if verbose:
                    print(f"[SEND ] 네트워크 대기: {e}", flush=True)
                break
        return ok, fail
