"""순환 영상 버퍼.

위험은 이미 일어난 뒤에 감지되므로 '이전 N초'를 항상 들고 있어야 한다.
picamera2의 CircularOutput이 인코딩된 프레임을 메모리에 순환 저장하고,
이벤트가 나면 그 시점 앞뒤를 <event_id>.mp4 로 떨군다.

저장이 끝나면 on_done(clip_info) 를 부른다.
    clip_info = {"path": Path, "duration_sec": int, "resolution": "1280x720"}
실패하면 on_done(None).
"""
from __future__ import annotations

import json
import subprocess
import threading
import time
from pathlib import Path
from typing import Callable, Optional


def probe_clip(path: Path) -> dict:
    """ffprobe 로 실제 길이와 해상도를 잰다.

    길이는 버퍼가 덜 찼을 때(부팅 직후 등) 설정값(15초)보다 짧을 수 있어서
    고정값이 아니라 실측값을 쓴다.
    """
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=width,height:format=duration",
         "-of", "json", str(path)],
        check=True, capture_output=True, text=True,
    ).stdout
    info = json.loads(out)
    st = info["streams"][0]
    dur = float(info["format"]["duration"])
    return {
        "path": Path(path),
        "duration_sec": int(round(dur)),
        "resolution": f"{st['width']}x{st['height']}",
    }


class RingBuffer:
    def __init__(self, picam, fps=30, pre_s=10, post_s=5, bitrate=4_000_000):
        # 카메라 없는 환경(테스트)에서도 import 되도록 여기서 불러온다.
        from picamera2.encoders import H264Encoder
        from picamera2.outputs import CircularOutput

        self.picam = picam
        self.fps = fps
        self.pre_s = pre_s
        self.post_s = post_s
        self.encoder = H264Encoder(bitrate=bitrate)
        self.output = CircularOutput(buffersize=int(fps * pre_s))
        self._busy = threading.Lock()

    def start(self):
        self.picam.start_encoder(self.encoder, self.output, name="main")

    def stop(self):
        try:
            self.picam.stop_encoder()
        except Exception:
            pass

    def save(self, out_dir: Path, on_done: Optional[Callable] = None,
             stem: str = "clip") -> bool:
        """이벤트 전후 구간을 <stem>.mp4 로 저장. 즉시 반환, 백그라운드 처리.

        이미 다른 클립을 저장 중이면 False 를 돌려준다(클립 없이 이벤트만 남음).
        """
        if not self._busy.acquire(blocking=False):
            return False
        threading.Thread(target=self._worker, args=(Path(out_dir), stem, on_done),
                         daemon=True).start()
        return True

    def _worker(self, out_dir: Path, stem: str, on_done):
        info = None
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            raw = out_dir / f"{stem}.h264"
            mp4 = out_dir / f"{stem}.mp4"

            self.output.fileoutput = str(raw)
            self.output.start()
            time.sleep(self.post_s)
            self.output.stop()

            subprocess.run(
                ["ffmpeg", "-y", "-loglevel", "error",
                 "-framerate", str(self.fps), "-i", str(raw),
                 "-c", "copy", str(mp4)],
                check=True,
            )
            raw.unlink(missing_ok=True)
            info = probe_clip(mp4)
        except Exception as e:
            print(f"[ring] 저장 실패: {e}", flush=True)
        finally:
            self._busy.release()
            if on_done:
                on_done(info)
