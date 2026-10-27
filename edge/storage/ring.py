"""순환 영상 버퍼.

위험은 이미 일어난 뒤에 감지되므로 '이전 N초'를 항상 들고 있어야 한다.
picamera2의 CircularOutput이 인코딩된 프레임을 메모리에 순환 저장하고,
이벤트가 나면 그 시점 앞뒤를 파일로 떨군다.
"""
from __future__ import annotations

import subprocess
import threading
import time
from pathlib import Path

from picamera2.encoders import H264Encoder
from picamera2.outputs import CircularOutput


class RingBuffer:
    def __init__(self, picam, fps=30, pre_s=10, post_s=5, bitrate=4_000_000):
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

    def save(self, out_dir: Path, on_done=None):
        """이벤트 전후 구간을 mp4로 저장. 즉시 반환하고 백그라운드에서 처리."""
        if not self._busy.acquire(blocking=False):
            return False
        threading.Thread(target=self._worker, args=(Path(out_dir), on_done),
                         daemon=True).start()
        return True

    def _worker(self, out_dir: Path, on_done):
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            raw = out_dir / "clip.h264"
            mp4 = out_dir / "clip.mp4"

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
            if on_done:
                on_done(mp4)
        except Exception as e:
            print(f"[ring] 저장 실패: {e}", flush=True)
        finally:
            self._busy.release()
