"""영상 파일에서 이벤트 구간 잘라내기 (시뮬레이션·녹화 영상 테스트용).

카메라 입력은 링버퍼(ring.py)가 클립을 만든다. 영상 파일을 파이프라인에 넣을 때는 링버퍼가
없으므로, AI 가 넘긴 trigger.video_sec(영상 안 시각) 앞뒤를 원본 파일에서 잘라낸다.

    [video_sec - pre_s, video_sec + post_s]  → <out>.mp4
    영상 시작·끝에 걸리면 있는 만큼만 (길이는 실측값을 쓴다)

-c copy 는 키프레임 단위로만 잘려서 시작 시각이 몇 초씩 틀어질 수 있다. 짧은 클립이라
다시 인코딩한다 (Pi 5 에서 15초 640x360 기준 수 초).
"""
from __future__ import annotations

import subprocess
from pathlib import Path

from edge.storage.ring import probe_clip


def cut_clip(src: Path, video_sec: float, out_path: Path, *, pre_s: float = 10.0,
             post_s: float = 5.0, timeout: float = 120.0) -> dict:
    """src 의 video_sec 앞 pre_s 초 ~ 뒤 post_s 초를 out_path 로 저장하고 probe_clip 결과를 돌려준다.

    실패하면 예외 (호출한 쪽이 클립 없음으로 처리).
    """
    src, out_path = Path(src), Path(out_path)
    if not src.exists():
        raise FileNotFoundError(f"원본 영상이 없다: {src}")
    start = max(0.0, float(video_sec) - pre_s)
    length = float(video_sec) + post_s - start
    if length <= 0:
        raise ValueError(f"자를 길이가 없다: video_sec={video_sec}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(out_path.stem + ".part.mp4")
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error",
         "-ss", f"{start:.3f}", "-i", str(src), "-t", f"{length:.3f}",
         "-map", "0:v:0", "-an",
         "-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-pix_fmt", "yuv420p",
         "-movflags", "+faststart", str(tmp)],
        check=True, timeout=timeout,
    )
    tmp.replace(out_path)        # 반쯤 쓰인 mp4 가 남지 않게
    info = probe_clip(out_path)
    if info["duration_sec"] <= 0:
        raise ValueError(f"잘라낸 클립 길이가 0초: {out_path}")
    return info
