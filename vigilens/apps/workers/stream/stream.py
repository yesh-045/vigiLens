import os
import time
import subprocess
from vigilens.core.config import settings
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def start_rtsp_segmenter(
    rtsp_url: str,
    out_dir: str = "/workspace/vigilens/notebooks/rtsp_chunks",
    chunk_seconds: int = 4,
    reencode: bool = True,
    fps: int = 15,
    target_width: int = 640,
    target_height: int = 360,
):
    """Continuously segment an RTSP stream into MP4 chunks using ffmpeg."""
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    # Timestamped filenames; ffmpeg will create one file per segment.
    out_pattern = str(Path(out_dir) / settings.chunk_output_pattern)

    # NOTE:
    # - If your RTSP stream already has frequent keyframes, you can set reencode=False
    #   (faster, lower CPU). If chunks look corrupted / not playable, use reencode=True.
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-rtsp_transport",
        "tcp",
        "-fflags",
        "nobuffer",
        "-flags",
        "low_delay",
        "-i",
        rtsp_url,
        "-an",
    ]

    if reencode:
        gop = max(1, fps * chunk_seconds)
        vf = (
            f"fps={fps},"
            f"scale={target_width}:{target_height}:force_original_aspect_ratio=decrease,"
            f"pad={target_width}:{target_height}:(ow-iw)/2:(oh-ih)/2,"
            "setsar=1"
        )
        cmd += [
            "-vf",
            vf,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-tune",
            "zerolatency",
            # Force clean segment boundaries: keyframe exactly at each chunk boundary
            "-force_key_frames",
            f"expr:gte(t,n_forced*{chunk_seconds})",
            "-g",
            str(gop),
            "-keyint_min",
            str(gop),
            "-sc_threshold",
            "0",
            "-pix_fmt",
            "yuv420p",
        ]
    else:
        cmd += ["-c:v", "copy"]

    cmd += [
        "-f",
        "segment",
        "-segment_time",
        str(chunk_seconds),
        "-reset_timestamps",
        "1",
        "-strftime",
        "1",
        "-segment_format",
        "mp4",
        # Put MP4 metadata (moov atom) up-front per segment, for better reader compatibility.
        "-segment_format_options",
        "movflags=+faststart",
        "-y",
        out_pattern,
    ]

    return subprocess.Popen(cmd)


def wait_until_video_ready(
    path: str, sleep_s: float = 0.25, timeout_s: float = 60.0
) -> None:
    """Wait until the segment is readable (avoids 'moov atom not found' races).

    We validate by running ffprobe successfully against the file.
    """
    deadline = time.time() + timeout_s
    last_size = -1

    while time.time() < deadline:
        try:
            size = os.path.getsize(path)
        except FileNotFoundError:
            time.sleep(sleep_s)
            continue

        # quick guard: wait for size to stop growing
        if size != last_size:
            last_size = size
            time.sleep(sleep_s)
            continue
        if size <= 0:
            time.sleep(sleep_s)
            continue

        # structural validation: ffprobe must succeed
        p = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=avg_frame_rate,r_frame_rate",
                "-show_entries",
                "format=duration",
                "-of",
                "default=nw=1",
                path,
            ],
            capture_output=True,
            text=True,
        )
        if p.returncode == 0 and "duration=" in (p.stdout or ""):
            return

        time.sleep(sleep_s)

    raise TimeoutError(f"Video never became readable: {path}")
