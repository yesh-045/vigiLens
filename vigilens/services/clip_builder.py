import os
import tempfile
from typing import Sequence

import numpy as np

from vigilens.integrations.storage import s3_client


def _require_cv2():
    try:
        import cv2  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "opencv-python-headless is required for scene clip building"
        ) from exc
    return cv2


def sample_frames_from_video(
    video_path: str,
    sample_every_seconds: int,
    fps: int = 15,
) -> list[np.ndarray]:
    """Read frames into memory using OpenCV without dumping image files."""
    cv2 = _require_cv2()
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return []

    sample_every_n_frames = max(1, int(sample_every_seconds * fps))
    frames: list[np.ndarray] = []
    frame_idx = 0

    while True:
        ok, frame = cap.read()
        if not ok:
            break
        if frame_idx % sample_every_n_frames == 0:
            frames.append(frame)
        frame_idx += 1

    cap.release()
    return frames


def build_clip(
    frames: Sequence[np.ndarray],
    fps: int,
    clip_duration_seconds: int,
) -> str | None:
    cv2 = _require_cv2()
    if not frames:
        return None

    max_frames = max(1, fps * clip_duration_seconds)
    selected_frames = list(frames[:max_frames])
    height, width = selected_frames[0].shape[:2]

    fd, output_path = tempfile.mkstemp(prefix="scene_clip_", suffix=".mp4")
    os.close(fd)

    writer = cv2.VideoWriter(
        output_path,
        cv2.VideoWriter_fourcc(*"mp4v"),
        fps,
        (width, height),
    )
    for frame in selected_frames:
        writer.write(frame)
    writer.release()

    return output_path


def upload_clip_to_minio(clip_path: str, stream_id: str) -> str:
    clip_name = os.path.basename(clip_path)
    key = f"{stream_id}/scene_clips/{clip_name}"
    s3_client.upload_file(clip_path, key)
    return s3_client.get_presigned_url(key)
