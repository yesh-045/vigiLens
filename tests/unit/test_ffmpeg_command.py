"""Tests for vigilens.apps.workers.stream.stream — ffmpeg command construction."""

from unittest.mock import patch, MagicMock


from vigilens.apps.workers.stream.stream import start_rtsp_segmenter


class TestStartRtspSegmenter:
    """Verify the ffmpeg command is built correctly without actually spawning it."""

    @patch("vigilens.apps.workers.stream.stream.subprocess.Popen")
    def test_reencode_mode_includes_libx264(self, mock_popen):
        mock_popen.return_value = MagicMock(pid=1234)
        start_rtsp_segmenter(
            rtsp_url="rtsp://host/stream",
            out_dir="/tmp/test_out",
            chunk_seconds=10,
            reencode=True,
            fps=15,
            target_width=640,
            target_height=360,
        )
        cmd = mock_popen.call_args[0][0]
        assert "libx264" in cmd
        assert "-c:v" in cmd

    @patch("vigilens.apps.workers.stream.stream.subprocess.Popen")
    def test_copy_mode_uses_copy_codec(self, mock_popen):
        mock_popen.return_value = MagicMock(pid=1234)
        start_rtsp_segmenter(
            rtsp_url="rtsp://host/stream",
            out_dir="/tmp/test_out",
            chunk_seconds=10,
            reencode=False,
            fps=15,
        )
        cmd = mock_popen.call_args[0][0]
        assert "copy" in cmd
        assert "libx264" not in cmd

    @patch("vigilens.apps.workers.stream.stream.subprocess.Popen")
    def test_rtsp_url_in_command(self, mock_popen):
        mock_popen.return_value = MagicMock(pid=1234)
        url = "rtsp://192.168.1.1:554/live"
        start_rtsp_segmenter(rtsp_url=url, out_dir="/tmp/out", chunk_seconds=5)
        cmd = mock_popen.call_args[0][0]
        assert url in cmd

    @patch("vigilens.apps.workers.stream.stream.subprocess.Popen")
    def test_segment_time_matches_chunk_seconds(self, mock_popen):
        mock_popen.return_value = MagicMock(pid=1234)
        start_rtsp_segmenter(
            rtsp_url="rtsp://host/stream",
            out_dir="/tmp/out",
            chunk_seconds=7,
        )
        cmd = mock_popen.call_args[0][0]
        idx = cmd.index("-segment_time")
        assert cmd[idx + 1] == "7"

    @patch("vigilens.apps.workers.stream.stream.subprocess.Popen")
    def test_output_pattern_contains_out_dir(self, mock_popen, tmp_path):
        mock_popen.return_value = MagicMock(pid=1234)
        out_dir = str(tmp_path / "custom_output")
        start_rtsp_segmenter(
            rtsp_url="rtsp://host/stream",
            out_dir=out_dir,
            chunk_seconds=5,
        )
        cmd = mock_popen.call_args[0][0]
        # The last arg should be the output pattern
        assert out_dir in cmd[-1]
        assert "chunk_" in cmd[-1]

    @patch("vigilens.apps.workers.stream.stream.subprocess.Popen")
    def test_gop_calculation(self, mock_popen):
        """GOP should be fps * chunk_seconds."""
        mock_popen.return_value = MagicMock(pid=1234)
        start_rtsp_segmenter(
            rtsp_url="rtsp://host/stream",
            out_dir="/tmp/out",
            chunk_seconds=4,
            fps=15,
            reencode=True,
        )
        cmd = mock_popen.call_args[0][0]
        gop = 15 * 4  # 60
        # -g should be set to the GOP value
        g_idx = cmd.index("-g")
        assert cmd[g_idx + 1] == str(gop)

    @patch("vigilens.apps.workers.stream.stream.subprocess.Popen")
    def test_video_filter_includes_dimensions(self, mock_popen):
        mock_popen.return_value = MagicMock(pid=1234)
        start_rtsp_segmenter(
            rtsp_url="rtsp://host/stream",
            out_dir="/tmp/out",
            chunk_seconds=5,
            fps=10,
            target_width=1280,
            target_height=720,
            reencode=True,
        )
        cmd = mock_popen.call_args[0][0]
        vf_idx = cmd.index("-vf")
        vf_str = cmd[vf_idx + 1]
        assert "1280" in vf_str
        assert "720" in vf_str
        assert "fps=10" in vf_str

    @patch("vigilens.apps.workers.stream.stream.subprocess.Popen")
    def test_returns_popen_object(self, mock_popen):
        sentinel = MagicMock(pid=42)
        mock_popen.return_value = sentinel
        result = start_rtsp_segmenter(
            rtsp_url="rtsp://host/stream",
            out_dir="/tmp/out",
            chunk_seconds=5,
        )
        assert result is sentinel

    @patch("vigilens.apps.workers.stream.stream.subprocess.Popen")
    def test_faststart_movflag(self, mock_popen):
        mock_popen.return_value = MagicMock(pid=1)
        start_rtsp_segmenter(
            rtsp_url="rtsp://host/stream",
            out_dir="/tmp/out",
            chunk_seconds=5,
        )
        cmd = mock_popen.call_args[0][0]
        assert "movflags=+faststart" in cmd

    @patch("vigilens.apps.workers.stream.stream.subprocess.Popen")
    def test_creates_output_directory(self, mock_popen, tmp_path):
        mock_popen.return_value = MagicMock(pid=1)
        out_dir = str(tmp_path / "nested" / "output")
        start_rtsp_segmenter(
            rtsp_url="rtsp://host/stream",
            out_dir=out_dir,
            chunk_seconds=5,
        )
        import os

        assert os.path.isdir(out_dir)

