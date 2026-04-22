"""Tests for vigilens.core.config — Settings defaults and overrides."""

from vigilens.core.config import Settings


class TestSettingsDefaults:
    """Verify that default values match the documented contract."""

    def test_redis_url_default(self):
        s = Settings(
            _env_file=None,  # type: ignore[call-arg]
        )
        assert s.redis_url == "redis://localhost:6379/0"

    def test_stream_job_stream_default(self):
        s = Settings(_env_file=None)  # type: ignore[call-arg]
        assert s.stream_job_stream == "stream.jobs"

    def test_llm_job_stream_default(self):
        s = Settings(_env_file=None)  # type: ignore[call-arg]
        assert s.llm_job_stream == "llm.jobs"

    def test_chunk_dimensions_default(self):
        s = Settings(_env_file=None)  # type: ignore[call-arg]
        assert s.stream_chunk_target_width == 640
        assert s.stream_chunk_target_height == 360

    def test_num_active_streams_per_worker_default(self):
        s = Settings(_env_file=None)  # type: ignore[call-arg]
        assert s.num_active_streams_per_worker == 2

    def test_num_active_llm_streams_per_worker_default(self):
        s = Settings(_env_file=None)  # type: ignore[call-arg]
        assert s.num_active_llm_streams_per_worker == 10

    def test_screener_model_default(self):
        s = Settings(_env_file=None)  # type: ignore[call-arg]
        assert "Qwen" in s.screener_model

    def test_llm_model_default(self):
        s = Settings(_env_file=None)  # type: ignore[call-arg]
        assert "gemini" in s.llm_model

    def test_s3_fields_default_to_none(self):
        s = Settings(_env_file=None)  # type: ignore[call-arg]
        assert s.s3_endpoint is None
        assert s.s3_bucket is None
        assert s.aws_access_key_id is None
        assert s.aws_secret_access_key is None


class TestSettingsOverrides:
    """Verify that explicit values override defaults."""

    def test_override_redis_url(self):
        s = Settings(redis_url="redis://custom:1234/1", _env_file=None)  # type: ignore[call-arg]
        assert s.redis_url == "redis://custom:1234/1"

    def test_override_s3_endpoint(self):
        s = Settings(s3_endpoint="http://minio:9000", _env_file=None)  # type: ignore[call-arg]
        assert s.s3_endpoint == "http://minio:9000"

    def test_override_chunk_dimensions(self):
        s = Settings(
            stream_chunk_target_width=1280,
            stream_chunk_target_height=720,
            _env_file=None,  # type: ignore[call-arg]
        )
        assert s.stream_chunk_target_width == 1280
        assert s.stream_chunk_target_height == 720

    def test_override_timeouts(self):
        s = Settings(
            ingestion_timeout=600,
            screener_timeout=60,
            llm_timeout=120,
            _env_file=None,  # type: ignore[call-arg]
        )
        assert s.ingestion_timeout == 600
        assert s.screener_timeout == 60
        assert s.llm_timeout == 120

    def test_extra_env_ignored(self):
        """Settings has extra='ignore', so unknown keys should not crash."""
        s = Settings(
            some_random_var="hello",
            _env_file=None,  # type: ignore[call-arg]
        )
        assert not hasattr(s, "some_random_var")

