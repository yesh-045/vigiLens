"""Integration tests for vigilens.integrations.storage.S3Client with mocked boto3."""

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture()
def s3_client():
    """Create an S3Client with mocked boto3 and test settings."""
    mock_boto_client = MagicMock()
    mock_boto_client.upload_file = MagicMock()
    mock_boto_client.generate_presigned_url = MagicMock(
        return_value="http://minio:9000/test-bucket/key?sig=abc"
    )

    with (
        patch("vigilens.integrations.storage.settings") as mock_settings,
        patch("boto3.client", return_value=mock_boto_client),
    ):
        mock_settings.s3_endpoint = "http://localhost:9000"
        mock_settings.s3_bucket = "test-bucket"
        mock_settings.aws_access_key_id = "testkey"
        mock_settings.aws_secret_access_key = "testsecret"
        mock_settings.chunk_presigned_url_ttl = 600

        from vigilens.integrations.storage import S3Client

        client = S3Client()
        client._mock_boto = mock_boto_client  # expose for assertions
        yield client


class TestUploadFile:
    def test_upload_file_calls_boto(self, s3_client, make_stub_mp4):
        path = make_stub_mp4("test.mp4")
        result = s3_client.upload_file(path, "stream/chunks/test.mp4")
        assert result == "s3://test-bucket/stream/chunks/test.mp4"
        s3_client._mock_boto.upload_file.assert_called_once()

    def test_upload_file_default_extra_args(self, s3_client, make_stub_mp4):
        path = make_stub_mp4("test.mp4")
        s3_client.upload_file(path, "key.mp4")
        call_args = s3_client._mock_boto.upload_file.call_args
        extra = call_args[1].get("ExtraArgs") or call_args[0][3]
        assert extra["ContentType"] == "video/mp4"

    def test_upload_file_custom_extra_args(self, s3_client, make_stub_mp4):
        path = make_stub_mp4("test.mp4")
        custom = {"ContentType": "application/octet-stream"}
        s3_client.upload_file(path, "key.mp4", extra_args=custom)
        call_args = s3_client._mock_boto.upload_file.call_args
        extra = call_args[1].get("ExtraArgs") or call_args[0][3]
        assert extra["ContentType"] == "application/octet-stream"


class TestUploadChunkToS3:
    def test_upload_without_tenant(self, s3_client, make_stub_mp4):
        path = make_stub_mp4("chunk_001.mp4")
        result = s3_client.upload_chunks_to_s3([path], "s_abc123")[0]
        assert "url" in result
        assert "presigned_url" in result
        assert "s_abc123/chunks/chunk_001.mp4" in result["url"]

    def test_upload_with_tenant(self, s3_client, make_stub_mp4):
        path = make_stub_mp4("chunk_002.mp4")
        result = s3_client.upload_chunks_to_s3([path], "s_abc123", tenant_id="t_xyz")[0]
        assert "t_xyz/s_abc123/chunks/chunk_002.mp4" in result["url"]

    def test_presigned_url_generated(self, s3_client, make_stub_mp4):
        path = make_stub_mp4("chunk_003.mp4")
        result = s3_client.upload_chunks_to_s3(
            [path], "s_abc123", generate_presigned_url=True
        )[0]
        assert result["presigned_url"] is not None
        s3_client._mock_boto.generate_presigned_url.assert_called_once()

    def test_no_presigned_url(self, s3_client, make_stub_mp4):
        path = make_stub_mp4("chunk_004.mp4")
        result = s3_client.upload_chunks_to_s3(
            [path], "s_abc123", generate_presigned_url=False
        )[0]
        assert "presigned_url" not in result

    def test_missing_bucket_raises(self, make_stub_mp4):
        mock_boto = MagicMock()
        with (
            patch("vigilens.integrations.storage.settings") as ms,
            patch("boto3.client", return_value=mock_boto),
        ):
            ms.s3_endpoint = "http://localhost:9000"
            ms.s3_bucket = None
            ms.aws_access_key_id = "k"
            ms.aws_secret_access_key = "s"
            from vigilens.integrations.storage import S3Client

            client = S3Client()
            path = make_stub_mp4("chunk.mp4")
            with pytest.raises(ValueError, match="S3_BUCKET"):
                client.upload_chunks_to_s3([path], "s_xxx")


class TestGetPresignedUrl:
    def test_returns_url(self, s3_client):
        url = s3_client.get_presigned_url("some/key.mp4", expires_in=300)
        assert url is not None
        s3_client._mock_boto.generate_presigned_url.assert_called_once_with(
            "get_object",
            Params={"Bucket": "test-bucket", "Key": "some/key.mp4"},
            ExpiresIn=300,
        )

