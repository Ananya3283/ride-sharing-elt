import io

import pytest

from pipeline import extract


class FakeResponse(io.BytesIO):
    def __init__(self, body: bytes, content_length: int):
        super().__init__(body)
        self.headers = {"Content-Length": str(content_length)}


def test_download_writes_complete_file(tmp_path, monkeypatch):
    monkeypatch.setattr(extract.urllib.request, "urlopen", lambda url, timeout: FakeResponse(b"abc", 3))
    dest = tmp_path / "trips.parquet"
    extract._download("https://example.test/trips.parquet", dest)
    assert dest.read_bytes() == b"abc"


def test_download_rejects_truncated_file(tmp_path, monkeypatch):
    # Server promised 10 bytes but closed the connection after 3.
    monkeypatch.setattr(extract.urllib.request, "urlopen", lambda url, timeout: FakeResponse(b"abc", 10))
    with pytest.raises(OSError, match="Truncated"):
        extract._download("https://example.test/trips.parquet", tmp_path / "trips.parquet")
