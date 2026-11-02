"""Extract: download one month of NYC TLC high-volume FHV trips (Uber, Lyft) into the raw zone."""

import logging
import tempfile
import urllib.request
from pathlib import Path

from pipeline.config import (
    RAW_ZONES_KEY,
    SOURCE_ZONES_URL,
    Settings,
    raw_trips_key,
    source_trips_url,
)
from pipeline.lake import ensure_bucket, object_exists, s3_client

log = logging.getLogger(__name__)


def _download(url: str, dest: Path) -> None:
    with urllib.request.urlopen(url, timeout=60) as resp, dest.open("wb") as out:
        expected = resp.headers.get("Content-Length")
        while chunk := resp.read(1 << 20):
            out.write(chunk)
    # A connection closed early ends the loop without an error. Fail instead of uploading a
    # truncated file, because extract_month would then skip it as "already in lake" forever.
    if expected is not None and dest.stat().st_size != int(expected):
        raise OSError(f"Truncated download of {url}: {dest.stat().st_size} of {expected} bytes")


def _upload_from_url(client, bucket: str, key: str, url: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / Path(key).name
        log.info("Downloading %s", url)
        _download(url, path)
        log.info("Uploading %.1f MB to s3://%s/%s", path.stat().st_size / 1e6, bucket, key)
        client.upload_file(str(path), bucket, key)


def extract_month(month: str, settings: Settings, force: bool = False) -> dict:
    """Copy the month's trip file and the zone lookup into the lake. Idempotent unless force=True."""
    client = s3_client(settings)
    ensure_bucket(client, settings.lake_bucket)

    trips_key = raw_trips_key(month)
    if force or not object_exists(client, settings.lake_bucket, trips_key):
        _upload_from_url(client, settings.lake_bucket, trips_key, source_trips_url(month))
    else:
        log.info("Already in lake, skipping: %s", trips_key)

    if force or not object_exists(client, settings.lake_bucket, RAW_ZONES_KEY):
        _upload_from_url(client, settings.lake_bucket, RAW_ZONES_KEY, SOURCE_ZONES_URL)

    return {"trips_key": trips_key, "zones_key": RAW_ZONES_KEY}
