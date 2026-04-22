import asyncio
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any

DB_PATH = os.environ.get("DB_PATH", "streams.db")


def _utc_now_sql() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in cursor.fetchall()]
    return column in cols


def get_connection() -> sqlite3.Connection:
    """Returns a new connection to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db() -> None:
    """Initializes SQLite tables and indexes used by Vigilens."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS streams (
                id TEXT PRIMARY KEY,
                url TEXT,
                status TEXT,
                camera_id TEXT,
                created_at DATETIME,
                updated_at DATETIME
            )
            """
        )
        if not _column_exists(conn, "streams", "camera_id"):
            cursor.execute("ALTER TABLE streams ADD COLUMN camera_id TEXT")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                timestamp DATETIME,
                camera_id TEXT,
                event_type TEXT,
                confidence REAL,
                description TEXT,
                clip_url TEXT,
                stream_id TEXT,
                dedupe_key TEXT
            )
            """
        )
        if not _column_exists(conn, "events", "dedupe_key"):
            cursor.execute("ALTER TABLE events ADD COLUMN dedupe_key TEXT")
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_events_time
            ON events (camera_id, timestamp DESC)
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_events_dedupe_key
            ON events (dedupe_key)
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS scene_timeline (
                id TEXT PRIMARY KEY,
                timestamp DATETIME,
                camera_id TEXT,
                summary TEXT,
                clip_url TEXT,
                stream_id TEXT,
                is_compacted INTEGER DEFAULT 0
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_scene_time
            ON scene_timeline (camera_id, timestamp DESC)
            """
        )
        conn.commit()


def create_stream(
    stream_id: str,
    url: str,
    status: str = "created",
    camera_id: str | None = None,
) -> None:
    """Creates a new stream record."""
    now = _utc_now_sql()
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            if _column_exists(conn, "streams", "camera_id"):
                cursor.execute(
                    """
                    INSERT INTO streams (id, url, status, camera_id, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (stream_id, url, status, camera_id, now, now),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO streams (id, url, status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (stream_id, url, status, now, now),
                )
            conn.commit()
    except sqlite3.OperationalError as exc:
        if "no such table: streams" not in str(exc).lower():
            raise
        init_db()
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO streams (id, url, status, camera_id, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (stream_id, url, status, camera_id, now, now),
            )
            conn.commit()


def update_stream_status(stream_id: str, status: str) -> None:
    """Updates the status of an existing stream."""
    now = _utc_now_sql()
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE streams
                SET status = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, now, stream_id),
            )
            conn.commit()
    except sqlite3.OperationalError as exc:
        if "no such table: streams" not in str(exc).lower():
            raise
        init_db()
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE streams
                SET status = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, now, stream_id),
            )
            conn.commit()


def get_stream_status(stream_id: str) -> str | None:
    """Retrieves the status of a specific stream."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM streams WHERE id = ?", (stream_id,))
        row = cursor.fetchone()
        if row:
            return row["status"]
        return None


def get_stream(stream_id: str) -> dict[str, Any] | None:
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, url, status, camera_id, created_at, updated_at FROM streams WHERE id = ?",
                (stream_id,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.OperationalError as exc:
        if "no such table: streams" not in str(exc).lower():
            raise
        init_db()
        return None


def save_event(
    *,
    event_id: str,
    timestamp: str,
    camera_id: str | None,
    event_type: str,
    confidence: float,
    description: str,
    clip_url: str,
    stream_id: str | None = None,
    dedupe_key: str | None = None,
) -> int:
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO events (
                    id, timestamp, camera_id, event_type, confidence, description, clip_url, stream_id, dedupe_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    timestamp,
                    camera_id,
                    event_type,
                    confidence,
                    description,
                    clip_url,
                    stream_id,
                    dedupe_key,
                ),
            )
            inserted = cursor.rowcount
            conn.commit()
            return inserted
    except sqlite3.OperationalError as exc:
        if "no such table: events" not in str(exc).lower():
            raise
        init_db()
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO events (
                    id, timestamp, camera_id, event_type, confidence, description, clip_url, stream_id, dedupe_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    timestamp,
                    camera_id,
                    event_type,
                    confidence,
                    description,
                    clip_url,
                    stream_id,
                    dedupe_key,
                ),
            )
            inserted = cursor.rowcount
            conn.commit()
            return inserted


def query_recent_events(
    *, camera_id: str | None, within_hours: int = 1, limit: int = 5
) -> list[dict[str, Any]]:
    query = """
        SELECT id, timestamp, camera_id, event_type, confidence, description, clip_url, stream_id
        FROM events
        WHERE timestamp > datetime('now', ?)
    """
    params: list[Any] = [f"-{within_hours} hour"]
    if camera_id:
        query += " AND camera_id = ?"
        params.append(camera_id)
    query += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, tuple(params))
        return [dict(row) for row in cursor.fetchall()]


def save_scene_timeline(
    *,
    timeline_id: str,
    timestamp: str,
    camera_id: str | None,
    summary: str,
    clip_url: str,
    stream_id: str | None = None,
    is_compacted: int = 0,
) -> None:
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO scene_timeline (
                    id, timestamp, camera_id, summary, clip_url, stream_id, is_compacted
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    timeline_id,
                    timestamp,
                    camera_id,
                    summary,
                    clip_url,
                    stream_id,
                    is_compacted,
                ),
            )
            conn.commit()
    except sqlite3.OperationalError as exc:
        if "no such table: scene_timeline" not in str(exc).lower():
            raise
        init_db()
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO scene_timeline (
                    id, timestamp, camera_id, summary, clip_url, stream_id, is_compacted
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    timeline_id,
                    timestamp,
                    camera_id,
                    summary,
                    clip_url,
                    stream_id,
                    is_compacted,
                ),
            )
            conn.commit()


def query_recent_scene_timeline(
    *, camera_id: str | None, within_hours: int = 1, limit: int = 10
) -> list[dict[str, Any]]:
    query = """
        SELECT id, timestamp, camera_id, summary, clip_url, stream_id, is_compacted
        FROM scene_timeline
        WHERE timestamp > datetime('now', ?)
    """
    params: list[Any] = [f"-{within_hours} hour"]
    if camera_id:
        query += " AND camera_id = ?"
        params.append(camera_id)
    query += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, tuple(params))
        return [dict(row) for row in cursor.fetchall()]


def compress_old_scene_timeline(retention_hours: int = 24) -> int:
    """Compress old scene_timeline rows into coarse summary rows and delete originals."""
    threshold = f"-{retention_hours} hour"
    compressed_count = 0
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT camera_id, COUNT(*) AS n, MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts
            FROM scene_timeline
            WHERE timestamp < datetime('now', ?)
              AND is_compacted = 0
            GROUP BY camera_id
            """,
            (threshold,),
        )
        groups = cursor.fetchall()

        for row in groups:
            camera_id = row["camera_id"]
            n = row["n"]
            min_ts = row["min_ts"]
            max_ts = row["max_ts"]
            if not n:
                continue

            compacted_id = f"cmp_{camera_id or 'unknown'}_{int(datetime.now(timezone.utc).timestamp())}"
            summary = f"Compacted {n} scene entries from {min_ts} to {max_ts}"
            cursor.execute(
                """
                INSERT OR IGNORE INTO scene_timeline (
                    id, timestamp, camera_id, summary, clip_url, stream_id, is_compacted
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (compacted_id, max_ts, camera_id, summary, "", None, 1),
            )
            cursor.execute(
                """
                DELETE FROM scene_timeline
                WHERE camera_id IS ?
                  AND timestamp < datetime('now', ?)
                  AND is_compacted = 0
                """,
                (camera_id, threshold),
            )
            compressed_count += n

        conn.commit()

    return compressed_count


# --- Asynchronous Wrappers ---


async def init_db_async() -> None:
    await asyncio.to_thread(init_db)


async def create_stream_async(
    stream_id: str,
    url: str,
    status: str = "created",
    camera_id: str | None = None,
) -> None:
    await asyncio.to_thread(create_stream, stream_id, url, status, camera_id)


async def update_stream_status_async(stream_id: str, status: str) -> None:
    await asyncio.to_thread(update_stream_status, stream_id, status)


async def get_stream_status_async(stream_id: str) -> str | None:
    return await asyncio.to_thread(get_stream_status, stream_id)


async def get_stream_async(stream_id: str) -> dict[str, Any] | None:
    return await asyncio.to_thread(get_stream, stream_id)


async def save_event_async(**kwargs: Any) -> int:
    return await asyncio.to_thread(save_event, **kwargs)


async def query_recent_events_async(
    *, camera_id: str | None, within_hours: int = 1, limit: int = 5
) -> list[dict[str, Any]]:
    return await asyncio.to_thread(
        query_recent_events,
        camera_id=camera_id,
        within_hours=within_hours,
        limit=limit,
    )


async def save_scene_timeline_async(**kwargs: Any) -> None:
    await asyncio.to_thread(save_scene_timeline, **kwargs)


async def query_recent_scene_timeline_async(
    *, camera_id: str | None, within_hours: int = 1, limit: int = 10
) -> list[dict[str, Any]]:
    return await asyncio.to_thread(
        query_recent_scene_timeline,
        camera_id=camera_id,
        within_hours=within_hours,
        limit=limit,
    )


async def compress_old_scene_timeline_async(retention_hours: int = 24) -> int:
    return await asyncio.to_thread(compress_old_scene_timeline, retention_hours)
