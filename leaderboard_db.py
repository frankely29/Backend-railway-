from __future__ import annotations

from core import DB_BACKEND, _db_exec


def _try_exec(sql: str) -> None:
    try:
        _db_exec(sql)
    except Exception:
        pass


def init_leaderboard_schema() -> None:
    if DB_BACKEND == "postgres":
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS driver_work_state (
              user_id BIGINT PRIMARY KEY,
              last_seen_at BIGINT,
              last_lat DOUBLE PRECISION,
              last_lng DOUBLE PRECISION,
              last_heading DOUBLE PRECISION,
              updated_at BIGINT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS driver_daily_stats (
              user_id BIGINT NOT NULL,
              nyc_date DATE NOT NULL,
              miles_worked DOUBLE PRECISION NOT NULL DEFAULT 0,
              hours_worked DOUBLE PRECISION NOT NULL DEFAULT 0,
              trips_recorded INTEGER NOT NULL DEFAULT 0,
              pickups_recorded INTEGER NOT NULL DEFAULT 0,
              heartbeat_count INTEGER NOT NULL DEFAULT 0,
              updated_at BIGINT NOT NULL,
              PRIMARY KEY (user_id, nyc_date),
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS leaderboard_badges_current (
              user_id BIGINT NOT NULL,
              metric TEXT NOT NULL,
              period TEXT NOT NULL,
              period_key TEXT NOT NULL,
              rank_position INTEGER NOT NULL,
              badge_code TEXT NOT NULL,
              has_crown BOOLEAN NOT NULL DEFAULT FALSE,
              awarded_at BIGINT NOT NULL,
              is_current BOOLEAN NOT NULL DEFAULT TRUE,
              PRIMARY KEY(user_id, metric, period, period_key),
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )
        _try_exec("ALTER TABLE leaderboard_badges_current ADD COLUMN IF NOT EXISTS has_crown BOOLEAN NOT NULL DEFAULT FALSE;")
        _db_exec(
            "CREATE INDEX IF NOT EXISTS idx_leaderboard_badges_lookup ON leaderboard_badges_current(user_id, is_current, period, metric);"
        )
        _db_exec(
            """
            DELETE FROM leaderboard_badges_current t
            USING (
              SELECT ctid
              FROM (
                SELECT ctid,
                       ROW_NUMBER() OVER (
                         PARTITION BY user_id, metric, period, period_key
                         ORDER BY awarded_at DESC, ctid DESC
                       ) AS rn
                FROM leaderboard_badges_current
              ) ranked
              WHERE rn > 1
            ) d
            WHERE t.ctid = d.ctid;
            """
        )
        _db_exec(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_leaderboard_badges_current_identity ON leaderboard_badges_current(user_id, metric, period, period_key);"
        )
        return

    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS driver_work_state (
          user_id INTEGER PRIMARY KEY,
          last_seen_at INTEGER,
          last_lat REAL,
          last_lng REAL,
          last_heading REAL,
          updated_at INTEGER NOT NULL,
          FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS driver_daily_stats (
          user_id INTEGER NOT NULL,
          nyc_date TEXT NOT NULL,
          miles_worked REAL NOT NULL DEFAULT 0,
          hours_worked REAL NOT NULL DEFAULT 0,
          trips_recorded INTEGER NOT NULL DEFAULT 0,
          pickups_recorded INTEGER NOT NULL DEFAULT 0,
          heartbeat_count INTEGER NOT NULL DEFAULT 0,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY (user_id, nyc_date),
          FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS leaderboard_badges_current (
          user_id INTEGER NOT NULL,
          metric TEXT NOT NULL,
          period TEXT NOT NULL,
          period_key TEXT NOT NULL,
          rank_position INTEGER NOT NULL,
          badge_code TEXT NOT NULL,
          has_crown INTEGER NOT NULL DEFAULT 0,
          awarded_at INTEGER NOT NULL,
          is_current INTEGER NOT NULL DEFAULT 1,
          PRIMARY KEY(user_id, metric, period, period_key),
          FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
    _try_exec("ALTER TABLE leaderboard_badges_current ADD COLUMN has_crown INTEGER NOT NULL DEFAULT 0;")
    _db_exec(
        "CREATE INDEX IF NOT EXISTS idx_leaderboard_badges_lookup ON leaderboard_badges_current(user_id, is_current, period, metric);"
    )
