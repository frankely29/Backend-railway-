from __future__ import annotations

from core import DB_BACKEND, _db_exec


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
              awarded_at BIGINT NOT NULL,
              is_current BOOLEAN NOT NULL DEFAULT TRUE,
              PRIMARY KEY(user_id, metric, period, period_key),
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )
        _db_exec(
            "CREATE INDEX IF NOT EXISTS idx_leaderboard_badges_lookup ON leaderboard_badges_current(user_id, is_current, period, metric);"
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
          awarded_at INTEGER NOT NULL,
          is_current INTEGER NOT NULL DEFAULT 1,
          PRIMARY KEY(user_id, metric, period, period_key),
          FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
    _db_exec(
        "CREATE INDEX IF NOT EXISTS idx_leaderboard_badges_lookup ON leaderboard_badges_current(user_id, is_current, period, metric);"
    )
