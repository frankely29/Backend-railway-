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
              last_nyc_date DATE,
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
              id BIGSERIAL PRIMARY KEY,
              user_id BIGINT NOT NULL,
              metric TEXT NOT NULL,
              period TEXT NOT NULL,
              rank_position INTEGER NOT NULL,
              badge_code TEXT NOT NULL,
              period_key TEXT NOT NULL,
              awarded_at BIGINT NOT NULL,
              is_current BOOLEAN NOT NULL DEFAULT TRUE,
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )
        _db_exec("CREATE INDEX IF NOT EXISTS idx_leaderboard_badges_lookup ON leaderboard_badges_current(user_id, is_current, period, metric);")
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS leaderboard_email_prefs (
              user_id BIGINT PRIMARY KEY,
              weekly_enabled BOOLEAN NOT NULL DEFAULT TRUE,
              monthly_enabled BOOLEAN NOT NULL DEFAULT TRUE,
              yearly_enabled BOOLEAN NOT NULL DEFAULT TRUE,
              created_at BIGINT NOT NULL,
              updated_at BIGINT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS leaderboard_report_log (
              user_id BIGINT NOT NULL,
              report_type TEXT NOT NULL,
              period_key TEXT NOT NULL,
              sent_at BIGINT NOT NULL,
              status TEXT NOT NULL,
              error_message TEXT,
              PRIMARY KEY (user_id, report_type, period_key),
              FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
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
          last_nyc_date TEXT,
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
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL,
          metric TEXT NOT NULL,
          period TEXT NOT NULL,
          rank_position INTEGER NOT NULL,
          badge_code TEXT NOT NULL,
          period_key TEXT NOT NULL,
          awarded_at INTEGER NOT NULL,
          is_current INTEGER NOT NULL DEFAULT 1,
          FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
    _db_exec("CREATE INDEX IF NOT EXISTS idx_leaderboard_badges_lookup ON leaderboard_badges_current(user_id, is_current, period, metric);")
    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS leaderboard_email_prefs (
          user_id INTEGER PRIMARY KEY,
          weekly_enabled INTEGER NOT NULL DEFAULT 1,
          monthly_enabled INTEGER NOT NULL DEFAULT 1,
          yearly_enabled INTEGER NOT NULL DEFAULT 1,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS leaderboard_report_log (
          user_id INTEGER NOT NULL,
          report_type TEXT NOT NULL,
          period_key TEXT NOT NULL,
          sent_at INTEGER NOT NULL,
          status TEXT NOT NULL,
          error_message TEXT,
          PRIMARY KEY (user_id, report_type, period_key),
          FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
