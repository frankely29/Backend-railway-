from __future__ import annotations

from core import DB_BACKEND, _db_exec


def ensure_work_battles_schema() -> None:
    if DB_BACKEND == "postgres":
        _db_exec(
            """
            CREATE TABLE IF NOT EXISTS work_battle_challenges (
              id BIGSERIAL PRIMARY KEY,
              metric_key TEXT NOT NULL,
              period_key TEXT NOT NULL,
              challenger_user_id BIGINT NOT NULL,
              challenged_user_id BIGINT NOT NULL,
              status TEXT NOT NULL,
              created_at_ms BIGINT NOT NULL,
              expires_at_ms BIGINT NOT NULL,
              accepted_at_ms BIGINT,
              last_action_at_ms BIGINT,
              period_start_date DATE,
              period_end_date DATE,
              ends_at_ms BIGINT,
              challenger_start_value DOUBLE PRECISION,
              challenged_start_value DOUBLE PRECISION,
              winner_user_id BIGINT,
              loser_user_id BIGINT,
              challenger_final_value DOUBLE PRECISION,
              challenged_final_value DOUBLE PRECISION,
              completed_at_ms BIGINT,
              result_code TEXT,
              canceled_by_user_id BIGINT,
              declined_by_user_id BIGINT,
              FOREIGN KEY(challenger_user_id) REFERENCES users(id),
              FOREIGN KEY(challenged_user_id) REFERENCES users(id),
              FOREIGN KEY(winner_user_id) REFERENCES users(id),
              FOREIGN KEY(loser_user_id) REFERENCES users(id),
              FOREIGN KEY(canceled_by_user_id) REFERENCES users(id),
              FOREIGN KEY(declined_by_user_id) REFERENCES users(id)
            );
            """
        )
        _db_exec(
            "CREATE INDEX IF NOT EXISTS idx_work_battles_challenged_status ON work_battle_challenges(challenged_user_id, status);"
        )
        _db_exec(
            "CREATE INDEX IF NOT EXISTS idx_work_battles_challenger_status ON work_battle_challenges(challenger_user_id, status);"
        )
        _db_exec(
            "CREATE INDEX IF NOT EXISTS idx_work_battles_status_expires ON work_battle_challenges(status, expires_at_ms);"
        )
        _db_exec(
            "CREATE INDEX IF NOT EXISTS idx_work_battles_status_ends ON work_battle_challenges(status, ends_at_ms);"
        )
        _db_exec(
            "CREATE INDEX IF NOT EXISTS idx_work_battles_created_desc ON work_battle_challenges(created_at_ms DESC, id DESC);"
        )
        return

    _db_exec(
        """
        CREATE TABLE IF NOT EXISTS work_battle_challenges (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          metric_key TEXT NOT NULL,
          period_key TEXT NOT NULL,
          challenger_user_id INTEGER NOT NULL,
          challenged_user_id INTEGER NOT NULL,
          status TEXT NOT NULL,
          created_at_ms INTEGER NOT NULL,
          expires_at_ms INTEGER NOT NULL,
          accepted_at_ms INTEGER,
          last_action_at_ms INTEGER,
          period_start_date TEXT,
          period_end_date TEXT,
          ends_at_ms INTEGER,
          challenger_start_value REAL,
          challenged_start_value REAL,
          winner_user_id INTEGER,
          loser_user_id INTEGER,
          challenger_final_value REAL,
          challenged_final_value REAL,
          completed_at_ms INTEGER,
          result_code TEXT,
          canceled_by_user_id INTEGER,
          declined_by_user_id INTEGER,
          FOREIGN KEY(challenger_user_id) REFERENCES users(id),
          FOREIGN KEY(challenged_user_id) REFERENCES users(id),
          FOREIGN KEY(winner_user_id) REFERENCES users(id),
          FOREIGN KEY(loser_user_id) REFERENCES users(id),
          FOREIGN KEY(canceled_by_user_id) REFERENCES users(id),
          FOREIGN KEY(declined_by_user_id) REFERENCES users(id)
        );
        """
    )
    _db_exec(
        "CREATE INDEX IF NOT EXISTS idx_work_battles_challenged_status ON work_battle_challenges(challenged_user_id, status);"
    )
    _db_exec(
        "CREATE INDEX IF NOT EXISTS idx_work_battles_challenger_status ON work_battle_challenges(challenger_user_id, status);"
    )
    _db_exec(
        "CREATE INDEX IF NOT EXISTS idx_work_battles_status_expires ON work_battle_challenges(status, expires_at_ms);"
    )
    _db_exec(
        "CREATE INDEX IF NOT EXISTS idx_work_battles_status_ends ON work_battle_challenges(status, ends_at_ms);"
    )
    _db_exec(
        "CREATE INDEX IF NOT EXISTS idx_work_battles_created_desc ON work_battle_challenges(created_at_ms DESC, id DESC);"
    )
