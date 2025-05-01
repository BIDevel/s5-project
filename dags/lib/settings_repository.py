from typing import Dict, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class EtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict


class EtlSettingsRepository:
    def __init__(self, shema_table: str) -> None:
        self.shema_table = shema_table
        

    def get_setting(self, 
                    conn: Connection, 
                    etl_key: str) -> Optional[EtlSetting]:
        with conn.cursor(row_factory=class_row(EtlSetting)) as cur:
            cur.execute(
                f"""
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM {self.shema_table}
                    WHERE workflow_key = %(etl_key)s;
                """,
                {   
                    "etl_key": etl_key
                },
            )
            obj = cur.fetchone()

        return obj

    def save_setting(self, conn: Connection, 
                     workflow_key: str, 
                     workflow_settings: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                    INSERT INTO {self.shema_table} (workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "etl_key": workflow_key,
                    "etl_setting": workflow_settings
                },
            )
