from logging import Logger
from typing import List
from datetime import datetime, date, time
import json

from psycopg import Connection
from psycopg.rows import dict_row
from pydantic import BaseModel

from lib.pg_connect import PgConnect
from lib.dict_util import json2str

from lib.settings_repository import EtlSetting, EtlSettingsRepository

class DdsTimestampDestObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date


class DdsTimestampOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamp(self, last_loaded: datetime) -> List[DdsTimestampDestObj]:
        #dt_ts = datetime.strptime(last_loaded,"%Y-%m-%d %H:%M:%S")
        with self._db.client().cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                    ;with order_date as (
                    select
                        id,
                        object_value::json->>'date' as "ts",
                        object_value::json->>'final_status' as "status"
                    from stg.ordersystem_orders
                    where update_ts > %(last_loaded)s
                ),
                order_ts as (
                    select
                        id,
                        date_trunc('seconds', ts::timestamp) as "ts"
                    from order_date
                    where status = 'CLOSED' or status = 'CANCELLED'
                )
                select
                    id,
                    ts,
                    extract(year from ts) as "year",
                    extract(month from ts) as "month",
                    extract(day from ts) as "day",
                    ts::time as "time",
                    ts::date as "date"
                from order_ts;
                """, {
                    "last_loaded": last_loaded 
                }
            )
            objs = cur.fetchall()
        #print(objs)
        return objs


class DdsTimestampsDestRepository:

    def insert_timestamp(self, conn: Connection, dm_timestamps: DdsTimestampDestObj) -> None:
        # Сюда данные попадают уже в формате DdsTimestampDestObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ON CONFLICT(ts) do NOTHING;
                """,
                {
                    "ts": dm_timestamps.ts,
                    "year": dm_timestamps.year,
                    "month": dm_timestamps.month,
                    "day": dm_timestamps.day,
                    "time": dm_timestamps.time,
                    "date": dm_timestamps.date
                },
            )


class DdsTimestampLoader:
    WF_KEY = "dds_dm_timestamps_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    BATCH_LIMIT = 1000  # Загружаем пачками
    SHEMA_TABLE = 'dds.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DdsTimestampOriginRepository(pg_origin)
        self.dds = DdsTimestampsDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log


    def parse_of_data(self, raws: List[DdsTimestampDestObj]) -> List[DdsTimestampDestObj]:
        res = []
        for r in raws:
            t = DdsTimestampDestObj(
                                    id = r['id'],
                                    ts = r['ts'],
                                    year = r['year'],
                                    month = r['month'],
                                    day = r['day'],
                                    time = r['time'],
                                    date = r['date']
                                    )
            res.append(t)
        return res


    def data_load(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY:datetime.strptime("1900-01-01 00:00:00","%Y-%m-%d %H:%M:%S")})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            print(last_loaded)
            load_queue = self.origin.list_timestamp(last_loaded)

            # выбираем функцию для парсинга в зависимости от вида таблицы
            data_to_load = self.parse_of_data(load_queue)
            self.log.info(f"Found {len(data_to_load)} dm_timestamp to load.")

            if not data_to_load:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for row in data_to_load:
                self.dds.insert_timestamp(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.ts for t in data_to_load])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


