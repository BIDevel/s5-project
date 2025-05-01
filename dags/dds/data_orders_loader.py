from logging import Logger
from typing import List
from datetime import datetime
import json

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from lib.pg_connect import PgConnect
from lib.dict_util import json2str

from lib.settings_repository import EtlSetting, EtlSettingsRepository


class DmOrderOriginObj(BaseModel):
    id: int
    user_id: int
    restaurant_id: int
    timestamp_id: int
    order_key: str
    order_status: str
    ts: datetime
    

class DmOrderDestObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int
    ts: datetime


class DmBonusEvObj(BaseModel):
    id: int
    event_type: str
    event_ts: datetime
    event_value: str

class DmOrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, last_loaded: datetime ) -> List[DmOrderOriginObj]:
        print(last_loaded)
        with self._db.client().cursor(row_factory=class_row(DmOrderOriginObj)) as cur:
            cur.execute(
                """with orders as (
                        select
                            id,
                            update_ts as "active_from",
                            object_value::json->>'_id' as "order_key",
                            object_value::json->>'final_status' as "order_status",
                            object_value::json->'restaurant'->>'id' as "restaurant_id",
                            object_value::json->'user'->>'id' as "user_id"
                        from stg.ordersystem_orders
                        where update_ts > %(last_loaded)s
                    )
                    select distinct on (o.id)
                        o.id,
                        u.id as "user_id",
                        r.id as "restaurant_id",
                        t.id as "timestamp_id",
                        o.order_key,
                        o.order_status,
                        t.ts
                    from orders o
                        join dds.dm_restaurants r on
                            r.restaurant_id = o.restaurant_id and
                            ((r.active_from <= o.active_from and o.active_from < r.active_to) or
                            r.active_to = '2099-12-31'::timestamp
                            )
                        join dds.dm_timestamps t on t.id = o.id
                        join dds.dm_users u on u.user_id = o.user_id
                    order by o.id, r.restaurant_id asc
                """, {
                    "last_loaded": last_loaded 
                }
            )
            objs = cur.fetchall()
        return objs


class DmOrdersDestRepository:

    def insert_to_db(self, conn: Connection, dm_orders: DmOrderDestObj) -> None:
        # Сюда данные попадают уже в формате DmOrderDestObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, order_status, restaurant_id, timestamp_id, user_id)
                    VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s)
                    on conflict (id) do update 
                    SET
                        user_id = EXCLUDED.user_id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        order_key = EXCLUDED.order_key,
                        order_status = EXCLUDED.order_status;
                """,
                {
                    "order_key": dm_orders.order_key,
                    "order_status": dm_orders.order_status,
                    "restaurant_id": dm_orders.restaurant_id,
                    "timestamp_id": dm_orders.timestamp_id,
                    "user_id": dm_orders.user_id,
                },
            )


class DmOrderLoader:
    WF_KEY = "dds_dm_orders_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    BATCH_LIMIT = 1000  # Загружаем пачками
    SHEMA_TABLE = 'dds.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmOrdersOriginRepository(pg_origin)
        self.dds = DmOrdersDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log

    def parse_of_data(self, raws: List[DmOrderDestObj]) -> List[DmOrderDestObj]:
        res = []
        for r in raws:
            print(r)
            t = DmOrderDestObj(
                                    id = r.id,
                                    order_key=r.order_key,
                                    order_status=r.order_status,
                                    restaurant_id=r.restaurant_id,
                                    timestamp_id=r.timestamp_id,
                                    user_id=r.user_id,
                                    ts = r.ts
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
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: datetime.strptime("1900-01-01 00:00:00","%Y-%m-%d %H:%M:%S")})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            print(last_loaded)
            load_queue = self.origin.list_orders(last_loaded)
            

            # выбираем функцию для парсинга в зависимости от вида таблицы
            data_to_load = self.parse_of_data(load_queue)
            self.log.info(f"Found {len(data_to_load)} dm_orders to load.")

            if not data_to_load:
                self.log.info("Quitting.")
                return


            # Сохраняем объекты в базу dwh.
            for row in data_to_load:
                self.dds.insert_to_db(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            print(load_queue)
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


