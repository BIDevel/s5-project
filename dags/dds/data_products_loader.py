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


class DmProductOriginObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class DmProductDestObj(BaseModel):
    id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime
    restaurant_id: int


class DmProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, dm_product_threshold: str, limit: int) -> List[DmProductOriginObj]:
        with self._db.client().cursor(row_factory=class_row(DmProductOriginObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем пачку объектов.
                """, {
                    "threshold": dm_product_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmProductsDestRepository:

    def insert_to_db(self, conn: Connection, dm_products: DmProductDestObj) -> None:
        # Сюда данные попадают уже в формате DmProductDestObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(product_id, product_name, product_price, active_from, active_to, restaurant_id)
                    VALUES (%(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s, %(restaurant_id)s)
                    ON CONFLICT(product_id, active_to) do update 
                            SET
                                active_to = EXCLUDED.active_from;
                """,
                {
                    "product_id": dm_products.product_id,
                    "product_name": dm_products.product_name,
                    "product_price": dm_products.product_price,
                    "active_from": dm_products.active_from,
                    "active_to": dm_products.active_to,
                    "restaurant_id": dm_products.restaurant_id,
                },
            )


class DmProductLoader:
    WF_KEY = "dds_dm_products_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    BATCH_LIMIT = 1000  # Загружаем пачками
    SHEMA_TABLE = 'dds.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmProductsOriginRepository(pg_origin)
        self.dds = DmProductsDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log

    def get_restaurant_id(self, restaurant_id) -> int:
        with self.pg_dest.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_restaurants as r
                    WHERE r.restaurant_id = %(restaurant_id)s
                """,
                {
                    "restaurant_id": restaurant_id,
                },
            )
            objs = cur.fetchone()[0]
        return objs

    def parse_of_data(self, raws: List[DmProductDestObj]) -> List[DmProductDestObj]:
        res = []
        for r in raws:
            json_of_data = json.loads(r.object_value)["menu"]
            for p in json_of_data:
                t = DmProductDestObj(
                                        id = r.id,
                                        product_id=p['_id'],
                                        product_name=p['name'],
                                        product_price=p['price'],
                                        active_from=datetime.strftime(r.update_ts, "%Y-%m-%d %H:%M:%S"),
                                        active_to=datetime(year=2099, month=12, day=31),
                                        restaurant_id=self.get_restaurant_id(r.object_id),
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
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: 0})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_products(last_loaded, self.BATCH_LIMIT)

            # выбираем функцию для парсинга в зависимости от вида таблицы
            data_to_load = self.parse_of_data(load_queue)
            self.log.info(f"Found {len(data_to_load)} dm_products to load.")

            if not data_to_load:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for row in data_to_load:
                self.dds.insert_to_db(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] += len(data_to_load)
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


