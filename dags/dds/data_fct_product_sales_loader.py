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

class DmFctOriginObj(BaseModel):
    id: int
    event_type: str
    event_ts: datetime
    event_value: str


class DmFctDestObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class DmFctsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_fcts(self, fct_threshold: str, limit: int) -> List[DmFctOriginObj]:
        with self._db.client().cursor(row_factory=class_row(DmFctOriginObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_type, event_ts, event_value
                    FROM stg.bonussystem_events
                    WHERE id > %(threshold)s
                        AND event_type = 'bonus_transaction'
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем пачку объектов.
                """, {
                    "threshold": fct_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmFctsDestRepository:

    def insert_to_db(self, conn: Connection, dm_fcts: DmFctDestObj) -> None:
        # Сюда данные попадают уже в формате DmFctDestObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ON CONFLICT (id) DO NOTHING;
                """,
                {
                    "product_id": dm_fcts.product_id,
                    "order_id": dm_fcts.order_id,
                    "count": dm_fcts.count,
                    "price": dm_fcts.price,
                    "total_sum": dm_fcts.total_sum,
                    "bonus_payment": dm_fcts.bonus_payment,
                    "bonus_grant": dm_fcts.bonus_grant,
                },
            )


class DmFctLoader:
    WF_KEY = "dds_fct_product_sales_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    BATCH_LIMIT = 1000  # Загружаем пачками
    SHEMA_TABLE = 'dds.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmFctsOriginRepository(pg_origin)
        self.dds = DmFctsDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log

    def get_dm_product_id(self, product_id, update_ts) -> int:
        with self.pg_dest.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_products as p
                    WHERE p.product_id = %(product_id)s
                        AND p.active_to = %(update_ts)s
                """,
                {
                    "product_id": product_id,
                    "update_ts": update_ts,
                },
            )
            x = cur.fetchall()
            if x !=[]:
                objs = x[0][0]
            else:
                objs = 0
        return objs


    def get_dm_order_id(self, order_id) -> int:
        with self.pg_dest.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_orders as o
                    WHERE o.order_key = %(order_id)s
                """,
                {
                    "order_id": order_id,
                },
            )
            x = cur.fetchall()
            if x !=[]:
                objs = x[0][0]
            else:
                objs = 0
        return objs
    

    def parse_of_data(self, raws: List[DmFctDestObj]) -> List[DmFctDestObj]:
        res = []
        for r in raws:
            json_of_data = json.loads(r.event_value)
            json_of_data_pm = json_of_data["product_payments"]
            order_id = self.get_dm_order_id(json_of_data['order_id'])
            if order_id == 0:
                continue

            for product in json_of_data_pm:                   
                t = DmFctDestObj(
                                    id = r.id,
                                    product_id=self.get_dm_product_id(product["product_id"], datetime(year=2099, month=12, day=31)),
                                    order_id=order_id,
                                    price=product["price"],
                                    count=product["quantity"],
                                    total_sum=product["product_cost"],
                                    bonus_payment=product["bonus_payment"],
                                    bonus_grant=product["bonus_grant"] , 
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
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_fcts(last_loaded, self.BATCH_LIMIT)

            # выбираем функцию для парсинга в зависимости от вида таблицы
            data_to_load = self.parse_of_data(load_queue)
            self.log.info(f"Found {len(data_to_load)} dm_fcts to load.")

            if not data_to_load:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for row in data_to_load:
                self.dds.insert_to_db(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in data_to_load])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


