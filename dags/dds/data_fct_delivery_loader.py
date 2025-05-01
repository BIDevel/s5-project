from logging import Logger
from typing import List, Union
from datetime import datetime

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from lib.pg_connect import PgConnect
from lib.dict_util import json2str

from lib.settings_repository import EtlSetting, EtlSettingsRepository

# d.id, orders.restaurant_id, d.courier_id, orders.id, orders.timestamp_id, d.rate, orders.order_sum, d.tip_sum, d.delivery_ts
class DmDeliveryOriginObj(BaseModel):
    delivery_id: int
    restaurant_id: int
    courier_id: int
    order_id: int
    order_timestamp_id: int
    rate: float
    order_sum: float
    tip_sum: float
    delivery_ts: datetime


class DmDeliveryDestObj(BaseModel):
    delivery_id: int
    restaurant_id: int
    courier_id: int
    order_id: int
    order_timestamp_id: int
    rate: float
    order_sum: float
    tip_sum: float
    delivery_ts: datetime



class DmDeliverysOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, last_loaded: datetime) -> Union[List[DmDeliveryOriginObj], None]:
        with self._db.client().cursor(row_factory=class_row(DmDeliveryOriginObj)) as cur:
            cur.execute(
                """with stg_orders as (
                        select o.id as "order_id",
                        (json_array_elements(o.object_value::json -> 'order_items') ->> 'price')::numeric(14, 2) as "product_price",
                        (json_array_elements(o.object_value::json -> 'order_items') ->> 'quantity')::int as "product_quantity",
                        o.object_value::json->>'final_status' as "order_status"
                        from stg.ordersystem_orders o),
                    orders_agg as (
                        select order_id,
                        sum(stg_orders.product_quantity * stg_orders.product_price) as "order_sum"
                        from stg_orders
                        where stg_orders.order_status = 'CLOSED'
                        group by stg_orders.order_id),
                    orders as (
                        select o.id, o.restaurant_id, o.timestamp_id, orders_agg.order_sum  
                        from dds.dm_orders o
                        join orders_agg on orders_agg.order_id = o.id)
                    select d.id delivery_id, orders.restaurant_id, d.courier_id, orders.id as order_id, orders.timestamp_id as order_timestamp_id, d.rate, orders.order_sum, d.tip_sum, d.delivery_ts
                    from dds.dm_deliveries d
                    join orders on orders.id = d.order_id
                    where d.delivery_ts > %(last_loaded)s
                """, {
                    "last_loaded": last_loaded 
                }
            )
            objs = cur.fetchall()
        return objs


class DmDeliverysDestRepository:

    def insert_dm_delivery(self, conn: Connection, dm_deliveries: DmDeliveryDestObj) -> None:
        # Сюда данные попадают уже в формате DmDeliveryDestObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_order_delivery(delivery_id, restaurant_id, courier_id, order_id, order_timestamp_id, rate, order_sum, tip_sum
                    )
                    VALUES (
                        %(delivery_id)s, 
                        %(restaurant_id)s, 
                        %(courier_id)s,
                        %(order_id)s,
                        %(order_timestamp_id)s,
                        %(rate)s,
                        %(order_sum)s,
                        %(tip_sum)s                       
                    )
                    on conflict (order_id) do update set
                        delivery_id = EXCLUDED.delivery_id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        courier_id = EXCLUDED.courier_id,
                        order_id = EXCLUDED.order_id,
                        order_timestamp_id = EXCLUDED.order_timestamp_id,
                        rate = EXCLUDED.rate,
                        order_sum = EXCLUDED.order_sum,
                        tip_sum = EXCLUDED.tip_sum;

                """,
                {
                    "delivery_id": dm_deliveries.delivery_id, 
                    "restaurant_id": dm_deliveries.restaurant_id,
                    "courier_id": dm_deliveries.courier_id,
                    "order_id": dm_deliveries.order_id,
                    "order_timestamp_id": dm_deliveries.order_timestamp_id,
                    "rate": dm_deliveries.rate,
                    "order_sum": dm_deliveries.order_sum,
                    "tip_sum": dm_deliveries. tip_sum 
                },
            )


class DmFctDeliveryLoader:
    WF_KEY = "dds_fct_deliveries_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    BATCH_LIMIT = 1000  # Загружаем пачками
    SHEMA_TABLE = 'dds.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmDeliverysOriginRepository(pg_origin)
        self.dds = DmDeliverysDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log


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
            load_queue = self.origin.list_deliveries(last_loaded)

            self.log.info(f"Found {len(load_queue)} dm_deliveries to load.")

            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for row in load_queue:
                self.dds.insert_dm_delivery(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.delivery_ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


