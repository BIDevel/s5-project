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


class DdsDeliveriesOriginObj(BaseModel):
    delivery_id: str
    courier_id: str
    order_id: str
    address: str
    delivery_ts: datetime
    rate: int
    tip_sum: float


class DdsDeliveriesDestObj(BaseModel):
    delivery_id: str
    courier_id: str
    order_id: str
    address: str
    delivery_ts: datetime
    rate: int
    tip_sum: float


class DdsDeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, last_loaded: datetime) -> List[DdsDeliveriesOriginObj]:
        with self._db.client().cursor(row_factory=class_row(DdsDeliveriesOriginObj)) as cur:
            cur.execute(
                """
                    select object_id delivery_id,
                    courier.id courier_id,
                    orders.id order_id,
                    object_value->>'address' address,
                    delivery_ts,
                    (object_value->>'rate')::int rate,
                    (object_value->>'tip_sum')::numeric(14,2) tip_sum
                from stg.deliverysystem_deliveries delivery
                        join dds.dm_couriers courier on courier.courier_id = delivery.object_value->>'courier_id'
                        join dds.dm_orders orders on orders.order_key = delivery.object_value->>'order_id'
                where delivery_ts > %(last_loaded)s
                """, {
                    "last_loaded": last_loaded 
                }
            )
            objs = cur.fetchall()
        return objs


class DdsDeliveriesDestRepository:

    def insert_dm_deliveries(self, conn: Connection, dm_deliviries: DdsDeliveriesDestObj) -> None:
        # Сюда данные попадают уже в формате DmUserDestObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(delivery_id, courier_id, order_id, address, delivery_ts, rate, tip_sum)
                    VALUES (%(delivery_id)s, %(courier_id)s, %(order_id)s, %(address)s, %(delivery_ts)s, %(rate)s, %(tip_sum)s)
                    on conflict (order_id) do update set
                            delivery_id = EXCLUDED.delivery_id,
                            courier_id = EXCLUDED.courier_id,
                            address = EXCLUDED.address,
                            delivery_ts = EXCLUDED.delivery_ts,
                            rate = EXCLUDED.rate,
                            tip_sum = EXCLUDED.tip_sum;
                """,
                {
                    "delivery_id": dm_deliviries.delivery_id,
                    "courier_id": dm_deliviries.courier_id,
                    "order_id": dm_deliviries.order_id,
                    "address": dm_deliviries.address,
                    "delivery_ts": dm_deliviries.delivery_ts,
                    "rate": dm_deliviries.rate,
                    "tip_sum": dm_deliviries.tip_sum
                },
            )


class DdsDeliveryLoader:
    WF_KEY = "dds_dm_deliveries_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    SHEMA_TABLE = 'dds.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DdsDeliveriesOriginRepository(pg_origin)
        self.dds = DdsDeliveriesDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log


    def parse_of_data(self, raws: List[DdsDeliveriesDestObj]) -> List[DdsDeliveriesDestObj]:
        res = []
        for r in raws: 
            t = DdsDeliveriesDestObj(
                                delivery_id = r.delivery_id,
                                courier_id = r.courier_id,
                                order_id = r.order_id,
                                address = r.address,
                                delivery_ts = r.delivery_ts,
                                rate = r.rate,
                                tip_sum = r.tip_sum
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
            load_queue = self.origin.list_deliveries(last_loaded)
            
            data_to_load = self.parse_of_data(load_queue)
            
            # выбираем функцию для парсинга в зависимости от вида таблицы
            self.log.info(f"Found {len(data_to_load)} dm_deliveries to load.")

            if not data_to_load:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for row in data_to_load:
                self.dds.insert_dm_deliveries(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.delivery_ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


