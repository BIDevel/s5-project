from logging import Logger
from typing import List
from datetime import datetime
import json

from psycopg import Connection
from pydantic import BaseModel

from lib.pg_connect import PgConnect
from lib.dict_util import json2str

from stg.api_delivery_system_dag.api_reader import ApiConnect
from lib.settings_repository import EtlSetting, EtlSettingsRepository


class DeliveryObj(BaseModel):
    object_id: str
    object_value: str
    object_ts: str

class DeliveriesOriginRepository:
    def __init__(self) -> None:
        pass

    def list_deliveries(self, sort:str, threshold: int, limit: int)-> List[DeliveryObj]:
        x = ApiConnect('deliveries', sort, limit , threshold)
        x.client()

        return x.client()


class DeliveryDestRepository:
    def insert_delivery(self, conn: Connection, delivery: dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliveries
                        (object_id, object_value, delivery_ts)
                    VALUES (%(object_id)s, 
                            %(object_value)s, 
                            %(delivery_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        delivery_ts = EXCLUDED.delivery_ts;
                """,
                {
                    "object_value": delivery["object_value"],
                    "delivery_ts": delivery["delivery_ts"],
                    "object_id": delivery["object_id"] 
                },
            )


class DeliveriesLoader:
    WF_KEY = "stg_from_api_delivery_deliveries_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50   
    SHEMA_TABLE = 'stg.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveriesOriginRepository()
        self.stg = DeliveryDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log

    def load_deliveries(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        while True:
            with self.pg_dest.connection() as conn:

                # Прочитываем состояние загрузки
                # Если настройки еще нет, заводим ее.
                wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)

                if not wf_setting:
                    wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: 0})

                # Вычитываем очередную пачку объектов.
                
                last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
                load_queue = self.origin.list_deliveries(sort="id", threshold=last_loaded, limit=self.BATCH_LIMIT)
                self.log.info(f"Found {len(load_queue)} deliverys to load.")

                if not load_queue:
                    self.log.info("Quitting.")
                    return

                # Сохраняем объекты в базу dwh.
                load_queue = [{'object_value': json.dumps(lq), 'object_id': lq['delivery_id'], 'delivery_ts': lq['delivery_ts']} for lq in load_queue]
                for delivery in load_queue:
                    self.stg.insert_delivery(conn, delivery)

                # Сохраняем прогресс.
                # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
                # либо откатятся все изменения целиком.
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded + len(load_queue)
                wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.

                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
        

