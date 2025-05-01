from logging import Logger
from typing import List
import json

from psycopg import Connection
from psycopg.rows import class_row, dict_row
from pydantic import BaseModel

from lib.pg_connect import PgConnect
from lib.dict_util import json2str

from lib.settings_repository import EtlSetting, EtlSettingsRepository


class DdsCourierOriginObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class DdsCourierDestObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str



class DdsCouriersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, dm_courier_threshold: str, limit: int) -> List[DdsCourierOriginObj]:
        with self._db.client().cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value
                    FROM stg.deliverysystem_couriers
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем пачку объектов.
                """, {
                    "threshold": dm_courier_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DdsCouriersDestRepository:
    def insert_dds_courier(self, conn: Connection, dm_couriers: DdsCourierDestObj) -> None:
        # Сюда данные попадают уже в формате CourierDestObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        courier_name = EXCLUDED.courier_name;
                """,
                {
                    "courier_id": dm_couriers.courier_id,
                    "courier_name": dm_couriers.courier_name
                },
            )


class DdsCourierLoader:
    WF_KEY = "dds_dm_couriers_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    BATCH_LIMIT = 1000  # Загружаем пачками
    SHEMA_TABLE = 'dds.srv_wf_settings'

    
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DdsCouriersOriginRepository(pg_origin)
        self.dds = DdsCouriersDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log

    def parse_dds_couriers(self, raws: List[dict]) -> List[DdsCourierDestObj]:
        res = []
        for r in raws:
            t = DdsCourierDestObj(id = r['id'],
                                  courier_id=r['object_value']['_id'],
                                  courier_name=r['object_value']['name'],
                             )

            res.append(t)
        return res
    

    def data_load(self, entity_to_upload):
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
            load_queue = self.origin.list_couriers(last_loaded, self.BATCH_LIMIT)

            self.log.info(f"Found {len(load_queue)} dds_couriers to load.")

            if not load_queue:
                self.log.info("Quitting.")
                return

            print(load_queue)
            if entity_to_upload == "dds_couriers_load":
                couries_to_load = self.parse_dds_couriers(load_queue)
            
            # Сохраняем объекты в базу dwh.
            for row in couries_to_load:
                self.dds.insert_dds_courier(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in couries_to_load])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


