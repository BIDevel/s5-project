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


class DmRestaurantOriginObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class DmRestaurantDestObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime


class DmRestaurantsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, dm_restaurant_threshold: str, limit: int) -> List[DmRestaurantOriginObj]:
        with self._db.client().cursor(row_factory=class_row(DmRestaurantOriginObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем пачку объектов.
                """, {
                    "threshold": dm_restaurant_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmRestaurantsDestRepository:

    def insert_to_db(self, conn: Connection, dm_restaurants: DmRestaurantDestObj) -> None:
        # Сюда данные попадают уже в формате DmRestaurantDestObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    on conflict (restaurant_id, active_to) do update SET
                        active_to = EXCLUDED.active_from;
                """,
                {
                    "restaurant_id": dm_restaurants.restaurant_id,
                    "restaurant_name": dm_restaurants.restaurant_name,
                    "active_from": dm_restaurants.active_from,
                    "active_to": dm_restaurants.active_to,
                },
            )


class DmRestaurantLoader:
    WF_KEY = "dds_dm_restaurants_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    BATCH_LIMIT = 1000  # Загружаем пачками
    SHEMA_TABLE = 'dds.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmRestaurantsOriginRepository(pg_origin)
        self.dds = DmRestaurantsDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log


    def parse_of_data(self, raws: List[DmRestaurantDestObj]) -> List[DmRestaurantDestObj]:
        res = []
        for r in raws:
            json_of_data = json.loads(r.object_value)
            
            t = DmRestaurantDestObj(id = r.id,
                                    restaurant_id=json_of_data['_id'],
                                    restaurant_name=json_of_data['name'],
                                    active_from=datetime.strptime(json_of_data['update_ts'], "%Y-%m-%d %H:%M:%S"),
                                    active_to=datetime(year=2099, month=12, day=31)
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
            load_queue = self.origin.list_restaurants(last_loaded, self.BATCH_LIMIT)

            # выбираем функцию для парсинга в зависимости от вида таблицы
            data_to_load = self.parse_of_data(load_queue)
            self.log.info(f"Found {len(data_to_load)} dm_restaurants to load.")

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


