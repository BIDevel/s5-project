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


class DmUserOriginObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class DmUserDestObj(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str


class DmUsersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self, dm_user_threshold: str, limit: int) -> List[DmUserOriginObj]:
        with self._db.client().cursor(row_factory=class_row(DmUserOriginObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_users
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем пачку объектов.
                """, {
                    "threshold": dm_user_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmUsersDestRepository:

    def insert_dm_user(self, conn: Connection, dm_users: DmUserDestObj) -> None:
        # Сюда данные попадают уже в формате DmUserDestObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users(user_id, user_name, user_login)
                    VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        user_name = EXCLUDED.user_name,
                        user_login = EXCLUDED.user_login;
                """,
                {
                    "user_id": dm_users.user_id,
                    "user_name": dm_users.user_name,
                    "user_login": dm_users.user_login
                },
            )


class DmUserLoader:
    WF_KEY = "dds_dm_users_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    BATCH_LIMIT = 1000  # Загружаем пачками
    SHEMA_TABLE = 'dds.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmUsersOriginRepository(pg_origin)
        self.dds = DmUsersDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log


    def parse_dm_users(self, raws: List[DmUserDestObj]) -> List[DmUserDestObj]:
        res = []
        for r in raws:
            user_json = json.loads(r.object_value)

            t = DmUserDestObj(id = r.id,
                              user_id=user_json['_id'],
                              user_name=user_json['name'],
                              user_login=user_json['login'],
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
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_users(last_loaded, self.BATCH_LIMIT)

            # выбираем функцию для парсинга в зависимости от вида таблицы
            if entity_to_upload == "dm_users_load":
                users_to_load = self.parse_dm_users(load_queue)
            self.log.info(f"Found {len(users_to_load)} dm_users to load.")

            if not users_to_load:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for row in users_to_load:
                self.dds.insert_dm_user(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in users_to_load])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


