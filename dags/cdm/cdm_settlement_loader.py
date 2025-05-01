from logging import Logger
from typing import List
from datetime import datetime, date

from airflow.operators.python import get_current_context

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from lib.pg_connect import PgConnect
from lib.dict_util import json2str
from lib.settings_repository import EtlSetting, EtlSettingsRepository


class DmReportOriginObj(BaseModel):
    restaurant_id: str
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float



class DmReportsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_reports(self, prev_date: date ) -> List[DmReportOriginObj]:
        with self._db.client().cursor(row_factory=class_row(DmReportOriginObj)) as cur:
            cur.execute(
                """
                    with restaurant_product as (
                        select p.id as "p_id",
                            p.product_id as "product_id",
                            r.restaurant_id,
                            r.restaurant_name,
                            r.id as "r_id",
                            p.active_from
                        from dds.dm_products p
                        join dds.dm_restaurants r on r.id = p.restaurant_id and r.active_from=p.active_from
                        where p.active_to = '2099-12-31 00:00:00'::timestamp
                    ),
                    restaurant_product_sales as (
                        select t.date,
                            fps.order_id,
                            rp.restaurant_id,
                            rp.restaurant_name,
                            sum(fps.total_sum) as "orders_total_sum",
                            sum(fps.bonus_payment) as "orders_bonus_payment_sum",
                            sum(fps.bonus_grant) as "orders_bonus_granted_sum",
                            sum(fps.total_sum) * 0.25 as "order_processing_fee",
                            sum(fps.total_sum) - sum(fps.total_sum) * 0.25 - sum(fps.bonus_payment) as "restaurant_reward_sum"
                        from dds.fct_product_sales fps
                        join dds.dm_timestamps t on t.id = fps.order_id
                        join restaurant_product rp on rp.p_id=fps.product_id
                        group by fps.order_id, t.date, rp.restaurant_id, rp.restaurant_name
                    )
                        select rps.restaurant_id,
                            rps.restaurant_name,
                            rps.date as "settlement_date",
                            count(rps.order_id) as "orders_count",
                            sum(rps.orders_total_sum) as "orders_total_sum",
                            sum(rps.orders_bonus_payment_sum) as "orders_bonus_payment_sum",
                            sum(rps.orders_bonus_granted_sum) as "orders_bonus_granted_sum",
                            sum(rps.order_processing_fee) as "order_processing_fee",
                            sum(rps.restaurant_reward_sum) as "restaurant_reward_sum"
                        from restaurant_product_sales rps
                        where rps.date > %(prev_date)s
                        group by rps.date, rps.restaurant_id, rps.restaurant_name
                """, {
                        "prev_date": prev_date
                }
            )
            objs = cur.fetchall() 
        return objs


class DmReportsDestRepository:
    def insert_to_db(self, conn: Connection, dm_reports: DmReportOriginObj) -> None:
        # Сюда данные попадают уже в формате DmReportOriginObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    insert into cdm.dm_settlement_report(restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                    VALUES (%(restaurant_id)s,  %(restaurant_name)s,%(settlement_date)s, %(orders_count)s, %(orders_total_sum)s,%(orders_bonus_payment_sum)s,%(orders_bonus_granted_sum)s,%(order_processing_fee)s,%(restaurant_reward_sum)s)
                """,
                {
                    "restaurant_id": dm_reports.restaurant_id,
                    "restaurant_name": dm_reports.restaurant_name,
                    "settlement_date": dm_reports.settlement_date,
                    "orders_count": dm_reports.orders_count,
                    "orders_total_sum": dm_reports.orders_total_sum,
                    "orders_bonus_payment_sum": dm_reports.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": dm_reports.orders_bonus_granted_sum,
                    "order_processing_fee": dm_reports.order_processing_fee,
                    "restaurant_reward_sum": dm_reports.restaurant_reward_sum,
                },
            )

class DmReportLoader:
    WF_KEY = "cdm_dm_settlement_report_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    SHEMA_TABLE = 'cdm.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmReportsOriginRepository(pg_origin)
        self.cdm = DmReportsDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log    
        


    def parse_of_data(self, raws: List[DmReportOriginObj]) -> List[DmReportOriginObj]:
        res = []
        
        for r in raws:
            t = DmReportOriginObj(
                                restaurant_id=r.restaurant_id,
                                restaurant_name=r.restaurant_name,
                                settlement_date=r.settlement_date,
                                orders_count=r.orders_count,
                                orders_total_sum=r.orders_total_sum,
                                orders_bonus_payment_sum=r.orders_bonus_payment_sum,
                                orders_bonus_granted_sum=r.orders_bonus_granted_sum,
                                order_processing_fee=r.order_processing_fee,
                                restaurant_reward_sum=r.restaurant_reward_sum
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
                wf_setting = EtlSetting(id=0, 
                                        workflow_key=self.WF_KEY, 
                                        workflow_settings={self.LAST_LOADED_ID_KEY: date(1900,1,1)})

            # Вычитываем очередную пачку объектов.
            print(wf_setting)
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_reports(last_loaded)


            # выбираем функцию для парсинга в зависимости от вида таблицы
            data_to_load = self.parse_of_data(load_queue)
            self.log.info(f"Found {len(data_to_load)} dm_reports to load.")

            if not data_to_load:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for row in data_to_load:
                self.cdm.insert_to_db(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.settlement_date for t in data_to_load]).strftime('%Y-%m-%d')
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


