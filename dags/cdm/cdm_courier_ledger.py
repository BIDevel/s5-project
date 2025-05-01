from logging import Logger
from typing import List
from datetime import datetime
from dateutil.relativedelta import relativedelta

from airflow.operators.python import get_current_context

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from lib.pg_connect import PgConnect
from lib.dict_util import json2str
from lib.settings_repository import EtlSetting, EtlSettingsRepository
from dateutil.relativedelta import relativedelta  


class FactsOriginObj(BaseModel):
    courier_id: int
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_reward_sum: float
    courier_tips_sum: float
    year: int
    month: int


class FactsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_reports(self, report_year: int, report_month: int ) -> List[FactsOriginObj]:
        with self._db.client().cursor(row_factory=class_row(FactsOriginObj)) as cur:
            
            cur.execute(
                """
                    with order_delivery as (
                    select
                        f.courier_id,
                        t.year "settlement_year",
                        t.month "settlement_month",
                        count(f.order_id) "orders_count",
                        sum(f.order_sum) "orders_total_sum",
                        avg(f.rate) "rate_avg",
                        sum(f.order_sum) * 0.25 "order_processing_fee",
                        sum(f.tip_sum) "courier_tips_sum",
                        case when avg(f.rate) < 4 then GREATEST(sum(f.order_sum) * 0.05, 100)
                            when 4 <= avg(f.rate) and avg(f.rate) < 4.5 then GREATEST(sum(f.order_sum) * 0.07, 150)
                            when 4.5 <= avg(f.rate) and avg(f.rate) < 4.9 then  GREATEST(sum(f.order_sum) * 0.08, 175)
                            when avg(f.rate) >= 4.9 then GREATEST(sum(f.order_sum) * 0.1, 200)
                        END "courier_order_sum",
                        t."year",
                        t."month"
                    from dds.fct_order_delivery f
                    join dds.dm_timestamps t on t.id = f.order_timestamp_id
                    where t."year" = %(year)s and t."month" = %(month)s
                    group by f.courier_id, t.year, t.month
                )
                select c.courier_id,
                    c.courier_name,
                    d.settlement_year,
                    d.settlement_month,
                    orders_count,
                    orders_total_sum,
                    rate_avg,
                    order_processing_fee,
                    courier_order_sum,
                    courier_tips_sum,
                    courier_order_sum + courier_tips_sum * 0.95 "courier_reward_sum"
                from order_delivery d
                join dds.dm_couriers c on c.id = d.courier_id;
                """, {
                    "year": report_year,
                    "month": report_month
                }
            )
            objs = cur.fetchall() 
        return objs


class DestRepository:

    def insert_to_db(self, 
                     conn: Connection, 
                     dm_reports: FactsOriginObj) -> None:
        # Сюда данные попадают уже в формате DmReportOriginObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        order_processing_fee,
                        courier_order_sum,
                        courier_tips_sum,
                        courier_reward_sum
                        )
                    VALUES (%(courier_id)s, 
                            %(courier_name)s, 
                            %(settlement_year)s, 
                            %(settlement_month)s, 
                            %(orders_count)s,
                            %(orders_total_sum)s,
                            %(rate_avg)s,
                            %(order_processing_fee)s,
                            %(courier_order_sum)s,
                            %(courier_tips_sum)s,
                            %(courier_reward_sum)s)
                    on conflict (courier_id, settlement_year, settlement_month) do update 
                    set
                        courier_name = EXCLUDED.courier_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_tips_sum = EXCLUDED.courier_tips_sum,
                        courier_reward_sum = EXCLUDED.courier_order_sum + EXCLUDED.courier_tips_sum * 0.95;
                """,
                {
                    "courier_id": dm_reports.courier_id,
                    "courier_name": dm_reports.courier_name,
                    "settlement_year": dm_reports.settlement_year,
                    "settlement_month": dm_reports.settlement_month,
                    "orders_count": dm_reports.orders_count,
                    "orders_total_sum": dm_reports.orders_total_sum,
                    "rate_avg": dm_reports.rate_avg,
                    "order_processing_fee": dm_reports.order_processing_fee,
                    "courier_order_sum": dm_reports.courier_order_sum,
                    "courier_tips_sum": dm_reports.courier_tips_sum,
                    "courier_reward_sum": dm_reports.courier_reward_sum
                },
            )

class DmCourierLoader:
    WF_KEY = "cdm_dm_courier_ledger_workflow"
    LAST_LOADED_YID_KEY = "year"
    LAST_LOADED_MID_KEY = "month"
    SHEMA_TABLE = 'cdm.srv_wf_settings'
    

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = FactsOriginRepository(pg_origin)
        self.cdm = DestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log

        previous_month_date = datetime.now() + relativedelta(months=-1)  
        self.current_month = previous_month_date.month 
        self.current_year = previous_month_date.year
         
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
                                        workflow_settings={self.LAST_LOADED_YID_KEY: datetime.now().year , self.LAST_LOADED_MID_KEY: datetime.now().month})
            

            # Вычитываем очередную пачку объектов.
            report_year = wf_setting.workflow_settings[self.LAST_LOADED_YID_KEY]
            report_month = wf_setting.workflow_settings[self.LAST_LOADED_YID_KEY]
            load_queue = self.origin.list_reports(report_year, report_month)


            # выбираем функцию для парсинга в зависимости от вида таблицы
            self.log.info(f"Found {len(load_queue)} dm_reports to load.")

            if not load_queue:
                self.log.info("Quitting.")
                return 
            # Сохраняем объекты в базу dwh.
            for row in load_queue:
                self.cdm.insert_to_db(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_YID_KEY] = self.current_year
            wf_setting.workflow_settings[self.LAST_LOADED_MID_KEY] = self.current_month
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_YID_KEY]}")


