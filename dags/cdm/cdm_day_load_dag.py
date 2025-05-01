import logging
import pendulum

from airflow.decorators import dag, task

from cdm.cdm_settlement_loader import DmReportLoader

from lib.pg_connect import ConnectionBuilder


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0 1 * * *',  # Задаем расписание выполнения дага - каждый день
    start_date=pendulum.datetime(2025, 5, 1, tz="UTC"),  # Дата начала выполнения дага. 
    catchup=True,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня).
    tags=['sprint5', 'cdm', 'day'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу запущен.
)
def project5_cdm_day_load_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_settlement_report")
    def load_dm_settlement_report():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmReportLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_settlement_report_dict = load_dm_settlement_report()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    dm_settlement_report_dict


stg_to_dds_dag = project5_cdm_day_load_dag()
