import logging
import pendulum

from airflow.decorators import dag, task

from cdm.cdm_courier_ledger import DmCourierLoader


from lib.pg_connect import ConnectionBuilder


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0 1 1 * *',  # Задаем расписание выполнения дага - 1- числа каждого месяца
    start_date=pendulum.datetime(2023, 7, 1, tz="UTC"),  # Дата начала выполнения дага. 
    catchup=True,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня).
    tags=['sprint5', 'cdm'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу запущен.
)
def project5_cdm_month_load_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")


    # Объявляем таск, который загружает данные.
    @task(task_id="cdm_courier_ledger")
    def load_cdm_courier_ledger():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmCourierLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_courier_ledger = load_cdm_courier_ledger()


    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    dm_courier_ledger


stg_to_dds_dag = project5_cdm_month_load_dag()
