# Документация по созданию хранилища данных для формирования двух витрин данных с информацией о взаиморасчетах с контрагентами и выплатах курьерам.

# I. Требования к системе
## 1. Общие требования
	Хранить историю данных с источника AS IS (пер. «как есть»).

	В этом случае вероятность ошибки при преобразовании данных уменьшится и можно будет в любой момент времени точно оценить, какие данные были в источнике.
	Стабильность при недоступности одного или нескольких источников.

	Процессы ETL не должны портить данные, если источник не будет доступен.
	Модель данных «Снежинка» в слое DDS.

	«Снежинка» позволит быстрее и удобнее формировать новые витрины и соответствующие отчёты. О слое DDS мы ещё расскажем в следующей теме.
	Стабильность при изменении формата данных в источниках.

	Процессы ETL не должны портить данные, если формат данных в источниках изменится.

## 2. Бизнес-требования: Необходимо построить хранилище данных для формирования двух витрин.
###  2.2. Витрина с информацией о взаиморасчетах с контрагентами. 
* Название витрины **dm_settlement_report**
* Источники данных - **подсистемы данных обработки заказов** и **данных по оплате бонусами**
* В витрине должны быть следующие поля:
	* id — идентификатор записи;
	* restaurant_id — идентификатор ресторана (строковый, из системы-источника);
	* restaurant_name — название ресторана;
	* settlement_date — дата отчёта;
	* orders_count — количество заказов;
	* orders_total_sum — общая сумма заказов клиентов;
	* orders_bonus_payment_sum — сумма оплат бонусами;
	* orders_bonus_granted_sum — сумма накопленных бонусов;
	* order_processing_fee — сумма, удержанная компанией за обработку заказов;
	* restaurant_reward_sum — сумма, которую необходимо перечислить ресторану.


###  2.2. Витрина, содержащую информацию о выплатах курьерам. 
* Название витрины **dm_courier_ledger**
* Источники данных - **подсистемы данных курьерской службы**
* В витрине должны быть следующие поля:
	* id — идентификатор записи.
	* courier_id — ID курьера, которому перечисляем.
	* courier_name — Ф. И. О. курьера.
	* settlement_year — год отчёта.
	* settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.
	* orders_count — количество заказов за период (месяц).
	* orders_total_sum — общая стоимость заказов.
	* rate_avg — средний рейтинг курьера по оценкам пользователей.
	* order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
	* courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в * * зависимости от рейтинга (см. ниже).
	* courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
	* courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).

* Правила расчёта процента выплаты курьеру в зависимости от рейтинга, где r — это средний рейтинг курьера в расчётном месяце:
	r < 4 — 5% от заказа, но не менее 100 р.;
	4 <= r < 4.5 — 7% от заказа, но не менее 150 р.;
	4.5 <= r < 4.9 — 8% от заказа, но не менее 175 р.;
	4.9 <= r — 10% от заказа, но не менее 200 р.

* Отчёт собирается по дате заказа. Если заказ был сделан ночью и даты заказа и доставки не совпадают, в отчёте стоит ориентироваться на дату заказа, а не дату доставки. Иногда заказы, сделанные ночью до 23:59, доставляют на следующий день: дата заказа и доставки не совпадёт. Это важно, потому что такие случаи могут выпадать в том числе и на последний день месяца. Тогда начисление курьеру относите к дате заказа, а не доставки.


## 3. Хранилище должно строится на основе трех источников данных, с возможностью их расширения. Сейчас источники такие :
- Система обработки заказов \ информация о заказах \ MongoDB ;
	- Документоориентированная база MongoDB с информацией о ресторанах и заказах.
	 	Данные находятся в облачном БД MONGO ВИ по следующим параметрам: 

		'''
			General			Connection String Scheme		mongodb
			Text			Host							rc1a-ba83ae33hvt4pokq.mdb.yandexcloud.net:27018
			Authentication	Authentication 					Method	Username/Password
			Text			Username						student
			Text			Password						student1
			Text			Authentication Database			db-mongo
			Text			Authentication Mechanism		Default
			TLS/SSL			TLS/SSL Connection				On
			Text			Certificate Authority (.pem)	Выберите файл с сертификатом, который сохранили ранее.
			Advanced		Read Preference					Default
			Text			Replica Set Name				rs01
			Text			Default Authentication Database	db-mongo
		'''

		- таблицы:
			- orders
			- restaurants
			- users

- Система оплаты баллами \ данные по оплате бонусами \PostgreSQL.
	- Реляционная база данных PostgreSQL с информацией о бонусах клиентов, набор таблиц в 3НФ; 
		Данные находятся в облачном БД по следующим параметрам: 

		'''
			Host	        rc1a-1kn18k47wuzaks6h.mdb.yandexcloud.net
			Port	        6432
			Database	    de-public
			Username	    student
			Password	    student1
			SSL	            Use SSL
			CA Certificate	Выберите файл с сертификатом, который сохранили ранее.
			SSL Mode	    verify-full 
		'''

		- схема Public 
		- таблицы:
			- outbox
			- ranks
			- users
	
- Система данных курьерской службы. Эти данные загружаются из API 
	- Спецификация GET /couriers
		- Метод /couriers используется для того, чтобы получить список курьеров с учётом фильтров, переданных в запросе.
		- Доступ по следующей спецификации 

			'''
				curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ offset }}'
				--header 'X-Nickname: {{ your_nickname }}' 
				--header 'X-Cohort: {{ your_cohort_number }}'
				--header 'X-API-KEY: {{ api_key }}' 
			'''

		- Метод возвращает список курьеров. Каждый элемент списка содержит: 
			* _id — ID курьера в БД;
			* name — имя курьера.
	- Спецификация GET / deliveries 
		- Метод /deliveries используется для того, чтобы получить список совершённых доставок с учётом фильтров, переданных в запросе.
		- Доступ по следующей спецификации 

			'''
				curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?restaurant_id={{ restaurant_id }}&from={{ from }}&to={{ to }}&sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ limit }}' --header 'X-Nickname: {{ your_nickname }}' \
				--header 'X-Cohort: {{ your_cohort_number }}' \
				--header 'X-API-KEY: {{ api_key }}'  
			'''

		- Метод возвращает список совершённых доставок с учётом фильтров в запросе.  Каждый элемент списка содержит: 
			* order_id — ID заказа;
			* order_ts — дата и время создания заказа;
			* delivery_id — ID доставки;
			* courier_id — ID курьера;
			* address — адрес доставки;
			* delivery_ts — дата и время совершения доставки;
			* rate — рейтинг доставки, который выставляет покупатель: целочисленное значение от 1 до 5;
			* tip_sum — сумма чаевых, которые оставил покупатель курьеру (в руб.).

## 4. Настроить повторное обращение к источникам. Источники могут быть недоступны по несколько часов
## 5. Учесть дубликаты. В источниках данные могут обновляться и дублироваться
## 6. Учесть изменение формата данных. В источниках данные могут менять свой формат

# II. Выполненная работа
	1. Обеспечен доступ к перечисленным источникам данных, а именно к 
		- подсистеме данных обработки заказов (Mongodb)
		- подсистеме данных по оплате бонусами (Postgresql)
		- подсистеме данных курьерской службы (API)
	2. Спроектированj DWH хранилище со следующими слоями
		- stg (слой сырых данных) AS IS, 3NF
		- dds (слой детальных данных) модель данных "снежинка"
		- cdm (слой витрин)
	3. Спроектирован механизм инкрементальной загрузки данных при помощи технических таблиц **srv_wf_settings** для каждого слоя DWH. Каждая следующая загрузка осуществляется исходя из метки предыдущей загрузки
	4. Созданы схемы и таблицы для каждого слоя 
		- Создание технических таблиц меток для инкрементной загрузки
		stg.srv_wf_settings
		cdm.srv_wf_settings
		dds.srv_wf_settings

		- Создание таблиц для данных из Mongo db , подсистема заказов
		stg.ordersystem_orders
		stg.ordersystem_restaurants
		stg.ordersystem_users

		- Создание таблиц для данных из PG db , подсистема бонусов
		stg.bonussystem_users
		stg.bonussystem_ranks
		stg.bonussystem_events

		- Создание таблиц для данных из API, подсистема доставки 
		stg.deliverysystem_couriers
		stg.deliverysystem_deliveries

		- Создание таблиц для витрины dm_settlement_report
		dds.dm_users
		dds.dm_restaurants
		dds.dm_timestamps
		dds.dm_products
		dds.dm_orders
		dds.fct_product_sales

		- Создание таблиц для витрины dm_courier_ledger
		dds.dm_couriers		
		dds.dm_deliveries
		dds.fct_order_delivery

		- Создание таблиц витрин
		cdm.dm_settlement_report
		cdm.dm_courier_ledger
	
	5. для создания схем и таблиц в клиенте PostgreSQL:
			* init.sql - запускается однократно, в каталоге init
			* init_delivery.sql - запускается однократно, в каталоге init
		
	6. Реализован механизм загрузки данных в указанные таблицы по средствам ПО Airflow, путем создания нескольких DAG:
			

		- Для заполнения слоя stg:
			* bonus_system_ranks_dag.py 		- запускается раз в 20 минут
			* order_system_restaurants_dag.py 	- запускается раз в 20 минут
			* api_deliverys_system_dag.py 		- запускается раз в 20 минут

		- Для заполнения слоя dds:
			* data_load_dag.py   				- запускается раз в 20 минут

		- Для заполнения слоя cdm:
			* cdm_day_load_dag.py 				- запускается раз в день
			* cdm_month_load_dag.py    			- запускается раз в месяц

# III. Подтверждение результата
	Реализованный механизм создания структуры и заполнения хранилища данных был протестирован с использованием боевых источников путем развертования системы на локальной машине с использованием иснтрумента контейнерезации Docker.

# IV. Структура репозитория

	'''
    ├──dags
    |   ├── cdm
    |   |   ├── cdm_day_load_dag.py 
    |   |   ├── cdm_month_load_dag.py
    |   |   ├── cdm_settlement_loader.py 
    |   |	└── cdm_courier_ledger.py
    |   |
    |   ├── dds
    |   |    ├── data_couriers_loader.py
    |   |    ├── data_deliveries_loader.py
    |   |    ├── data_fct_delivery_loader.py
    |   |    ├── data_fct_product_sales_loader.py
    |   |    ├── data_products_loader.py 
    |   |    ├── data_orders_loader.py 
    |   |    ├── data_restaurants_loader.py
    |   |    ├── data_timestamps_loader.py 
    |   |    ├── data_users_loader.py 
    |   |	 └── data_load_dag.py
    |   |
    |   └── stg
    |        ├── api_deliverys_system_dag
    |        |    ├── api_delivery_system_dag.py 
    |        |    ├── api_reader.py 
    |        |    ├── couriers_loader.py 
    |        |    └── deliveries_loader.py
    |        |	 
    |        ├── bonus_system_ranks_dag 
    |        |    ├── bonus_system_ranks_dag.py
    |        |    ├── events_loader.py 
    |        |    ├── ranks_loader.py 
    |        |    └── users_loader.py
    |        | 
    |        └── order_system_restaurants_dag.py  
    |             ├── order_loader.py 
    |             ├── order_reader.py 
    |             ├── order_system_restaurants_dag.py 
    |             ├── pg_saver_orders.py 
    |             ├── pg_saver_restaurants.py 
    |             ├── pg_saver_users.py 
    |             ├── restaurant_loader.py 
    |             ├── restaurant_reader.py 
    |             ├── user_loader.py 
    |             └── user_reader.py 
    |    
    └── init
         ├── init_delivery.sql             
         └── init.sql          
	'''

