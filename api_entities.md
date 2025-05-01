# 1. Список полей, которые необходимы для витрины.:
		id                   — идентификатор записи.
	new	courier_id           — ID курьера, которому перечисляем.
	new	courier_name         — Ф. И. О. курьера.
		settlement_year      — год отчёта.
		settlement_month     — месяц отчёта, где 1 — январь и 12 — декабрь.
		orders_count         — количество заказов за период (месяц).
		orders_total_sum     — общая стоимость заказов.
	new	rate_avg             — средний рейтинг курьера по оценкам пользователей.
		order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
	new	courier_order_sum	 — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
	new	courier_tips_sum 	 — сумма, которую пользователи оставили курьеру в качестве чаевых.
	new	courier_reward_sum   — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).


	## Правила расчёта процента выплаты курьеру в зависимости от рейтинга, где r — это средний рейтинг курьера в расчётном месяце:
		r < 4 — 5% от заказа, но не менее 100 р.;
		4 <= r < 4.5 — 7% от заказа, но не менее 150 р.;
		4.5 <= r < 4.9 — 8% от заказа, но не менее 175 р.;
		4.9 <= r — 10% от заказа, но не менее 200 р.

# 2. Список таблиц в слое DDS, из которых необходимо взять поля для витрины. 

	curent table
	dds.dm_timestamps
	
	need table 
	dds.dm_couriers
	dds.dm_deliveries
	dds.fct_deliveries_made
	
	
# 3. На основе списка таблиц в DDS составляем список сущностей и полей, которые необходимо загрузить из API. Использовать все методы API необязательно: важно загрузить ту информацию, которая нужна для выполнения задачи.
	
## Анализ источников API:
	- **GET /couriers** возращает следующие параметры:
		-- _id 
		-- name
	- **GET / deliveries** возращает следующие параметры:
		-- order_id
		-- order_ts
		-- delivery_id
		-- courier_id
		-- address
		-- delivery_ts
		-- rate
		-- sum
		-- tip_sum
	- **GET /restaurants** возращает следующие параметры:
		-- id
		-- name
		
### Результат анализа
	Используем только **GET /couriers** и **GET / deliveries**. Информация из **GET /restaurants** в БД уже загружена
		
	
	- Для dds.dm_couriers необходимы следующие сущности из **GET /couriers**
		-- courier_id 
		-- courier_name

	- Для dds.dm_deliveries необходимы следующие сущности из **GET / deliveries**
		-- order_id
		-- order_ts
		-- delivery_id
		-- courier_id
		-- rate 
		-- sum
		-- tip_sum
	
### Создаем таблицы в stg и загружаем данные из выбранных API
	courier_id 
	courier_name
	user ratings       = AVG rate
	courier_order_sum  = SUM orders_total_sum * percent
	courier_tips_sum   = SUM tips
	courier_reward_sum = courier_order_sum + courier_tips_sum * 0.95