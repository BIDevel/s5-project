-- CDM
drop table if exists cdm.dm_courier_ledger CASCADE;

create table cdm.dm_courier_ledger(
    id serial primary key,
    courier_id varchar not null,
    courier_name varchar not null,
    settlement_year smallint not null,
    settlement_month smallint not null,
    orders_count smallint not null default 0,
    orders_total_sum numeric(14,2) not null default 0,
    rate_avg numeric(3,2) not null,
    order_processing_fee numeric(14,2) not null default 0,
    courier_order_sum numeric(14,2) not null default 0,
    courier_tips_sum numeric(14,2) not null default 0,
    courier_reward_sum numeric(14,2) not null default 0,
    constraint dm_delivery_report_settlement_year_check check ( settlement_year > 2021 and settlement_year < 2099 ),
    constraint dm_delivery_report_settlement_month_check check ( settlement_month > 0 and settlement_month < 13 ),
    constraint dm_delivery_report_orders_count_check check ( orders_count >= 0 ),
    constraint dm_delivery_report_orders_total_sum_check check ( orders_total_sum >= 0 ),
    constraint dm_delivery_report_order_processing_fee_check check ( order_processing_fee >= 0 ),
    constraint dm_delivery_report_courier_order_sum_check check ( courier_order_sum >= 0 ),
    constraint dm_delivery_report_courier_tips_sum_check check ( courier_tips_sum >= 0 ),
    constraint dm_delivery_report_courier_reward_sum_check check ( courier_reward_sum >= 0 ),
    constraint dm_delivery_report_courier_id_settlement_year_month_unique unique (courier_id, settlement_year, settlement_month),
    constraint dm_delivery_report_rate_avg check ( rate_avg>=1 and rate_avg<=5 )
);

-- STG
drop table if exists stg.deliverysystem_couriers CASCADE;

create table stg.deliverysystem_couriers(
    id int generated always as identity primary key,
    object_id varchar not null,
    object_value json not null,
    constraint deliverysystem_couriers_unique unique (object_id)
);

drop table if exists stg.deliverysystem_deliveries CASCADE;

create table stg.deliverysystem_deliveries(
    id int generated always as identity primary key,
    object_id varchar not null,
    object_value json not null,
    delivery_ts timestamp not null,
    constraint deliverysystem_deliveries_unique unique (object_id)
);

-- DDS
drop table if exists dds.dm_couriers CASCADE;

create table dds.dm_couriers(
    id serial not null primary key,
    courier_id varchar not null,
    courier_name  varchar not null,
    constraint dds_dm_couriers_courier_id_unique unique (courier_id)
);

drop table if exists dds.dm_deliveries CASCADE;

create table dds.dm_deliveries(
    id serial not null primary key,
    delivery_id varchar not null,
    courier_id int not null,
    order_id int not null,
    address varchar not null,
    delivery_ts timestamp not null,
    rate smallint not null,
    tip_sum numeric(14,2) not null default 0,
    constraint dm_deliveries_courier_id_fk FOREIGN KEY (courier_id) references dds.dm_couriers(id),
    constraint dm_deliveries_order_id_fk FOREIGN KEY (order_id) references dds.dm_orders(id),
    constraint dm_delivery_rate_check check ( rate>=1 and rate<=5 ),
    constraint dm_delivery_tip_sum_check check ( tip_sum >= 0 ),
    constraint dm_delivery_delivery_ts_check check ( delivery_ts>='01.01.2022'::timestamp and delivery_ts<'01.01.2500'::timestamp ),
    constraint dm_delivery_delivery_order_id_unique unique (order_id)
);

drop table if exists dds.fct_order_delivery CASCADE;

create table dds.fct_order_delivery(
    id serial not null primary key,
    delivery_id int not null references dds.dm_deliveries(id),
    restaurant_id int not null references dds.dm_restaurants(id),
    courier_id int not null references dds.dm_couriers(id),
    order_id int not null references dds.dm_orders(id),
    order_timestamp_id int not null references dds.dm_timestamps(id),
    rate smallint not null check ( rate>=1 and rate<=5 ),
    order_sum numeric(14,2) not null check ( order_sum >= 0 ),
    tip_sum numeric(14,2) not null check ( tip_sum>=0 ),
    constraint fct_order_delivery_order_id_unique unique (order_id)
);
