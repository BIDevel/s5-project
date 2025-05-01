-- schema stg
create schema if not exists stg;

drop table if exists stg.srv_wf_settings;

-- workflow states
CREATE TABLE stg.srv_wf_settings (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);

drop table if exists stg.bonussystem_users CASCADE;

-- bonus system on PG
create table stg.bonussystem_users
(
    id integer not null,
    order_user_id text not null
);
alter table stg.bonussystem_users
    add constraint bonussystem_users_id_uindex unique (id);

drop table if exists stg.bonussystem_ranks CASCADE;


create table stg.bonussystem_ranks
(
    id integer not null,
    name varchar(2048) not null,
    bonus_percent numeric(19, 5) not null,
    min_payment_threshold numeric(19, 5) not null
);
alter table stg.bonussystem_ranks
    add constraint bonussystem_ranks_id_uindex unique (id);

drop table if exists stg.bonussystem_events CASCADE;

create table stg.bonussystem_events
(
    id integer not null,
    event_ts timestamp not null,
    event_type varchar not null,
    event_value text not null
);
create index idx_bonussystem_events__event_ts
    on stg.bonussystem_events (event_ts);
alter table stg.bonussystem_events
    add constraint bonussystem_events_id_uindex unique (id);

-- order system on Mongo
drop table if exists stg.ordersystem_orders CASCADE;

create table stg.ordersystem_orders(
  id serial not null,
  object_id varchar not null,
  object_value text not null,
  update_ts timestamp without time zone not null
);
alter table stg.ordersystem_orders
    add constraint ordersystem_orders_object_id_uindex unique (object_id);

drop table if exists stg.ordersystem_restaurants CASCADE;

create table stg.ordersystem_restaurants(
  id serial not null,
  object_id varchar not null,
  object_value text not null,
  update_ts timestamp without time zone not null
);
alter table stg.ordersystem_restaurants
    add constraint ordersystem_restaurants_object_id_uindex unique (object_id);

drop table if exists stg.ordersystem_users CASCADE;

create table stg.ordersystem_users(
  id serial not null,
  object_id varchar not null,
  object_value text not null,
  update_ts timestamp without time zone not null
);
alter table stg.ordersystem_users
    add constraint ordersystem_users_object_id_uindex unique (object_id);


-- schema dds
create schema if not exists dds;

-- workflow states
drop table if exists dds.srv_wf_settings CASCADE;

CREATE TABLE dds.srv_wf_settings (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);

drop table if exists dds.dm_users CASCADE;

create table dds.dm_users (
    id serial not null primary key,
    user_id  varchar not null,
    user_name  varchar not null,
    user_login  varchar not null
);

drop table if exists dds.dm_restaurants CASCADE;

create table dds.dm_restaurants (
    id serial not null primary key ,
    restaurant_id  varchar not null,
    restaurant_name  varchar not null,
    active_from timestamp not null,
    active_to timestamp not null
);
-- Добавим уникальность, для поддержки SCD 2
alter table dds.dm_restaurants
    add constraint dm_restaurants_restaurant_id_active_to_uindex
        unique (restaurant_id, active_to);

drop table if exists dds.dm_products CASCADE;

create table dds.dm_products (
    id serial not null primary key,
    product_id  varchar not null,
    restaurant_id  int not null,
    product_name  varchar not null,
    product_price numeric(14,2) default(0) not null,
    active_from timestamp not null,
    active_to timestamp not null
);
alter table dds.dm_products
    add check ( product_price >= 0 );
-- Добавим уникальность, для поддержки SCD 2
alter table dds.dm_products
    add constraint dm_products_product_id_active_to_uindex
        unique (product_id, active_to);
alter table dds.dm_products
    add constraint dm_products_restaurants_fkey foreign key (restaurant_id) references dds.dm_restaurants(id);

drop table if exists dds.dm_timestamps CASCADE;

create table dds.dm_timestamps(
    id serial not null primary key,
    ts timestamp not null,
    year smallint not null check ( year>=2022 and year<2500 ),
    month smallint not null check ( month>=1 and month  <= 12 ),
    day smallint not null check ( day>=1 and day <= 31 ),
    time time not null,
    date date  not null,
    constraint dds_dm_timestamps_ts_unique
        unique (ts)
);

drop table if exists dds.dm_orders CASCADE;

create table dds.dm_orders(
    id serial not null primary key,
    user_id int not null references dds.dm_users(id),
    restaurant_id int not null references dds.dm_restaurants(id),
    timestamp_id int not null references dds.dm_timestamps(id),
    order_key varchar not null,
    order_status varchar not null
);

drop table if exists dds.fct_product_sales CASCADE;

create table dds.fct_product_sales(
    id serial not null primary key,
    product_id int not null references dds.dm_products(id),
    order_id int not null references dds.dm_orders(id),
    count int not null default(0) check ( count >= 0 ),
    price numeric(14,2) not null default(0) check ( price >= 0 ),
    total_sum numeric(14,2) not null default(0) check ( total_sum >= 0 ),
    bonus_payment numeric(14,2) not null default(0) check ( bonus_payment >= 0 ),
    bonus_grant numeric(14,2) not null default(0) check ( bonus_grant >= 0 )
);
-- Добавим уникальный индекс, для on conflict
alter table dds.fct_product_sales
    add constraint fct_product_sales_product_id_order_id_uindex
        unique (product_id, order_id);


-- schema cdm
create schema if not exists cdm;

-- workflow states
drop table if exists cdm.srv_wf_settings CASCADE;

CREATE TABLE cdm.srv_wf_settings(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);

drop table if exists cdm.dm_settlement_report CASCADE;

create table cdm.dm_settlement_report(
    id serial,
    restaurant_id varchar(25) not null,
    restaurant_name varchar not null,
    settlement_date date not null,
    orders_count int not null,
    orders_total_sum numeric(14,2) not null,
    orders_bonus_payment_sum numeric(14,2) not null,
    orders_bonus_granted_sum numeric(14,2) not null,
    order_processing_fee numeric(14,2) not null,
    restaurant_reward_sum numeric(14,2) not null
);
alter table cdm.dm_settlement_report
    add constraint pk_dm_settlement_report primary key (id);
alter table cdm.dm_settlement_report
    add constraint dm_settlement_report_settlement_date_check check ( settlement_date >= '01.01.2022' and settlement_date < '01.01.2500' );
alter table cdm.dm_settlement_report
    alter column orders_count set default 0;
alter table cdm.dm_settlement_report
    alter column orders_total_sum set default 0;
alter table cdm.dm_settlement_report
    alter column orders_bonus_payment_sum set default 0;
alter table cdm.dm_settlement_report
    alter column orders_bonus_granted_sum set default 0;
alter table cdm.dm_settlement_report
    alter column order_processing_fee set default 0;
alter table cdm.dm_settlement_report
    alter column restaurant_reward_sum set default 0;
alter table cdm.dm_settlement_report
    add constraint dm_settlement_report_orders_count_check check ( orders_count>=0 );
alter table cdm.dm_settlement_report
    add constraint dm_settlement_report_orders_total_sum_check check ( orders_total_sum >= 0 );
alter table cdm.dm_settlement_report
    add constraint dm_settlement_report_orders_bonus_payment_sum_check check ( orders_bonus_payment_sum >= 0 );
alter table cdm.dm_settlement_report
    add constraint dm_settlement_report_orders_bonus_granted_sum_check check ( orders_bonus_granted_sum >= 0 );
alter table cdm.dm_settlement_report
    add constraint dm_settlement_report_order_processing_fee_check check ( order_processing_fee >= 0 );
alter table cdm.dm_settlement_report
    add constraint dm_settlement_report_restaurant_reward_sum_check check ( restaurant_reward_sum >= 0 );
alter table cdm.dm_settlement_report
    add constraint dm_settlement_report_restaurant_name_settlement_date_unique unique (restaurant_id, settlement_date);
