drop table if exists xpoints.order_items;
drop table if exists xpoints.transactions;
drop table if exists xpoints.points_balances;
drop table if exists xpoints.shopping_cart_items;
drop table if exists xpoints.catalog_images;
drop table if exists xpoints.catalog_items;
drop table if exists xpoints.customers;
drop table if exists xpoints.image_urls;
drop table if exists xpoints.images;

drop schema if exists xpoints cascade;

drop role if exists rewards_ro;
drop role if exists rewards_rw;

create schema xpoints;

create table xpoints.customers
(
    id          uuid primary key default gen_random_uuid(),
    username    varchar(50),
    first_name  varchar(50),
    last_name   varchar(50),
    maiden_name varchar(50),
    gender      varchar(10),
    email       varchar(50),
    phone_num   varchar(20),
    age         int,
    address     varchar(100),
    city        varchar(50),
    state       varchar(25),
    state_code  varchar(20),
    postal_code varchar(10)
);

create index async on xpoints.customers (username);

create table xpoints.catalog_items
(
    id                  uuid primary key default gen_random_uuid(),
    name                varchar(50),
    description         varchar(200),
    category            varchar(20),
    usd_price           numeric(8,2),
    points_price        int,
    rating              real,
    sku                 varchar(20),
    weight              real,
    width               real,
    height              real,
    depth               real,
    thumbnail_id        uuid
);

create table xpoints.catalog_images
(
    item_id             uuid,
    image_id            uuid,
    primary key (item_id, image_id)
);

create index async on xpoints.catalog_images (item_id);

create table xpoints.shopping_cart_items
(
    customer_id     uuid,
    item_id         uuid,
    quantity        int,
    primary key (customer_id, item_id)
);

create index async on xpoints.shopping_cart_items (customer_id);

create table xpoints.points_balances
(
    customer_id     uuid primary key,
    points_balance  bigint not null
);

create table xpoints.transactions
(
    id                  uuid primary key default gen_random_uuid(),
    customer_id         uuid,
    tx_type             varchar(10),
    points              bigint,
    tx_dt               timestamp default now(),
    tx_description      varchar(50)
);

create index async on xpoints.transactions (customer_id);

create table xpoints.order_items
(
    tx_id               uuid,
    cat_item_id         uuid,
    unit_cnt            int,
    unit_points_price   int,
    primary key (tx_id, cat_item_id)
);

create index async on xpoints.order_items (tx_id);

create table xpoints.images
(
    id          uuid primary key default gen_random_uuid(),
    filename    varchar(100)
);

create table xpoints.image_urls
(
    image_id           uuid,
    region             varchar(20),
    presigned_url      varchar(2000),
    created            timestamp default now(),
    primary key (image_id, region)
);

create role rewards_ro with login;
grant usage on schema xpoints to rewards_ro;
grant select on all tables in schema xpoints to rewards_ro;

create role rewards_rw with login;
grant usage on schema xpoints to rewards_rw;
grant select, insert, update, delete on all tables in schema xpoints to rewards_rw;
