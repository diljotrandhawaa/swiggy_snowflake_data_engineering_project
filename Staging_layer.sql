use role sysadmin;
use schema sandbox.stage_sch;


-- Location Dimension: 

-- Create the location staging table
create table stage_sch.location (
    locationid text,
    city text,
    state text,
    zipcode text,
    activeflag text,
    createddate text,
    modifieddate text,
    -- Audit columns for traceability
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'Raw staging table for location data. All columns are text for flexible ingestion.';

-- Create an append-only stream for delta tracking
create or replace stream stage_sch.location_stm on table stage_sch.location
append_only = true
comment = 'Tracks new records added to the location staging table';


copy into stage_sch.location (
    locationid, city, state, zipcode, activeflag, createddate, modifieddate, 
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
from (
    select 
        t.$1::text as locationid,
        t.$2::text as city,
        t.$3::text as state,
        t.$4::text as zipcode,
        t.$5::text as activeflag,
        t.$6::text as createddate,
        t.$7::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/initial/location t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

-- Verify the load
select * from stage_sch.location;

-- Restaurant Dimension: 

-- Create staging table with all text values and PII tagging for the phone column
CREATE OR REPLACE TABLE stage_sch.restaurant (
    restaurantid text,
    name text,
    cuisinetype text,
    pricing_for_2 text,
    restaurant_phone text WITH TAG (common.pii_policy_tag = 'SENSITIVE'),
    operatinghours text,
    locationid text,
    activeflag text,
    openstatus text,
    locality text,
    restaurant_address text,
    latitude text,
    longitude text,
    createddate text,
    modifieddate text,
    -- Audit columns
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp DEFAULT CURRENT_TIMESTAMP
);

-- Create stream to capture delta changes
CREATE OR REPLACE STREAM stage_sch.restaurant_stm ON TABLE stage_sch.restaurant
APPEND_ONLY = true;

-- Load initial data from internal stage
COPY INTO stage_sch.restaurant (restaurantid, name, cuisinetype, pricing_for_2, restaurant_phone, 
    operatinghours, locationid, activeflag, openstatus, locality, restaurant_address, 
    latitude, longitude, createddate, modifieddate, _stg_file_name, _stg_file_load_ts, _stg_file_md5)
FROM (
    SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11, t.$12, t.$13, t.$14, t.$15,
    METADATA$FILENAME, METADATA$FILE_LAST_MODIFIED, METADATA$FILE_CONTENT_KEY
    FROM @stage_sch.csv_stg/initial/restaurant/restaurant-delhi+NCR.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'stage_sch.csv_file_format')
ON_ERROR = abort_statement;

select * from stage_sch.restaurant


-- Customer Dimension: 
-- Create customer staging table with PII tags
CREATE OR REPLACE TABLE stage_sch.customer (
    customerid text,
    name text,
    mobile text WITH TAG (common.pii_policy_tag = 'PII'),
    email text WITH TAG (common.pii_policy_tag = 'EMAIL'),
    loginbyusing text,
    gender text WITH TAG (common.pii_policy_tag = 'PII'),
    dob text WITH TAG (common.pii_policy_tag = 'PII'),
    anniversary text,
    preferences text,
    createddate text,
    modifieddate text,
    -- Audit columns
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp DEFAULT CURRENT_TIMESTAMP
);

-- Stream for delta tracking
CREATE OR REPLACE STREAM stage_sch.customer_stm ON TABLE stage_sch.customer
APPEND_ONLY = true;

-- Load Initial Customer Data
COPY INTO stage_sch.customer (customerid, name, mobile, email, loginbyusing, gender, dob, anniversary, preferences, createddate, modifieddate, _stg_file_name, _stg_file_load_ts, _stg_file_md5)
FROM (
    SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11,
    METADATA$FILENAME, METADATA$FILE_LAST_MODIFIED, METADATA$FILE_CONTENT_KEY
    FROM @stage_sch.csv_stg/initial/customer/customers-initial.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'stage_sch.csv_file_format')
ON_ERROR = abort_statement;

-- Customer Address:

-- Create the raw staging table
create or replace table stage_sch.customeraddress (
    addressid text, 
    customerid text, 
    flatno text, 
    houseno text, 
    floor text, 
    building text, 
    landmark text, 
    locality text, 
    city text, 
    state text, 
    pincode text, 
    coordinates text, 
    primaryflag text, 
    addresstype text, 
    createddate text, 
    modifieddate text,
    -- Audit columns
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
);

-- Create stream for delta tracking
create or replace stream stage_sch.customeraddress_stm on table stage_sch.customeraddress
append_only = true;

-- Load Initial Data
copy into stage_sch.customeraddress (addressid, customerid, flatno, houseno, floor, building, 
    landmark, locality, city, pincode, state, coordinates, primaryflag, addresstype, 
    createddate, modifieddate, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$11, t.$10, t.$12, t.$13, t.$14, t.$15, t.$16,
    metadata$filename, metadata$file_last_modified, metadata$file_content_key, current_timestamp
    from @stage_sch.csv_stg/initial/customer-address t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

-- Menu Dimension:

-- Create menu staging table
CREATE OR REPLACE TABLE stage_sch.menu (
    menuid text, 
    restaurantid text, 
    itemname text, 
    description text, 
    price text, 
    category text, 
    availability text, 
    itemtype text, 
    createddate text, 
    modifieddate text,
    _stg_file_name text, _stg_file_load_ts timestamp, _stg_file_md5 text, _copy_data_ts timestamp DEFAULT current_timestamp
);

-- Stream for delta tracking
CREATE OR REPLACE STREAM stage_sch.menu_stm ON TABLE stage_sch.menu APPEND_ONLY = true;

-- Load Initial Menu Data
COPY INTO stage_sch.menu (menuid, restaurantid, itemname, description, price, category, availability, itemtype, createddate, modifieddate, _stg_file_name, _stg_file_load_ts, _stg_file_md5)
FROM (
    SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, 
    metadata$filename, metadata$file_last_modified, metadata$file_content_key
    FROM @stage_sch.csv_stg/initial/menu t
)
FILE_FORMAT = (FORMAT_NAME = 'stage_sch.csv_file_format');

-- Delivery Agent:
create or replace table stage_sch.deliveryagent (
    deliveryagentid text, name text, phone text, vehicletype text, locationid text,
    status text, gender text, rating text, createddate text, modifieddate text,
    _stg_file_name text, _stg_file_load_ts timestamp, _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
);

create or replace stream stage_sch.deliveryagent_stm on table stage_sch.deliveryagent
append_only = true;

copy into stage_sch.deliveryagent (deliveryagentid, name, phone, vehicletype, locationid, status, gender, rating, createddate, modifieddate, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select t.$1::text, t.$2::text, t.$3::text, t.$4::text, t.$5::text, t.$6::text, t.$7::text, t.$8::text, t.$9::text, t.$10::text,
    metadata$filename, metadata$file_last_modified, metadata$file_content_key, current_timestamp
    from @stage_sch.csv_stg/initial/delivery-agent t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

-- Delivery Transaction Staging:
create or replace table stage_sch.delivery (
    deliveryid text, orderid text, deliveryagentid text, deliverystatus text,
    estimatedtime text, addressid text, deliverydate text, createddate text,
    modifieddate text, _stg_file_name text, _stg_file_load_ts timestamp,
    _stg_file_md5 text, _copy_data_ts timestamp default current_timestamp
);

create or replace stream stage_sch.delivery_stm on table stage_sch.delivery
append_only = true;

copy into stage_sch.delivery (deliveryid, orderid, deliveryagentid, deliverystatus, estimatedtime, addressid, deliverydate, createddate, modifieddate, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select t.$1::text, t.$2::text, t.$3::text, t.$4::text, t.$5::text, t.$6::text, t.$7::text, t.$8::text, t.$9::text,
    metadata$filename, metadata$file_last_modified, metadata$file_content_key, current_timestamp
    from @stage_sch.csv_stg/initial/delivery/delivery-initial-load.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

-- Orders Dimension: 

create or replace table stage_sch.orders (
    orderid text, customerid text, restaurantid text, orderdate text,
    totalamount text, status text, paymentmethod text, createddate text,
    modifieddate text, _stg_file_name text, _stg_file_load_ts timestamp,
    _stg_file_md5 text, _copy_data_ts timestamp default current_timestamp
);

create or replace stream stage_sch.orders_stm on table stage_sch.orders
append_only = true;

copy into stage_sch.orders (orderid, customerid, restaurantid, orderdate, totalamount, status, paymentmethod, createddate, modifieddate, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select t.$1::text, t.$2::text, t.$3::text, t.$4::text, t.$5::text, t.$6::text, t.$7::text, t.$8::text, t.$9::text,
    metadata$filename, metadata$file_last_modified, metadata$file_content_key, current_timestamp
    from @stage_sch.csv_stg/initial/orders/orders-initial.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

-- Order Item:
create or replace table stage_sch.orderitem (
    orderitemid text, orderid text, menuid text, quantity text, price text,
    subtotal text, createddate text, modifieddate text,
    _stg_file_name text, _stg_file_load_ts timestamp, _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
);

create or replace stream stage_sch.orderitem_stm on table stage_sch.orderitem
append_only = true;

copy into stage_sch.orderitem (orderitemid, orderid, menuid, quantity, price, subtotal, createddate, modifieddate, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select t.$1::text, t.$2::text, t.$3::text, t.$4::text, t.$5::text, t.$6::text, t.$7::text, t.$8::text,
    metadata$filename, metadata$file_last_modified, metadata$file_content_key, current_timestamp
    from @stage_sch.csv_stg/initial/order-items/ t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;