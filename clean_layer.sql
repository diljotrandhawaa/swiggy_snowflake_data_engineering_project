USE SCHEMA sandbox.clean_sch;

-- Location Dimension: 

CREATE OR REPLACE TABLE clean_sch.restaurant_location (
    restaurant_location_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    location_id NUMBER NOT NULL UNIQUE,
    city STRING(100) NOT NULL,
    state STRING(100) NOT NULL,
    state_code STRING(2) NOT NULL,
    is_union_territory BOOLEAN NOT NULL DEFAULT FALSE,
    capital_city_flag BOOLEAN NOT NULL DEFAULT FALSE,
    city_tier TEXT(6),
    zip_code STRING(10) NOT NULL,
    active_flag STRING(10) NOT NULL,
    created_ts TIMESTAMP_TZ NOT NULL,
    modified_ts TIMESTAMP_TZ,
    _stg_file_name STRING,
    _stg_file_load_ts TIMESTAMP_NTZ,
    _stg_file_md5 STRING,
    _copy_data_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Transform and Merge from Stage Stream
MERGE INTO clean_sch.restaurant_location AS target
USING (
    SELECT
        CAST(LocationID AS NUMBER) AS location_id,
        CAST(City AS STRING) AS city,
        CASE
            WHEN CAST(State AS STRING) = 'Delhi' THEN 'New Delhi'
            ELSE CAST(State AS STRING)
        END AS state,
        CASE
            WHEN State = 'Delhi' THEN 'DL'
            WHEN State = 'Maharashtra' THEN 'MH'
            WHEN State = 'Uttar Pradesh' THEN 'UP'
            WHEN State = 'Gujarat' THEN 'GJ'
            WHEN State = 'Rajasthan' THEN 'RJ'
            WHEN State = 'Kerala' THEN 'KL'
            WHEN State = 'Punjab' THEN 'PB'
            WHEN State = 'Karnataka' THEN 'KA'
            WHEN State = 'Madhya Pradesh' THEN 'MP'
            WHEN State = 'Odisha' THEN 'OR'
            WHEN State = 'Chandigarh' THEN 'CH'
            WHEN State = 'West Bengal' THEN 'WB'
            WHEN State = 'Sikkim' THEN 'SK'
            WHEN State = 'Andhra Pradesh' THEN 'AP'
            WHEN State = 'Assam' THEN 'AS'
            WHEN State = 'Jammu and Kashmir' THEN 'JK'
            WHEN State = 'Puducherry' THEN 'PY'
            WHEN State = 'Uttarakhand' THEN 'UK'
            WHEN State = 'Himachal Pradesh' THEN 'HP'
            WHEN State = 'Tamil Nadu' THEN 'TN'
            WHEN State = 'Goa' THEN 'GA'
            WHEN State = 'Telangana' THEN 'TG'
            WHEN State = 'Chhattisgarh' THEN 'CG'
            WHEN State = 'Jharkhand' THEN 'JH'
            WHEN State = 'Bihar' THEN 'BR'
            ELSE NULL
        END AS state_code,
        CASE
            WHEN State IN ('Delhi', 'Chandigarh', 'Puducherry', 'Jammu and Kashmir') THEN TRUE
            ELSE FALSE
        END AS is_union_territory,
        CASE
            WHEN (State = 'Delhi' AND City = 'New Delhi') THEN TRUE
            WHEN (State = 'Maharashtra' AND City = 'Mumbai') THEN TRUE
            ELSE FALSE
        END AS capital_city_flag,
        CASE
            WHEN City IN ('Mumbai', 'Delhi', 'Bengaluru', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune', 'Ahmedabad') THEN 'Tier-1'
            WHEN City IN ('Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Patna', 'Vadodara',
                          'Coimbatore', 'Ludhiana', 'Agra', 'Nashik', 'Ranchi', 'Meerut', 'Raipur', 'Guwahati', 'Chandigarh') THEN 'Tier-2'
            ELSE 'Tier-3'
        END AS city_tier,
        CAST(ZipCode AS STRING) AS zip_code,
        CAST(ActiveFlag AS STRING) AS active_flag,
        TO_TIMESTAMP_TZ(CreatedDate, 'YYYY-MM-DD HH24:MI:SS') AS created_ts,
        TO_TIMESTAMP_TZ(ModifiedDate, 'YYYY-MM-DD HH24:MI:SS') AS modified_ts,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5,
        CURRENT_TIMESTAMP AS _copy_data_ts
    FROM stage_sch.location_stm
) AS source
ON target.location_id = source.location_id
WHEN MATCHED AND (
    target.city != source.city OR
    target.state != source.state OR
    target.state_code != source.state_code OR
    target.is_union_territory != source.is_union_territory OR
    target.capital_city_flag != source.capital_city_flag OR
    target.city_tier != source.city_tier OR
    target.zip_code != source.zip_code OR
    target.active_flag != source.active_flag OR
    target.modified_ts != source.modified_ts
) THEN
    UPDATE SET
        target.city = source.city,
        target.state = source.state,
        target.state_code = source.state_code,
        target.is_union_territory = source.is_union_territory,
        target.capital_city_flag = source.capital_city_flag,
        target.city_tier = source.city_tier,
        target.zip_code = source.zip_code,
        target.active_flag = source.active_flag,
        target.modified_ts = source.modified_ts,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    INSERT (location_id, city, state, state_code, is_union_territory, capital_city_flag,
            city_tier, zip_code, active_flag, created_ts, modified_ts,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
    VALUES (source.location_id, source.city, source.state, source.state_code, source.is_union_territory,
            source.capital_city_flag, source.city_tier, source.zip_code, source.active_flag,
            source.created_ts, source.modified_ts,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._copy_data_ts);

-- Restaurant Dimension: 
-- Create clean table
CREATE OR REPLACE TABLE clean_sch.restaurant (
    restaurant_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    restaurant_id NUMBER UNIQUE,
    name STRING(100) NOT NULL,
    cuisine_type STRING,
    pricing_for_two NUMBER(10, 2),
    restaurant_phone STRING(15) WITH TAG (common.pii_policy_tag = 'SENSITIVE'),
    operating_hours STRING(100),
    location_id_fk NUMBER,
    active_flag STRING(10),
    open_status STRING(10),
    locality STRING(100),
    restaurant_address STRING,
    latitude NUMBER(9, 6),
    longitude NUMBER(9, 6),
    created_dt TIMESTAMP_TZ,
    modified_dt TIMESTAMP_TZ,
    _stg_file_name STRING,
    _stg_file_load_ts TIMESTAMP_NTZ,
    _stg_file_md5 STRING,
    _copy_data_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Stream to track changes in the clean layer for the dimension
CREATE OR REPLACE STREAM clean_sch.restaurant_stm ON TABLE clean_sch.restaurant;

-- Merge logic to transform and load data from stage stream
MERGE INTO clean_sch.restaurant AS target
USING (
    SELECT
        TRY_CAST(restaurantid AS NUMBER) AS restaurant_id,
        TRY_CAST(name AS STRING) AS name,
        TRY_CAST(cuisinetype AS STRING) AS cuisine_type,
        TRY_CAST(pricing_for_2 AS NUMBER(10, 2)) AS pricing_for_two,
        TRY_CAST(restaurant_phone AS STRING) AS restaurant_phone,
        TRY_CAST(operatinghours AS STRING) AS operating_hours,
        TRY_CAST(locationid AS NUMBER) AS location_id_fk,
        TRY_CAST(activeflag AS STRING) AS active_flag,
        TRY_CAST(openstatus AS STRING) AS open_status,
        TRY_CAST(locality AS STRING) AS locality,
        TRY_CAST(restaurant_address AS STRING) AS restaurant_address,
        TRY_CAST(latitude AS NUMBER(9, 6)) AS latitude,
        TRY_CAST(longitude AS NUMBER(9, 6)) AS longitude,
        TRY_TO_TIMESTAMP_NTZ(createddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS created_dt,
        TRY_TO_TIMESTAMP_NTZ(modifieddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS modified_dt,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5
    FROM stage_sch.restaurant_stm
) AS source
ON target.restaurant_id = source.restaurant_id
WHEN MATCHED THEN
    UPDATE SET
        target.name = source.name,
        target.cuisine_type = source.cuisine_type,
        target.pricing_for_two = source.pricing_for_two,
        target.restaurant_phone = source.restaurant_phone,
        target.operating_hours = source.operating_hours,
        target.location_id_fk = source.location_id_fk,
        target.active_flag = source.active_flag,
        target.open_status = source.open_status,
        target.locality = source.locality,
        target.restaurant_address = source.restaurant_address,
        target.latitude = source.latitude,
        target.longitude = source.longitude,
        target.modified_dt = source.modified_dt,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5
WHEN NOT MATCHED THEN
    INSERT (restaurant_id, name, cuisine_type, pricing_for_two, restaurant_phone, operating_hours,
            location_id_fk, active_flag, open_status, locality, restaurant_address,
            latitude, longitude, created_dt, modified_dt,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5)
    VALUES (source.restaurant_id, source.name, source.cuisine_type, source.pricing_for_two,
            source.restaurant_phone, source.operating_hours, source.location_id_fk,
            source.active_flag, source.open_status, source.locality, source.restaurant_address,
            source.latitude, source.longitude, source.created_dt, source.modified_dt,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);

select * from clean_sch.restaurant;

-- Customer Dimension: 
-- Create clean customer table
CREATE OR REPLACE TABLE clean_sch.customer (
    customer_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    customer_id STRING NOT NULL,
    name STRING(100) NOT NULL,
    mobile STRING(15) WITH TAG (common.pii_policy_tag = 'PII'),
    email STRING(100) WITH TAG (common.pii_policy_tag = 'EMAIL'),
    login_by_using STRING(50),
    gender STRING(10) WITH TAG (common.pii_policy_tag = 'PII'),
    dob DATE WITH TAG (common.pii_policy_tag = 'PII'),
    anniversary DATE,
    preferences STRING,
    created_dt TIMESTAMP_TZ,
    modified_dt TIMESTAMP_TZ,
    _stg_file_name STRING,
    _stg_file_load_ts TIMESTAMP_NTZ,
    _stg_file_md5 STRING,
    _copy_data_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Stream for tracking clean layer changes
CREATE OR REPLACE STREAM clean_sch.customer_stm ON TABLE clean_sch.customer;

-- Merge logic to transform and load from stage stream
MERGE INTO clean_sch.customer AS target
USING (
    SELECT 
        customerid::string AS customer_id,
        name::string AS name,
        mobile::string AS mobile,
        email::string AS email,
        loginbyusing::string AS login_by_using,
        gender::string AS gender,
        TRY_TO_DATE(dob, 'YYYY-MM-DD') AS dob,
        TRY_TO_DATE(anniversary, 'YYYY-MM-DD') AS anniversary,
        preferences::string AS preferences,
        TRY_TO_TIMESTAMP_TZ(createddate, 'YYYY-MM-DD HH24:MI:SS') AS created_dt,
        TRY_TO_TIMESTAMP_TZ(modifieddate, 'YYYY-MM-DD HH24:MI:SS') AS modified_dt,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5
    FROM stage_sch.customer_stm
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET target.name = source.name, target.email = source.email, target.modified_dt = source.modified_dt
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, mobile, email, login_by_using, gender, dob, anniversary, preferences, created_dt, _stg_file_name, _stg_file_load_ts, _stg_file_md5)
    VALUES (source.customer_id, source.name, source.mobile, source.email, source.login_by_using, source.gender, source.dob, source.anniversary, source.preferences, source.created_dt, source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);

-- Customer Address:

CREATE OR REPLACE TABLE CLEAN_SCH.CUSTOMER_ADDRESS (
    CUSTOMER_ADDRESS_SK NUMBER AUTOINCREMENT PRIMARY KEY,
    ADDRESS_ID INT,
    CUSTOMER_ID_FK INT,
    FLAT_NO STRING,
    HOUSE_NO STRING,
    FLOOR STRING,
    BUILDING STRING,
    LANDMARK STRING,
    locality STRING,
    CITY STRING,
    STATE STRING,
    PINCODE STRING,
    COORDINATES STRING,
    PRIMARY_FLAG STRING,
    ADDRESS_TYPE STRING,
    CREATED_DATE TIMESTAMP_TZ,
    MODIFIED_DATE TIMESTAMP_TZ,
    _STG_FILE_NAME STRING,
    _STG_FILE_LOAD_TS TIMESTAMP,
    _STG_FILE_MD5 STRING,
    _COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Stream for clean layer delta tracking
create or replace stream CLEAN_SCH.CUSTOMER_ADDRESS_STM on table CLEAN_SCH.CUSTOMER_ADDRESS;

-- Transform and Merge logic
MERGE INTO clean_sch.customer_address AS clean
USING (
    SELECT 
        CAST(addressid AS INT) AS address_id,
        CAST(customerid AS INT) AS customer_id_fk,
        flatno AS flat_no,
        houseno AS house_no,
        floor, building, landmark, locality, city, state, pincode, coordinates,
        primaryflag AS primary_flag,
        addresstype AS address_type,
        TRY_TO_TIMESTAMP_TZ(createddate, 'YYYY-MM-DD"T"HH24:MI:SS') AS created_date,
        TRY_TO_TIMESTAMP_TZ(modifieddate, 'YYYY-MM-DD"T"HH24:MI:SS') AS modified_date,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
    FROM stage_sch.customeraddress_stm
) AS stage
ON clean.address_id = stage.address_id
WHEN NOT MATCHED THEN
    INSERT (address_id, customer_id_fk, flat_no, house_no, floor, building, landmark, locality, city, state, pincode, coordinates, primary_flag, address_type, created_date, modified_date, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
    VALUES (stage.address_id, stage.customer_id_fk, stage.flat_no, stage.house_no, stage.floor, stage.building, stage.landmark, stage.locality, stage.city, stage.state, stage.pincode, stage.coordinates, stage.primary_flag, stage.address_type, stage.created_date, stage.modified_date, stage._stg_file_name, stage._stg_file_load_ts, stage._stg_file_md5, stage._copy_data_ts)
WHEN MATCHED THEN
    UPDATE SET clean.flat_no = stage.flat_no, clean.house_no = stage.house_no, clean.floor = stage.floor, clean.modified_date = stage.modified_date;

-- Menu Dimension: 

CREATE OR REPLACE TABLE clean_sch.menu (
    Menu_SK INT AUTOINCREMENT PRIMARY KEY,
    Menu_ID INT NOT NULL UNIQUE,
    Restaurant_ID_FK INT,
    Item_Name STRING NOT NULL,
    Description STRING NOT NULL,
    Price DECIMAL(10, 2) NOT NULL,
    Category STRING,
    Availability BOOLEAN,
    Item_Type STRING,
    Created_dt TIMESTAMP_NTZ,
    Modified_dt TIMESTAMP_NTZ,
    _STG_FILE_NAME STRING, _STG_FILE_LOAD_TS TIMESTAMP_NTZ, _STG_FILE_MD5 STRING, _COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Merge logic
MERGE INTO clean_sch.menu AS target
USING (
    SELECT TRY_CAST(menuid AS INT) AS Menu_ID, TRY_CAST(restaurantid AS INT) AS Restaurant_ID_FK, TRIM(itemname) AS Item_Name, TRIM(description) AS Description, TRY_CAST(price AS DECIMAL(10, 2)) AS Price, TRIM(category) AS Category, 
    CASE WHEN LOWER(availability) = 'true' THEN TRUE ELSE FALSE END AS Availability, TRIM(itemtype) AS Item_Type, TRY_CAST(createddate AS TIMESTAMP_NTZ) AS Created_dt, TRY_CAST(modifieddate AS TIMESTAMP_NTZ) AS Modified_dt,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
    FROM stage_sch.menu
) AS source
ON target.Menu_ID = source.Menu_ID
WHEN NOT MATCHED THEN
    INSERT (Menu_ID, Restaurant_ID_FK, Item_Name, Description, Price, Category, Availability, Item_Type, Created_dt, Modified_dt, _STG_FILE_NAME, _STG_FILE_LOAD_TS, _STG_FILE_MD5)
    VALUES (source.Menu_ID, source.Restaurant_ID_FK, source.Item_Name, source.Description, source.Price, source.Category, source.Availability, source.Item_Type, source.Created_dt, source.Modified_dt, source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);

-- Delivery Agent:
CREATE OR REPLACE TABLE clean_sch.delivery_agent (
    delivery_agent_sk INT AUTOINCREMENT PRIMARY KEY,
    delivery_agent_id INT NOT NULL UNIQUE,
    name STRING NOT NULL,
    phone STRING NOT NULL,
    vehicle_type STRING NOT NULL,
    location_id_fk INT,
    status STRING,
    gender STRING,
    rating number(4,2),
    created_dt TIMESTAMP_NTZ,
    modified_dt TIMESTAMP_NTZ,
    _stg_file_name STRING,
    _stg_file_load_ts TIMESTAMP,
    _stg_file_md5 STRING,
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

create or replace stream CLEAN_SCH.delivery_agent_stm on table CLEAN_SCH.delivery_agent;

MERGE INTO clean_sch.delivery_agent AS target
USING stage_sch.deliveryagent_stm AS source
ON target.delivery_agent_id = source.deliveryagentid
WHEN MATCHED THEN UPDATE SET target.phone = source.phone, target.rating = TRY_TO_DECIMAL(source.rating,4,2)
WHEN NOT MATCHED THEN INSERT (delivery_agent_id, name, phone, vehicle_type, location_id_fk, rating, _stg_file_name, _stg_file_load_ts, _stg_file_md5)
VALUES (TRY_TO_NUMBER(source.deliveryagentid), source.name, source.phone, source.vehicletype, TRY_TO_NUMBER(source.locationid), TRY_TO_NUMBER(source.rating), source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);

select * from delivery_agent;



CREATE OR REPLACE TABLE clean_sch.delivery (
    delivery_sk INT AUTOINCREMENT PRIMARY KEY,
    delivery_id INT NOT NULL,
    order_id_fk NUMBER NOT NULL,
    delivery_agent_id_fk NUMBER NOT NULL,
    delivery_status STRING,
    estimated_time STRING,
    customer_address_id_fk NUMBER NOT NULL,
    delivery_date TIMESTAMP,
    created_date TIMESTAMP,
    modified_date TIMESTAMP,
    _stg_file_name STRING, _stg_file_load_ts TIMESTAMP, _stg_file_md5 STRING, _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

create or replace stream CLEAN_SCH.delivery_stm on table CLEAN_SCH.delivery;

MERGE INTO clean_sch.delivery AS target
USING stage_sch.delivery_stm AS source
ON target.delivery_id = TO_NUMBER(source.deliveryid)
WHEN NOT MATCHED THEN INSERT (delivery_id, order_id_fk, delivery_agent_id_fk, delivery_status, customer_address_id_fk, _stg_file_name, _stg_file_load_ts, _stg_file_md5)
VALUES (TO_NUMBER(source.deliveryid), TO_NUMBER(source.orderid), TO_NUMBER(source.deliveryagentid), source.deliverystatus, TO_NUMBER(source.addressid), source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);


-- Orders Clean Table:
CREATE OR REPLACE TABLE CLEAN_SCH.ORDERS (
    ORDER_SK NUMBER AUTOINCREMENT PRIMARY KEY,
    ORDER_ID BIGINT UNIQUE,
    CUSTOMER_ID_FK BIGINT,
    RESTAURANT_ID_FK BIGINT,
    ORDER_DATE TIMESTAMP,
    TOTAL_AMOUNT DECIMAL(10, 2),
    STATUS STRING,
    PAYMENT_METHOD STRING,
    created_dt timestamp_tz,
    modified_dt timestamp_tz,
    _stg_file_name string, _stg_file_load_ts timestamp_ntz, _stg_file_md5 string, _copy_data_ts timestamp_ntz default current_timestamp
);

create or replace stream CLEAN_SCH.ORDERS_stm on table CLEAN_SCH.ORDERS;

MERGE INTO CLEAN_SCH.ORDERS AS target
USING STAGE_SCH.ORDERS_STM AS source
ON target.ORDER_ID = TRY_TO_NUMBER(source.ORDERID)
WHEN NOT MATCHED THEN INSERT (ORDER_ID, CUSTOMER_ID_FK, RESTAURANT_ID_FK, ORDER_DATE, TOTAL_AMOUNT, STATUS, CREATED_DT, _STG_FILE_NAME, _STG_FILE_LOAD_TS, _STG_FILE_MD5)
VALUES (TRY_TO_NUMBER(source.ORDERID), TRY_TO_NUMBER(source.CUSTOMERID), TRY_TO_NUMBER(source.RESTAURANTID), TRY_TO_TIMESTAMP(source.ORDERDATE), TRY_TO_DECIMAL(source.TOTALAMOUNT), source.STATUS, TRY_TO_TIMESTAMP_TZ(source.CREATEDDATE), source._STG_FILE_NAME, source._STG_FILE_LOAD_TS, source._STG_FILE_MD5);



--Order Item Clean Table:
CREATE OR REPLACE TABLE clean_sch.order_item (
    order_item_sk NUMBER AUTOINCREMENT primary key,
    order_item_id NUMBER NOT NULL UNIQUE,
    order_id_fk NUMBER NOT NULL,
    menu_id_fk NUMBER NOT NULL,
    quantity NUMBER(10, 2),
    price NUMBER(10, 2),
    subtotal NUMBER(10, 2),
    created_dt TIMESTAMP,
    modified_dt TIMESTAMP,
    _stg_file_name VARCHAR(255), _stg_file_load_ts TIMESTAMP, _stg_file_md5 VARCHAR(255), _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

create or replace stream CLEAN_SCH.order_item_stm on table CLEAN_SCH.order_item;

MERGE INTO clean_sch.order_item AS target
USING stage_sch.orderitem_stm AS source
ON target.order_item_id = source.orderitemid
WHEN NOT MATCHED THEN INSERT (order_item_id, order_id_fk, menu_id_fk, quantity, price, subtotal, _stg_file_name, _stg_file_load_ts, _stg_file_md5)
VALUES (source.orderitemid, source.orderid, source.menuid, source.quantity, source.price, source.subtotal, source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);