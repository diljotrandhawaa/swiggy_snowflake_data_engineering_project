USE SCHEMA sandbox.consumption_sch;

-- Location Dimension:
CREATE OR REPLACE TABLE consumption_sch.restaurant_location_dim (
    restaurant_location_hk NUMBER PRIMARY KEY,
    location_id NUMBER NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    state_code VARCHAR(2) NOT NULL,
    is_union_territory BOOLEAN NOT NULL DEFAULT FALSE,
    capital_city_flag BOOLEAN NOT NULL DEFAULT FALSE,
    city_tier VARCHAR(6),
    zip_code VARCHAR(10) NOT NULL,
    active_flag VARCHAR(10) NOT NULL,
    eff_start_dt TIMESTAMP_TZ NOT NULL,
    eff_end_dt TIMESTAMP_TZ,
    current_flag BOOLEAN NOT NULL DEFAULT TRUE
);

MERGE INTO consumption_sch.restaurant_location_dim AS target
USING clean_sch.restaurant_location_stm AS source
ON target.location_id = source.location_id AND target.active_flag = source.active_flag
WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET
        target.eff_end_dt = CURRENT_TIMESTAMP(),
        target.current_flag = FALSE
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    INSERT (restaurant_location_hk, location_id, city, state, state_code, is_union_territory,
            capital_city_flag, city_tier, zip_code, active_flag, eff_start_dt, eff_end_dt, current_flag)
    VALUES (
        HASH(SHA1_HEX(CONCAT(source.city, source.state, source.state_code, source.zip_code))),
        source.location_id, source.city, source.state, source.state_code, source.is_union_territory,
        source.capital_city_flag, source.city_tier, source.zip_code, source.active_flag,
        CURRENT_TIMESTAMP(), NULL, TRUE
    )
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'FALSE' THEN
    INSERT (restaurant_location_hk, location_id, city, state, state_code, is_union_territory,
            capital_city_flag, city_tier, zip_code, active_flag, eff_start_dt, eff_end_dt, current_flag)
    VALUES (
        HASH(SHA1_HEX(CONCAT(source.city, source.state, source.state_code, source.zip_code))),
        source.location_id, source.city, source.state, source.state_code, source.is_union_territory,
        source.capital_city_flag, source.city_tier, source.zip_code, source.active_flag,
        CURRENT_TIMESTAMP(), NULL, TRUE
    );


-- Restaurant Dimenstion: 

CREATE OR REPLACE TABLE consumption_sch.restaurant_dim (
    restaurant_hk NUMBER PRIMARY KEY,
    restaurant_id NUMBER,
    name STRING(100),
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
    eff_start_date TIMESTAMP_TZ,
    eff_end_date TIMESTAMP_TZ,
    is_current BOOLEAN
);

-- Merge logic for SCD Type 2 tracking
MERGE INTO consumption_sch.restaurant_dim AS target
USING clean_sch.restaurant_stm AS source
ON target.restaurant_id = source.restaurant_id
WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET
        target.eff_end_date = CURRENT_TIMESTAMP(),
        target.is_current = FALSE
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    INSERT (restaurant_hk, restaurant_id, name, cuisine_type, pricing_for_two, restaurant_phone,
            operating_hours, location_id_fk, active_flag, open_status, locality, restaurant_address,
            latitude, longitude, eff_start_date, eff_end_date, is_current)
    VALUES (
        HASH(SHA1_HEX(CONCAT(source.restaurant_id, source.name, source.cuisine_type))),
        source.restaurant_id, source.name, source.cuisine_type, source.pricing_for_two,
        source.restaurant_phone, source.operating_hours, source.location_id_fk,
        source.active_flag, source.open_status, source.locality, source.restaurant_address,
        source.latitude, source.longitude,
        CURRENT_TIMESTAMP(), NULL, TRUE
    );

-- Customer Dimension:

-- Create SCD2 customer dimension table
CREATE OR REPLACE TABLE consumption_sch.customer_dim (
    customer_hk NUMBER PRIMARY KEY,
    customer_id STRING NOT NULL,
    name STRING(100) NOT NULL,
    mobile STRING(15) WITH TAG (common.pii_policy_tag = 'PII'),
    email STRING(100) WITH TAG (common.pii_policy_tag = 'EMAIL'),
    gender STRING(10) WITH TAG (common.pii_policy_tag = 'PII'),
    dob DATE WITH TAG (common.pii_policy_tag = 'PII'),
    eff_start_date TIMESTAMP_TZ,
    eff_end_date TIMESTAMP_TZ,
    is_current BOOLEAN
);

-- Merge logic for SCD Type 2
MERGE INTO consumption_sch.customer_dim AS target
USING clean_sch.customer_stm AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET target.eff_end_date = CURRENT_TIMESTAMP(), target.is_current = FALSE
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    INSERT (customer_hk, customer_id, name, mobile, email, gender, dob, eff_start_date, is_current)
    VALUES (
        HASH(SHA1_HEX(CONCAT(source.customer_id, source.name, source.email, source.mobile))),
        source.customer_id, source.name, source.mobile, source.email, source.gender, source.dob,
        CURRENT_TIMESTAMP(), TRUE
    );

-- Customer Address:

CREATE OR REPLACE TABLE CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM (
    CUSTOMER_ADDRESS_HK NUMBER PRIMARY KEY,
    ADDRESS_ID INT,
    CUSTOMER_ID_FK STRING,
    FLAT_NO STRING,
    HOUSE_NO STRING,
    FLOOR STRING,
    BUILDING STRING,
    LANDMARK STRING,
    LOCALITY STRING,
    CITY STRING,
    STATE STRING,
    PINCODE STRING,
    COORDINATES STRING,
    PRIMARY_FLAG STRING,
    ADDRESS_TYPE STRING,
    EFF_START_DATE TIMESTAMP_TZ,
    EFF_END_DATE TIMESTAMP_TZ,
    IS_CURRENT BOOLEAN
);

-- SCD2 Merge Logic
MERGE INTO CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM AS target
USING CLEAN_SCH.CUSTOMER_ADDRESS_STM AS source
ON target.ADDRESS_ID = source.ADDRESS_ID AND target.CUSTOMER_ID_FK = source.CUSTOMER_ID_FK
WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET target.EFF_END_DATE = CURRENT_TIMESTAMP(), target.IS_CURRENT = FALSE
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    INSERT (CUSTOMER_ADDRESS_HK, ADDRESS_ID, CUSTOMER_ID_FK, FLAT_NO, HOUSE_NO, FLOOR, BUILDING, LANDMARK, LOCALITY, CITY, STATE, PINCODE, COORDINATES, PRIMARY_FLAG, ADDRESS_TYPE, EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
    VALUES (
        hash(SHA1_hex(CONCAT(source.ADDRESS_ID, source.CUSTOMER_ID_FK, source.FLAT_NO, source.HOUSE_NO, source.PINCODE))),
        source.ADDRESS_ID, source.CUSTOMER_ID_FK, source.FLAT_NO, source.HOUSE_NO, source.FLOOR, source.BUILDING, source.LANDMARK, source.LOCALITY, source.CITY, source.STATE, source.PINCODE, source.COORDINATES, source.PRIMARY_FLAG, source.ADDRESS_TYPE,
        CURRENT_TIMESTAMP(), NULL, TRUE
    );

-- Menu Dimension:

CREATE OR REPLACE TABLE consumption_sch.menu_dim (
    Menu_Dim_HK NUMBER PRIMARY KEY,
    Menu_ID INT NOT NULL,
    Restaurant_ID_FK INT NOT NULL,
    Item_Name STRING,
    Price DECIMAL(10, 2),
    EFF_START_DATE TIMESTAMP_NTZ,
    EFF_END_DATE TIMESTAMP_NTZ,
    IS_CURRENT BOOLEAN
);

MERGE INTO consumption_sch.MENU_DIM AS target
USING CLEAN_SCH.MENU_STM AS source
ON target.Menu_ID = source.Menu_ID AND target.IS_CURRENT = TRUE
WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET target.EFF_END_DATE = CURRENT_TIMESTAMP(), target.IS_CURRENT = FALSE
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    INSERT (Menu_Dim_HK, Menu_ID, Restaurant_ID_FK, Item_Name, Price, EFF_START_DATE, IS_CURRENT)
    VALUES (hash(SHA1_hex(CONCAT(source.Menu_ID, source.Restaurant_ID_FK, source.Item_Name, source.Price))), 
            source.Menu_ID, source.Restaurant_ID_FK, source.Item_Name, source.Price, CURRENT_TIMESTAMP(), TRUE);


-- Delivery Agent:

CREATE OR REPLACE TABLE consumption_sch.delivery_agent_dim (
    delivery_agent_hk NUMBER PRIMARY KEY,
    delivery_agent_id NUMBER NOT NULL,
    name STRING NOT NULL,
    rating NUMBER(4,2),
    eff_start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    eff_end_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

MERGE INTO consumption_sch.delivery_agent_dim AS target
USING CLEAN_SCH.delivery_agent_stm AS source
ON target.delivery_agent_id = source.delivery_agent_id AND target.is_current = TRUE
WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET target.eff_end_date = CURRENT_TIMESTAMP, target.is_current = FALSE
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    INSERT (delivery_agent_hk, delivery_agent_id, name, rating, eff_start_date, is_current)
    VALUES (hash(SHA1_HEX(CONCAT(source.delivery_agent_id, source.name, source.rating))), 
            source.delivery_agent_id, source.name, source.rating, CURRENT_TIMESTAMP, TRUE);

-- Date:

CREATE OR REPLACE TABLE CONSUMPTION_SCH.DATE_DIM (
    DATE_DIM_HK NUMBER PRIMARY KEY,
    CALENDAR_DATE DATE UNIQUE,
    YEAR NUMBER, QUARTER NUMBER, MONTH NUMBER, DAY_NAME STRING
);

INSERT INTO CONSUMPTION_SCH.DATE_DIM 
WITH RECURSIVE my_date_dim_cte AS (
    SELECT CURRENT_DATE() AS today, YEAR(today) AS year, QUARTER(today) AS quarter, MONTH(today) AS month, DAYNAME(today) AS day_name
    UNION ALL
    SELECT DATEADD('day', -1, today), YEAR(DATEADD('day', -1, today)), QUARTER(DATEADD('day', -1, today)), MONTH(DATEADD('day', -1, today)), DAYNAME(DATEADD('day', -1, today))
    FROM my_date_dim_cte
    WHERE today > (SELECT DATE(MIN(order_date)) FROM clean_sch.orders)
)
SELECT hash(SHA1_hex(today)), today, YEAR, QUARTER, MONTH, DAY_NAME FROM my_date_dim_cte;


-- Order Item:

CREATE OR REPLACE TABLE consumption_sch.order_item_fact (
    order_item_fact_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    order_item_id NUMBER,
    order_id NUMBER,
    customer_dim_key NUMBER,
    restaurant_dim_key NUMBER,
    menu_dim_key NUMBER,
    order_date_dim_key NUMBER,
    quantity NUMBER,
    subtotal NUMBER(10, 2),
    delivery_status VARCHAR
);

MERGE INTO consumption_sch.order_item_fact AS target
USING (
    SELECT oi.Order_Item_ID, oi.Order_ID_fk, c.CUSTOMER_HK, r.RESTAURANT_HK, m.Menu_Dim_HK, dd.DATE_DIM_HK, oi.Quantity, oi.Subtotal, d.delivery_status
    FROM clean_sch.order_item_stm oi
    JOIN clean_sch.orders_stm o ON oi.Order_ID_fk = o.Order_ID
    JOIN clean_sch.delivery_stm d ON o.Order_ID = d.Order_ID_fk
    JOIN consumption_sch.CUSTOMER_DIM c ON o.Customer_ID_fk = c.customer_id
    JOIN consumption_sch.restaurant_dim r ON o.Restaurant_ID_fk = r.restaurant_id
    JOIN consumption_sch.menu_dim m ON oi.MENU_ID_fk = m.menu_id
    JOIN CONSUMPTION_SCH.DATE_DIM dd ON dd.calendar_date = DATE(o.order_date)
) AS source
ON target.order_item_id = source.Order_Item_ID
WHEN NOT MATCHED THEN INSERT (order_item_id, order_id, customer_dim_key, restaurant_dim_key, menu_dim_key, order_date_dim_key, quantity, subtotal, delivery_status)
VALUES (source.Order_Item_ID, source.Order_ID_fk, source.CUSTOMER_HK, source.RESTAURANT_HK, source.Menu_Dim_HK, source.DATE_DIM_HK, source.Quantity, source.Subtotal, source.delivery_status);


-- KPI Views:

CREATE OR REPLACE VIEW consumption_sch.vw_yearly_revenue_kpis AS
SELECT d.year, SUM(fact.subtotal) AS total_revenue, COUNT(DISTINCT fact.order_id) AS total_orders,
       ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2) AS avg_revenue_per_order
FROM consumption_sch.order_item_fact fact
JOIN consumption_sch.date_dim d ON fact.order_date_dim_key = d.date_dim_hk
WHERE DELIVERY_STATUS = 'Delivered'
GROUP BY d.year;