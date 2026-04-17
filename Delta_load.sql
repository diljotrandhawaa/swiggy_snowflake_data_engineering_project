use role sysadmin;
use database sandbox;
use warehouse adhoc_wh;

-- ============================================================
-- DELTA LOAD: LOCATION
-- ============================================================
use schema stage_sch;

COPY INTO stage_sch.location (
    locationid, city, state, zipcode, activeflag,
    createddate, modifieddate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
FROM (
    SELECT
        t.$1::text, t.$2::text, t.$3::text, t.$4::text, t.$5::text,
        t.$6::text, t.$7::text,
        metadata$filename, metadata$file_last_modified,
        metadata$file_content_key, current_timestamp
    FROM @stage_sch.csv_stg/delta/location/ t
)
FILE_FORMAT = (FORMAT_NAME = 'stage_sch.csv_file_format')
ON_ERROR = abort_statement;

-- Verify stage stream has picked up changes
SELECT * FROM stage_sch.location_stm;

-- Clean layer merge (re-run)
use schema clean_sch;

MERGE INTO clean_sch.restaurant_location AS target
USING (
    SELECT
        CAST(LocationID AS NUMBER) AS location_id,
        CAST(City AS STRING) AS city,
        CASE WHEN CAST(State AS STRING) = 'Delhi' THEN 'New Delhi' ELSE CAST(State AS STRING) END AS state,
        CASE
            WHEN State = 'Delhi' THEN 'DL' WHEN State = 'Maharashtra' THEN 'MH'
            WHEN State = 'Uttar Pradesh' THEN 'UP' WHEN State = 'Gujarat' THEN 'GJ'
            WHEN State = 'Rajasthan' THEN 'RJ' WHEN State = 'Kerala' THEN 'KL'
            WHEN State = 'Punjab' THEN 'PB' WHEN State = 'Karnataka' THEN 'KA'
            WHEN State = 'Madhya Pradesh' THEN 'MP' WHEN State = 'Odisha' THEN 'OR'
            WHEN State = 'Chandigarh' THEN 'CH' WHEN State = 'West Bengal' THEN 'WB'
            WHEN State = 'Sikkim' THEN 'SK' WHEN State = 'Andhra Pradesh' THEN 'AP'
            WHEN State = 'Assam' THEN 'AS' WHEN State = 'Jammu and Kashmir' THEN 'JK'
            WHEN State = 'Puducherry' THEN 'PY' WHEN State = 'Uttarakhand' THEN 'UK'
            WHEN State = 'Himachal Pradesh' THEN 'HP' WHEN State = 'Tamil Nadu' THEN 'TN'
            WHEN State = 'Goa' THEN 'GA' WHEN State = 'Telangana' THEN 'TG'
            WHEN State = 'Chhattisgarh' THEN 'CG' WHEN State = 'Jharkhand' THEN 'JH'
            WHEN State = 'Bihar' THEN 'BR' ELSE NULL
        END AS state_code,
        CASE WHEN State IN ('Delhi','Chandigarh','Puducherry','Jammu and Kashmir') THEN TRUE ELSE FALSE END AS is_union_territory,
        CASE WHEN (State = 'Delhi' AND City = 'New Delhi') THEN TRUE WHEN (State = 'Maharashtra' AND City = 'Mumbai') THEN TRUE ELSE FALSE END AS capital_city_flag,
        CASE
            WHEN City IN ('Mumbai','Delhi','Bengaluru','Hyderabad','Chennai','Kolkata','Pune','Ahmedabad') THEN 'Tier-1'
            WHEN City IN ('Jaipur','Lucknow','Kanpur','Nagpur','Indore','Bhopal','Patna','Vadodara',
                          'Coimbatore','Ludhiana','Agra','Nashik','Ranchi','Meerut','Raipur','Guwahati','Chandigarh') THEN 'Tier-2'
            ELSE 'Tier-3'
        END AS city_tier,
        CAST(ZipCode AS STRING) AS zip_code,
        CAST(ActiveFlag AS STRING) AS active_flag,
        TO_TIMESTAMP_TZ(CreatedDate, 'YYYY-MM-DD HH24:MI:SS') AS created_ts,
        TO_TIMESTAMP_TZ(ModifiedDate, 'YYYY-MM-DD HH24:MI:SS') AS modified_ts,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5, CURRENT_TIMESTAMP AS _copy_data_ts
    FROM stage_sch.location_stm
) AS source
ON target.location_id = source.location_id
WHEN MATCHED AND (
    target.city != source.city OR target.state != source.state OR
    target.state_code != source.state_code OR target.active_flag != source.active_flag OR
    target.modified_ts != source.modified_ts
) THEN
    UPDATE SET
        target.city = source.city, target.state = source.state,
        target.state_code = source.state_code, target.is_union_territory = source.is_union_territory,
        target.capital_city_flag = source.capital_city_flag, target.city_tier = source.city_tier,
        target.zip_code = source.zip_code, target.active_flag = source.active_flag,
        target.modified_ts = source.modified_ts, target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts, target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    INSERT (location_id, city, state, state_code, is_union_territory, capital_city_flag,
            city_tier, zip_code, active_flag, created_ts, modified_ts,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
    VALUES (source.location_id, source.city, source.state, source.state_code, source.is_union_territory,
            source.capital_city_flag, source.city_tier, source.zip_code, source.active_flag,
            source.created_ts, source.modified_ts,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._copy_data_ts);

-- Verify clean layer
SELECT * FROM clean_sch.restaurant_location_stm;

-- Consumption layer merge (re-run)
use schema consumption_sch;

MERGE INTO consumption_sch.restaurant_location_dim AS target
USING clean_sch.restaurant_location_stm AS source
ON target.location_id = source.location_id AND target.active_flag = source.active_flag
WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET target.eff_end_dt = CURRENT_TIMESTAMP(), target.current_flag = FALSE
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    INSERT (restaurant_location_hk, location_id, city, state, state_code, is_union_territory,
            capital_city_flag, city_tier, zip_code, active_flag, eff_start_dt, eff_end_dt, current_flag)
    VALUES (HASH(SHA1_HEX(CONCAT(source.city, source.state, source.state_code, source.zip_code))),
            source.location_id, source.city, source.state, source.state_code, source.is_union_territory,
            source.capital_city_flag, source.city_tier, source.zip_code, source.active_flag,
            CURRENT_TIMESTAMP(), NULL, TRUE)
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'FALSE' THEN
    INSERT (restaurant_location_hk, location_id, city, state, state_code, is_union_territory,
            capital_city_flag, city_tier, zip_code, active_flag, eff_start_dt, eff_end_dt, current_flag)
    VALUES (HASH(SHA1_HEX(CONCAT(source.city, source.state, source.state_code, source.zip_code))),
            source.location_id, source.city, source.state, source.state_code, source.is_union_territory,
            source.capital_city_flag, source.city_tier, source.zip_code, source.active_flag,
            CURRENT_TIMESTAMP(), NULL, TRUE);

SELECT * FROM consumption_sch.restaurant_location_dim ORDER BY location_id, eff_start_dt;


-- ============================================================
-- DELTA LOAD: RESTAURANT
-- ============================================================
use schema stage_sch;

COPY INTO stage_sch.restaurant (
    restaurantid, name, cuisinetype, pricing_for_2, restaurant_phone,
    operatinghours, locationid, activeflag, openstatus, locality,
    restaurant_address, latitude, longitude, createddate, modifieddate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
FROM (
    SELECT
        t.$1::text, t.$2::text, t.$3::text, t.$4::text, t.$5::text,
        t.$6::text, t.$7::text, t.$8::text, t.$9::text, t.$10::text,
        t.$11::text, t.$12::text, t.$13::text, t.$14::text, t.$15::text,
        metadata$filename, metadata$file_last_modified,
        metadata$file_content_key, current_timestamp
    FROM @stage_sch.csv_stg/delta/restaurant/ t
)
FILE_FORMAT = (FORMAT_NAME = 'stage_sch.csv_file_format')
ON_ERROR = abort_statement;

SELECT * FROM stage_sch.restaurant_stm;

-- Clean layer merge (re-run)
use schema clean_sch;

MERGE INTO clean_sch.restaurant AS target
USING (
    SELECT
        TRY_CAST(restaurantid AS NUMBER) AS restaurant_id,
        TRY_CAST(name AS STRING) AS name,
        TRY_CAST(cuisinetype AS STRING) AS cuisine_type,
        TRY_CAST(pricing_for_2 AS NUMBER(10,2)) AS pricing_for_two,
        TRY_CAST(restaurant_phone AS STRING) AS restaurant_phone,
        TRY_CAST(operatinghours AS STRING) AS operating_hours,
        TRY_CAST(locationid AS NUMBER) AS location_id_fk,
        TRY_CAST(activeflag AS STRING) AS active_flag,
        TRY_CAST(openstatus AS STRING) AS open_status,
        TRY_CAST(locality AS STRING) AS locality,
        TRY_CAST(restaurant_address AS STRING) AS restaurant_address,
        TRY_CAST(latitude AS NUMBER(9,6)) AS latitude,
        TRY_CAST(longitude AS NUMBER(9,6)) AS longitude,
        TRY_TO_TIMESTAMP_NTZ(createddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS created_dt,
        TRY_TO_TIMESTAMP_NTZ(modifieddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS modified_dt,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5
    FROM stage_sch.restaurant_stm
) AS source
ON target.restaurant_id = source.restaurant_id
WHEN MATCHED THEN
    UPDATE SET
        target.name = source.name, target.cuisine_type = source.cuisine_type,
        target.pricing_for_two = source.pricing_for_two, target.restaurant_phone = source.restaurant_phone,
        target.operating_hours = source.operating_hours, target.location_id_fk = source.location_id_fk,
        target.active_flag = source.active_flag, target.open_status = source.open_status,
        target.locality = source.locality, target.restaurant_address = source.restaurant_address,
        target.latitude = source.latitude, target.longitude = source.longitude,
        target.modified_dt = source.modified_dt, target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts, target._stg_file_md5 = source._stg_file_md5
WHEN NOT MATCHED THEN
    INSERT (restaurant_id, name, cuisine_type, pricing_for_two, restaurant_phone, operating_hours,
            location_id_fk, active_flag, open_status, locality, restaurant_address,
            latitude, longitude, created_dt, modified_dt, _stg_file_name, _stg_file_load_ts, _stg_file_md5)
    VALUES (source.restaurant_id, source.name, source.cuisine_type, source.pricing_for_two,
            source.restaurant_phone, source.operating_hours, source.location_id_fk,
            source.active_flag, source.open_status, source.locality, source.restaurant_address,
            source.latitude, source.longitude, source.created_dt, source.modified_dt,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);

SELECT * FROM clean_sch.restaurant_stm;

-- Consumption layer merge (re-run)
use schema consumption_sch;

MERGE INTO consumption_sch.restaurant_dim AS target
USING clean_sch.restaurant_stm AS source
ON target.restaurant_id = source.restaurant_id
WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET target.eff_end_date = CURRENT_TIMESTAMP(), target.is_current = FALSE
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    INSERT (restaurant_hk, restaurant_id, name, cuisine_type, pricing_for_two, restaurant_phone,
            operating_hours, location_id_fk, active_flag, open_status, locality, restaurant_address,
            latitude, longitude, eff_start_date, eff_end_date, is_current)
    VALUES (HASH(SHA1_HEX(CONCAT(source.restaurant_id, source.name, source.cuisine_type))),
            source.restaurant_id, source.name, source.cuisine_type, source.pricing_for_two,
            source.restaurant_phone, source.operating_hours, source.location_id_fk,
            source.active_flag, source.open_status, source.locality, source.restaurant_address,
            source.latitude, source.longitude, CURRENT_TIMESTAMP(), NULL, TRUE);

SELECT * FROM consumption_sch.restaurant_dim ORDER BY restaurant_id, eff_start_date;


-- ============================================================
-- DELTA LOAD: CUSTOMER
-- ============================================================
use schema stage_sch;

COPY INTO stage_sch.customer (
    customerid, name, mobile, email, loginbyusing, gender, dob,
    anniversary, preferences, createddate, modifieddate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
FROM (
    SELECT
        t.$1::text, t.$2::text, t.$3::text, t.$4::text, t.$5::text,
        t.$6::text, t.$7::text, t.$8::text, t.$9::text, t.$10::text, t.$11::text,
        metadata$filename, metadata$file_last_modified,
        metadata$file_content_key, current_timestamp
    FROM @stage_sch.csv_stg/delta/customer/ t
)
FILE_FORMAT = (FORMAT_NAME = 'stage_sch.csv_file_format')
ON_ERROR = abort_statement;

SELECT * FROM stage_sch.customer_stm;

-- Clean layer merge (re-run)
use schema clean_sch;

MERGE INTO clean_sch.customer AS target
USING (
    SELECT
        customerid::string AS customer_id, name::string AS name,
        mobile::string AS mobile, email::string AS email,
        loginbyusing::string AS login_by_using, gender::string AS gender,
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
    UPDATE SET
        target.name = source.name, target.email = source.email,
        target.mobile = source.mobile, target.gender = source.gender,
        target.dob = source.dob, target.modified_dt = source.modified_dt,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, mobile, email, login_by_using, gender, dob,
            anniversary, preferences, created_dt, _stg_file_name, _stg_file_load_ts, _stg_file_md5)
    VALUES (source.customer_id, source.name, source.mobile, source.email, source.login_by_using,
            source.gender, source.dob, source.anniversary, source.preferences, source.created_dt,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);

SELECT * FROM clean_sch.customer_stm;

-- Consumption layer merge (re-run)
use schema consumption_sch;

MERGE INTO consumption_sch.customer_dim AS target
USING clean_sch.customer_stm AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET target.eff_end_date = CURRENT_TIMESTAMP(), target.is_current = FALSE
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    INSERT (customer_hk, customer_id, name, mobile, email, gender, dob, eff_start_date, is_current)
    VALUES (HASH(SHA1_HEX(CONCAT(source.customer_id, source.name, source.email, source.mobile))),
            source.customer_id, source.name, source.mobile, source.email,
            source.gender, source.dob, CURRENT_TIMESTAMP(), TRUE);

SELECT * FROM consumption_sch.customer_dim ORDER BY customer_id, eff_start_date;


-- ============================================================
-- DELTA LOAD: CUSTOMER ADDRESS
-- ============================================================
use schema stage_sch;

COPY INTO stage_sch.customeraddress (
    addressid, customerid, flatno, houseno, floor, building,
    landmark, locality, city, pincode, state, coordinates,
    primaryflag, addresstype, createddate, modifieddate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
FROM (
    SELECT
        t.$1::text, t.$2::text, t.$3::text, t.$4::text, t.$5::text, t.$6::text,
        t.$7::text, t.$8::text, t.$9::text, t.$10::text, t.$11::text, t.$12::text,
        t.$13::text, t.$14::text, t.$15::text, t.$16::text,
        metadata$filename, metadata$file_last_modified,
        metadata$file_content_key, current_timestamp
    FROM @stage_sch.csv_stg/delta/customer-address/ t
)
FILE_FORMAT = (FORMAT_NAME = 'stage_sch.csv_file_format')
ON_ERROR = abort_statement;

SELECT * FROM stage_sch.customeraddress_stm;

-- Clean layer merge (re-run)
use schema clean_sch;

MERGE INTO clean_sch.customer_address AS clean
USING (
    SELECT
        CAST(addressid AS INT) AS address_id, CAST(customerid AS INT) AS customer_id_fk,
        flatno AS flat_no, houseno AS house_no, floor, building, landmark, locality,
        city, state, pincode, coordinates,
        primaryflag AS primary_flag, addresstype AS address_type,
        TRY_TO_TIMESTAMP_TZ(createddate, 'YYYY-MM-DD"T"HH24:MI:SS') AS created_date,
        TRY_TO_TIMESTAMP_TZ(modifieddate, 'YYYY-MM-DD"T"HH24:MI:SS') AS modified_date,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
    FROM stage_sch.customeraddress_stm
) AS stage
ON clean.address_id = stage.address_id
WHEN MATCHED THEN
    UPDATE SET
        clean.flat_no = stage.flat_no, clean.house_no = stage.house_no,
        clean.floor = stage.floor, clean.building = stage.building,
        clean.modified_date = stage.modified_date,
        clean._stg_file_name = stage._stg_file_name,
        clean._stg_file_load_ts = stage._stg_file_load_ts,
        clean._stg_file_md5 = stage._stg_file_md5
WHEN NOT MATCHED THEN
    INSERT (address_id, customer_id_fk, flat_no, house_no, floor, building, landmark,
            locality, city, state, pincode, coordinates, primary_flag, address_type,
            created_date, modified_date, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
    VALUES (stage.address_id, stage.customer_id_fk, stage.flat_no, stage.house_no, stage.floor,
            stage.building, stage.landmark, stage.locality, stage.city, stage.state, stage.pincode,
            stage.coordinates, stage.primary_flag, stage.address_type, stage.created_date,
            stage.modified_date, stage._stg_file_name, stage._stg_file_load_ts,
            stage._stg_file_md5, stage._copy_data_ts);

SELECT * FROM clean_sch.customer_address_stm;

-- Consumption layer merge (re-run)
use schema consumption_sch;

MERGE INTO consumption_sch.customer_address_dim AS target
USING clean_sch.customer_address_stm AS source
ON target.address_id = source.address_id AND target.customer_id_fk = source.customer_id_fk
WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET target.eff_end_date = CURRENT_TIMESTAMP(), target.is_current = FALSE
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    INSERT (customer_address_hk, address_id, customer_id_fk, flat_no, house_no, floor, building,
            landmark, locality, city, state, pincode, coordinates, primary_flag, address_type,
            eff_start_date, eff_end_date, is_current)
    VALUES (HASH(SHA1_HEX(CONCAT(source.address_id, source.customer_id_fk, source.flat_no, source.house_no, source.pincode))),
            source.address_id, source.customer_id_fk, source.flat_no, source.house_no, source.floor,
            source.building, source.landmark, source.locality, source.city, source.state, source.pincode,
            source.coordinates, source.primary_flag, source.address_type,
            CURRENT_TIMESTAMP(), NULL, TRUE);

SELECT * FROM consumption_sch.customer_address_dim ORDER BY address_id, eff_start_date;


-- ============================================================
-- DELTA LOAD: MENU
-- ============================================================
use schema stage_sch;

COPY INTO stage_sch.menu (
    menuid, restaurantid, itemname, description, price, category,
    availability, itemtype, createddate, modifieddate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
FROM (
    SELECT
        t.$1::text, t.$2::text, t.$3::text, t.$4::text, t.$5::text,
        t.$6::text, t.$7::text, t.$8::text, t.$9::text, t.$10::text,
        metadata$filename, metadata$file_last_modified,
        metadata$file_content_key, current_timestamp
    FROM @stage_sch.csv_stg/delta/menu/ t
)
FILE_FORMAT = (FORMAT_NAME = 'stage_sch.csv_file_format')
ON_ERROR = abort_statement;

SELECT * FROM stage_sch.menu_stm;

-- Clean layer merge (re-run)
use schema clean_sch;

MERGE INTO clean_sch.menu AS target
USING (
    SELECT
        TRY_CAST(menuid AS INT) AS menu_id,
        TRY_CAST(restaurantid AS INT) AS restaurant_id_fk,
        TRIM(itemname) AS item_name,
        TRIM(description) AS description,
        TRY_CAST(price AS DECIMAL(10,2)) AS price,
        TRIM(category) AS category,
        CASE WHEN LOWER(availability) = 'true' THEN TRUE ELSE FALSE END AS availability,
        TRIM(itemtype) AS item_type,
        TRY_CAST(createddate AS TIMESTAMP_NTZ) AS created_dt,
        TRY_CAST(modifieddate AS TIMESTAMP_NTZ) AS modified_dt,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
    FROM stage_sch.menu_stm
) AS source
ON target.menu_id = source.menu_id
WHEN MATCHED THEN
    UPDATE SET
        target.item_name = source.item_name, target.price = source.price,
        target.category = source.category, target.availability = source.availability,
        target.item_type = source.item_type, target.modified_dt = source.modified_dt,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5
WHEN NOT MATCHED THEN
    INSERT (menu_id, restaurant_id_fk, item_name, description, price, category,
            availability, item_type, created_dt, modified_dt,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5)
    VALUES (source.menu_id, source.restaurant_id_fk, source.item_name, source.description,
            source.price, source.category, source.availability, source.item_type,
            source.created_dt, source.modified_dt,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);

SELECT * FROM clean_sch.menu_stm;

-- Consumption layer merge (re-run)
use schema consumption_sch;

MERGE INTO consumption_sch.menu_dim AS target
USING clean_sch.menu_stm AS source
ON target.menu_id = source.menu_id AND target.is_current = TRUE
WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET target.eff_end_date = CURRENT_TIMESTAMP(), target.is_current = FALSE
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    INSERT (menu_dim_hk, menu_id, restaurant_id_fk, item_name, price, eff_start_date, is_current)
    VALUES (HASH(SHA1_HEX(CONCAT(source.menu_id, source.restaurant_id_fk, source.item_name, source.price))),
            source.menu_id, source.restaurant_id_fk, source.item_name, source.price,
            CURRENT_TIMESTAMP(), TRUE);

SELECT * FROM consumption_sch.menu_dim ORDER BY menu_id, eff_start_date;


-- ============================================================
-- DELTA LOAD: DELIVERY AGENT
-- ============================================================
use schema stage_sch;

COPY INTO stage_sch.deliveryagent (
    deliveryagentid, name, phone, vehicletype, locationid, status,
    gender, rating, createddate, modifieddate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
FROM (
    SELECT
        t.$1::text, t.$2::text, t.$3::text, t.$4::text, t.$5::text,
        t.$6::text, t.$7::text, t.$8::text, t.$9::text, t.$10::text,
        metadata$filename, metadata$file_last_modified,
        metadata$file_content_key, current_timestamp
    FROM @stage_sch.csv_stg/delta/delivery-agent/ t
)
FILE_FORMAT = (FORMAT_NAME = 'stage_sch.csv_file_format')
ON_ERROR = abort_statement;

SELECT * FROM stage_sch.deliveryagent_stm;

-- Clean layer merge (re-run)
use schema clean_sch;

MERGE INTO clean_sch.delivery_agent AS target
USING stage_sch.deliveryagent_stm AS source
ON target.delivery_agent_id = source.deliveryagentid
WHEN MATCHED THEN
    UPDATE SET
        target.phone = source.phone,
        target.rating = TRY_TO_DECIMAL(source.rating, 4, 2),
        target.status = source.status,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5
WHEN NOT MATCHED THEN
    INSERT (delivery_agent_id, name, phone, vehicle_type, location_id_fk, rating,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5)
    VALUES (TRY_TO_NUMBER(source.deliveryagentid), source.name, source.phone, source.vehicletype,
            TRY_TO_NUMBER(source.locationid), TRY_TO_NUMBER(source.rating),
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);

SELECT * FROM clean_sch.delivery_agent_stm;

-- Consumption layer merge (re-run)
use schema consumption_sch;

MERGE INTO consumption_sch.delivery_agent_dim AS target
USING clean_sch.delivery_agent_stm AS source
ON target.delivery_agent_id = source.delivery_agent_id AND target.is_current = TRUE
WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET target.eff_end_date = CURRENT_TIMESTAMP(), target.is_current = FALSE
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    INSERT (delivery_agent_hk, delivery_agent_id, name, rating, eff_start_date, is_current)
    VALUES (HASH(SHA1_HEX(CONCAT(source.delivery_agent_id, source.name, source.rating))),
            source.delivery_agent_id, source.name, source.rating,
            CURRENT_TIMESTAMP(), TRUE);

SELECT * FROM consumption_sch.delivery_agent_dim ORDER BY delivery_agent_id, eff_start_date;


-- ============================================================
-- DELTA LOAD: ORDERS
-- ============================================================
use schema stage_sch;

COPY INTO stage_sch.orders (
    orderid, customerid, restaurantid, orderdate, totalamount,
    status, paymentmethod, createddate, modifieddate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
FROM (
    SELECT
        t.$1::text, t.$2::text, t.$3::text, t.$4::text, t.$5::text,
        t.$6::text, t.$7::text, t.$8::text, t.$9::text,
        metadata$filename, metadata$file_last_modified,
        metadata$file_content_key, current_timestamp
    FROM @stage_sch.csv_stg/delta/orders/ t
)
FILE_FORMAT = (FORMAT_NAME = 'stage_sch.csv_file_format')
ON_ERROR = abort_statement;

SELECT * FROM stage_sch.orders_stm;

-- Clean layer merge (re-run)
use schema clean_sch;

MERGE INTO clean_sch.orders AS target
USING stage_sch.orders_stm AS source
ON target.order_id = TRY_TO_NUMBER(source.orderid)
WHEN MATCHED THEN
    UPDATE SET
        target.status = source.status,
        target.total_amount = TRY_TO_DECIMAL(source.totalamount),
        target.modified_dt = TRY_TO_TIMESTAMP_TZ(source.modifieddate),
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id_fk, restaurant_id_fk, order_date, total_amount,
            status, created_dt, _stg_file_name, _stg_file_load_ts, _stg_file_md5)
    VALUES (TRY_TO_NUMBER(source.orderid), TRY_TO_NUMBER(source.customerid),
            TRY_TO_NUMBER(source.restaurantid), TRY_TO_TIMESTAMP(source.orderdate),
            TRY_TO_DECIMAL(source.totalamount), source.status,
            TRY_TO_TIMESTAMP_TZ(source.createddate),
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);

SELECT * FROM clean_sch.orders_stm;


-- ============================================================
-- DELTA LOAD: DELIVERY
-- ============================================================
use schema stage_sch;

COPY INTO stage_sch.delivery (
    deliveryid, orderid, deliveryagentid, deliverystatus, estimatedtime,
    addressid, deliverydate, createddate, modifieddate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
FROM (
    SELECT
        t.$1::text, t.$2::text, t.$3::text, t.$4::text, t.$5::text,
        t.$6::text, t.$7::text, t.$8::text, t.$9::text,
        metadata$filename, metadata$file_last_modified,
        metadata$file_content_key, current_timestamp
    FROM @stage_sch.csv_stg/delta/delivery/ t
)
FILE_FORMAT = (FORMAT_NAME = 'stage_sch.csv_file_format')
ON_ERROR = abort_statement;

SELECT * FROM stage_sch.delivery_stm;

-- Clean layer merge (re-run)
use schema clean_sch;

MERGE INTO clean_sch.delivery AS target
USING stage_sch.delivery_stm AS source
ON target.delivery_id = TO_NUMBER(source.deliveryid)
WHEN MATCHED THEN
    UPDATE SET
        target.delivery_status = source.deliverystatus,
        target.modified_date = TRY_TO_TIMESTAMP(source.modifieddate),
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5
WHEN NOT MATCHED THEN
    INSERT (delivery_id, order_id_fk, delivery_agent_id_fk, delivery_status,
            customer_address_id_fk, _stg_file_name, _stg_file_load_ts, _stg_file_md5)
    VALUES (TO_NUMBER(source.deliveryid), TO_NUMBER(source.orderid),
            TO_NUMBER(source.deliveryagentid), source.deliverystatus,
            TO_NUMBER(source.addressid),
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);

SELECT * FROM clean_sch.delivery_stm;


-- ============================================================
-- DELTA LOAD: ORDER ITEM
-- ============================================================
use schema stage_sch;

COPY INTO stage_sch.orderitem (
    orderitemid, orderid, menuid, quantity, price, subtotal,
    createddate, modifieddate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
FROM (
    SELECT
        t.$1::text, t.$2::text, t.$3::text, t.$4::text, t.$5::text,
        t.$6::text, t.$7::text, t.$8::text,
        metadata$filename, metadata$file_last_modified,
        metadata$file_content_key, current_timestamp
    FROM @stage_sch.csv_stg/delta/order-items/ t
)
FILE_FORMAT = (FORMAT_NAME = 'stage_sch.csv_file_format')
ON_ERROR = abort_statement;

SELECT * FROM stage_sch.orderitem_stm;

-- Clean layer merge (re-run)
use schema clean_sch;

MERGE INTO clean_sch.order_item AS target
USING stage_sch.orderitem_stm AS source
ON target.order_item_id = source.orderitemid
WHEN NOT MATCHED THEN
    INSERT (order_item_id, order_id_fk, menu_id_fk, quantity, price, subtotal,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5)
    VALUES (source.orderitemid, source.orderid, source.menuid, source.quantity,
            source.price, source.subtotal,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);

SELECT * FROM clean_sch.order_item_stm;


-- ============================================================
-- REFRESH FACT TABLE (run after all delta entity loads above)
-- ============================================================
use schema consumption_sch;

MERGE INTO consumption_sch.order_item_fact AS target
USING (
    SELECT
        oi.order_item_id, oi.order_id_fk, c.customer_hk, r.restaurant_hk,
        m.menu_dim_hk, dd.date_dim_hk, oi.quantity, oi.subtotal, d.delivery_status
    FROM clean_sch.order_item_stm oi
    JOIN clean_sch.orders_stm o ON oi.order_id_fk = o.order_id
    JOIN clean_sch.delivery_stm d ON o.order_id = d.order_id_fk
    JOIN consumption_sch.customer_dim c ON o.customer_id_fk = c.customer_id AND c.is_current = TRUE
    JOIN consumption_sch.restaurant_dim r ON o.restaurant_id_fk = r.restaurant_id AND r.is_current = TRUE
    JOIN consumption_sch.menu_dim m ON oi.menu_id_fk = m.menu_id AND m.is_current = TRUE
    JOIN consumption_sch.date_dim dd ON dd.calendar_date = DATE(o.order_date)
) AS source
ON target.order_item_id = source.order_item_id
WHEN NOT MATCHED THEN
    INSERT (order_item_id, order_id, customer_dim_key, restaurant_dim_key, menu_dim_key,
            order_date_dim_key, quantity, subtotal, delivery_status)
    VALUES (source.order_item_id, source.order_id_fk, source.customer_hk, source.restaurant_hk,
            source.menu_dim_hk, source.date_dim_hk, source.quantity, source.subtotal, source.delivery_status);

-- Final verification
SELECT * FROM consumption_sch.order_item_fact ORDER BY order_item_fact_sk DESC LIMIT 100;






BEGIN TRANSACTION;

    -- Clean layer: orders
    MERGE INTO clean_sch.orders AS target
    USING stage_sch.orders_stm AS source
    ON target.order_id = TRY_TO_NUMBER(source.orderid)
    WHEN MATCHED THEN
        UPDATE SET target.status = source.status,
                   target.modified_dt = TRY_TO_TIMESTAMP_TZ(source.modifieddate),
                   target._stg_file_name = source._stg_file_name,
                   target._stg_file_load_ts = source._stg_file_load_ts,
                   target._stg_file_md5 = source._stg_file_md5
    WHEN NOT MATCHED THEN
        INSERT (order_id, customer_id_fk, restaurant_id_fk, order_date, total_amount,
                status, created_dt, _stg_file_name, _stg_file_load_ts, _stg_file_md5)
        VALUES (TRY_TO_NUMBER(source.orderid), TRY_TO_NUMBER(source.customerid),
                TRY_TO_NUMBER(source.restaurantid), TRY_TO_TIMESTAMP(source.orderdate),
                TRY_TO_DECIMAL(source.totalamount), source.status,
                TRY_TO_TIMESTAMP_TZ(source.createddate),
                source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);

    -- Clean layer: delivery
    MERGE INTO clean_sch.delivery AS target
    USING stage_sch.delivery_stm AS source
    ON target.delivery_id = TO_NUMBER(source.deliveryid)
    WHEN MATCHED THEN
        UPDATE SET target.delivery_status = source.deliverystatus,
                   target._stg_file_name = source._stg_file_name,
                   target._stg_file_load_ts = source._stg_file_load_ts,
                   target._stg_file_md5 = source._stg_file_md5
    WHEN NOT MATCHED THEN
        INSERT (delivery_id, order_id_fk, delivery_agent_id_fk, delivery_status,
                customer_address_id_fk, _stg_file_name, _stg_file_load_ts, _stg_file_md5)
        VALUES (TO_NUMBER(source.deliveryid), TO_NUMBER(source.orderid),
                TO_NUMBER(source.deliveryagentid), source.deliverystatus,
                TO_NUMBER(source.addressid),
                source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);

    -- Clean layer: order item
    MERGE INTO clean_sch.order_item AS target
    USING stage_sch.orderitem_stm AS source
    ON target.order_item_id = source.orderitemid
    WHEN NOT MATCHED THEN
        INSERT (order_item_id, order_id_fk, menu_id_fk, quantity, price, subtotal,
                _stg_file_name, _stg_file_load_ts, _stg_file_md5)
        VALUES (source.orderitemid, source.orderid, source.menuid, source.quantity,
                source.price, source.subtotal,
                source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5);

COMMIT;


BEGIN TRANSACTION;

    MERGE INTO consumption_sch.order_item_fact AS target
    USING (
        SELECT
            oi.order_item_id, oi.order_id_fk, c.customer_hk, r.restaurant_hk,
            m.menu_dim_hk, dd.date_dim_hk, oi.quantity, oi.subtotal, d.delivery_status
        FROM clean_sch.order_item_stm oi
        JOIN clean_sch.orders_stm o ON oi.order_id_fk = o.order_id
        JOIN clean_sch.delivery_stm d ON o.order_id = d.order_id_fk
        JOIN consumption_sch.customer_dim c ON o.customer_id_fk = c.customer_id AND c.is_current = TRUE
        JOIN consumption_sch.restaurant_dim r ON o.restaurant_id_fk = r.restaurant_id AND r.is_current = TRUE
        JOIN consumption_sch.menu_dim m ON oi.menu_id_fk = m.menu_id AND m.is_current = TRUE
        JOIN consumption_sch.date_dim dd ON dd.calendar_date = DATE(o.order_date)
    ) AS source
    ON target.order_item_id = source.order_item_id
    WHEN NOT MATCHED THEN
        INSERT (order_item_id, order_id, customer_dim_key, restaurant_dim_key, menu_dim_key,
                order_date_dim_key, quantity, subtotal, delivery_status)
        VALUES (source.order_item_id, source.order_id_fk, source.customer_hk, source.restaurant_hk,
                source.menu_dim_hk, source.date_dim_hk, source.quantity, source.subtotal,
                source.delivery_status);

COMMIT;