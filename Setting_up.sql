-- Create development database
create database if not exists sandbox;
use database sandbox;

-- ### **2. 3-Layer Schema Architecture**
--Establish the schemas to support the different stages of the data pipeline, including a **common** schema for shared governance objects

create schema if not exists stage_sch;
create schema if not exists clean_sch;
create schema if not exists consumption_sch;
create schema if not exists common;


--### **3. File Format and Stage Configuration**
--The following code defines how Snowflake should interpret your raw CSV data and creates an internal location to store files before they are loaded [2].


use schema stage_sch;

-- Create file format to process the CSV file
create file format if not exists stage_sch.csv_file_format   
  type = 'csv'   
  compression = 'auto'   
  field_delimiter = ','   
  record_delimiter = '\n'   
  skip_header = 1   
  field_optionally_enclosed_by = '\042'   
  null_if = ('\\N');

-- Create the snowflake internal stage
create or replace stage stage_sch.csv_stg
directory = ( enable = true )
comment = 'this is the snowflake internal stage';


-- ### **4. Security and Governance (Tagging and Masking)**
-- To protect sensitive information (PII), the project uses **Object Tagging** and **Masking Policies**. This ensures that data like emails and phone numbers are redacted when queried by unauthorised users [2].


-- Create PII policy tag
create or replace tag 
  common.pii_policy_tag 
  allowed_values 'PII','PRICE','SENSITIVE','EMAIL'
comment = 'This is PII policy tag object';

-- Create masking policies for various data types
create or replace masking policy 
  common.pii_masking_policy as (pii_text string)
returns string -> to_varchar('** PII **');

create or replace masking policy 
  common.email_masking_policy as (email_text string)
returns string -> to_varchar('** EMAIL **');

create or replace masking policy 
  common.phone_masking_policy as (phone string)
returns string -> to_varchar('** Phone **');