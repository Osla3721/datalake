-- создаем STG слой
create external table acheryabkin.stg_payment (user_id int, 
      pay_doc_type text, 
      pay_doc_num int, 
      account text, 
      phone text, 
      billing_period date, 
      pay_date date, 
      sum numeric(10,2))
  location ('pxf://rt-2021-03-25-16-47-29-sfunu-acheryabkin/data_lake/stg/payment/*/?PROFILE=gs:parquet') 
  FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- создаем ODS слой
create table acheryabkin.ods_payment (user_id INT,
	pay_doc_type TEXT, 
	pay_doc_num INT,
	account TEXT,
	phone TEXT,
	billing_period DATE,
	pay_date DATE,
	sum NUMERIC(10,2));

-- переносим данные из STG в ODS
insert into acheryabkin.ods_payment
	select * from acheryabkin.stg_payment;

-- создаем VIEW
CREATE VIEW "rtk_de"."acheryabkin"."ods_v_payment" AS (
	SELECT
		user_id,
		pay_doc_type,
		pay_doc_num,
		account,
		phone,
		billing_period,
		pay_date,
		sum,
		user_id::TEXT AS USER_KEY,
		account::TEXT AS ACCOUNT_KEY,
		billing_period::TEXT AS BILLING_PERIOD_KEY,
		'PAYMENT - DATA LAKE'::TEXT AS RECORD_SOURCE,

		CAST((MD5(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''))) AS TEXT) AS USER_PK,
		CAST((MD5(NULLIF(UPPER(TRIM(CAST(account AS VARCHAR))), ''))) AS TEXT) AS ACCOUNT_PK,
		CAST((MD5(NULLIF(UPPER(TRIM(CAST(billing_period AS VARCHAR))), ''))) AS TEXT) AS BILLING_PERIOD_PK,
		CAST(MD5(NULLIF(CONCAT_WS('||',
			COALESCE(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''), '^^'),
			COALESCE(NULLIF(UPPER(TRIM(CAST(account AS VARCHAR))), ''), '^^'),
			COALESCE(NULLIF(UPPER(TRIM(CAST(billing_period AS VARCHAR))), ''), '^^')
		), '^^||^^||^^')) AS TEXT) AS PAY_PK,
		CAST(MD5(CONCAT_WS('||',
			COALESCE(NULLIF(UPPER(TRIM(CAST(phone AS VARCHAR))), ''), '^^')
		)) AS TEXT) AS USER_HASHDIFF,
	    current_date as LOAD_DATE,
	    pay_date AS EFFECTIVE_FROM

	FROM "rtk_de"."acheryabkin"."ods_payment");

-- создаем HUB'ы
create table acheryabkin.dds_hub_account (account_pk TEXT, 
	account_key VARCHAR, 
	load_date TIMESTAMP, 
	record_source VARCHAR);
create table acheryabkin.dds_hub_billing_period (billing_period_pk TEXT, 
	billing_period_key VARCHAR, 
	load_date TIMESTAMP, 
	record_source VARCHAR);
create table acheryabkin.dds_hub_pay_doc (pay_doc_pk TEXT, 
	pay_doc_type_key VARCHAR, 
	pay_doc_num_key VARCHAR, 
	load_date TIMESTAMP, 
	record_source VARCHAR);
create table acheryabkin.dds_hub_user (user_pk TEXT, 
	user_key VARCHAR, 
	load_date TIMESTAMP, 
	record_source VARCHAR);

-- создаем LINK'и
create table acheryabkin.dds_link_user_account_billing_pay (user_account_billing_pay_pk TEXT, 
	user_pk TEXT,
	account_pk TEXT,
	billing_period_pk TEXT,
	pay_doc_pk TEXT,
	load_date TIMESTAMP, 
	record_source VARCHAR);

-- создаем SAT'ы
create table acheryabkin.dds_sat_pay_details (user_account_billing_pay_pk TEXT, 
	pay_doc_hashdiff TEXT,
	pay_date DATE,
	sum NUMERIC(10,2),
	effective_from DATE,
	load_date TIMESTAMP, 
	record_source VARCHAR);

create table acheryabkin.dds_sat_user_details (user_pk TEXT, 
	user_hashdiff TEXT,
	phone VARCHAR,
	effective_from DATE,
	load_date TIMESTAMP,
	record_source VARCHAR);
	