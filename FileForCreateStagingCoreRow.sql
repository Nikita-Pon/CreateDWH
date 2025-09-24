--alter table film_src.inventory add column deleted timestamp OPTIONS(column_name 'deleted') null;
--alter table staging.inventory add column deleted timestamp null;
--alter table film_src.staff add column deleted timestamp OPTIONS(column_name 'deleted') null;
--alter table staging.staff add column deleted timestamp null;
--alter table staging.inventory add column last_update timestamp not null;
--delete from staging.inventory;
--alter table core.dim_inventory add column effective_date_from timestamp default to_date('1900-01-01', 'yyyy-MM-hh') not null;
--alter table core.dim_inventory add column effective_date_to timestamp default to_date('1900-01-01', 'yyyy-MM-hh') not null;
--alter table core.dim_inventory add column is_active boolean default true not null;
--ALTER TABLE core.dim_staff drop CONSTRAINT dim_staff_staff_id_key;

--Создание таблиц staging слоя
DROP TABLE if exists staging.last_update;
create table staging.last_update(
	table_name varchar(50) not null,
	update_dt timestamp not null
);


drop table if exists staging.film;
CREATE TABLE staging.film (
	film_id int NOT NULL,
	title varchar(255) NOT NULL,
	description text NULL,
	release_year int2 NULL,
	language_id int2 NOT NULL,
	rental_duration int2 NOT NULL,
	rental_rate numeric(4, 2) NOT NULL,
	length int2 NULL,
	replacement_cost numeric(5, 2) NOT NULL,
	rating varchar(10) NULL,
	last_update timestamp NOT NULL,
	special_features _text NULL,
	fulltext tsvector NOT NULL
);

drop procedure if exists staging.film_load();
create procedure staging.film_load()
as $$
	begin
		--или delete from staging.film;
 		truncate table staging.film;
		
		INSERT INTO staging.film
		(film_id,
		title,
		description,
		release_year,
		language_id,
		rental_duration,
		rental_rate,
		length,
		replacement_cost,
		rating,
		last_update,
		special_features,
		fulltext)
		select
			film_id,
			title,
			description,
			release_year,
			language_id,
			rental_duration,
			rental_rate,
			length,
			replacement_cost,
			rating,
			last_update,
			special_features,
			fulltext
		from film_src.film;
		
	end;
$$ language plpgsql; 

DROP TABLE if exists staging.inventory;
CREATE TABLE staging.inventory (
	inventory_id int NOT NULL,
	film_id int2 NOT NULL,
	store_id int2 NOT NULL,
	last_update timestamp not null,
	deleted timestamp null
);

DROP table if exists staging.rental;
CREATE TABLE staging.rental (
	rental_id int NOT NULL,
	rental_date timestamp NOT NULL,
	inventory_id int4 NOT NULL,
	customer_id int2 NOT NULL,
	return_date timestamp NULL,
	staff_id int2 NOT NULL,
	last_update timestamp NOT NULL
);

DROP table if exists staging.payment;
CREATE TABLE staging.payment (
	payment_id int NOT NULL,
	customer_id int2 NOT NULL,
	staff_id int2 NOT NULL,
	rental_id int4 NOT NULL,
	amount numeric(5, 2) NOT NULL,
	payment_date timestamp NOT NULL
);

DROP table if exists staging.staff;
CREATE TABLE staging.staff (
	staff_id int4 NOT NULL,
	first_name varchar(45) NOT NULL,
	last_name varchar(45) not NULL,
	store_id int2 NOT null,
	last_update timestamp not null,
	deleted timestamp null
);

DROP table if exists staging.address;
CREATE TABLE staging.address (
	address_id int NOT NULL,
	address varchar(50) NOT NULL,
	district varchar(20) NOT NULL,
	city_id int2 NOT NULL
);

DROP table if exists staging.city;
CREATE TABLE staging.city (
	city_id int NOT NULL,
	city varchar(50) NOT NULL
);

DROP table if exists staging.store;
CREATE TABLE staging.store (
	store_id int NOT NULL,
	address_id int2 NOT NULL
);

create or replace procedure staging.inventory_load()
as $$
	declare
		last_update_dt timestamp;
	
	begin
		last_update_dt = coalesce(
			(select max(update_dt)
			from staging.last_update
			where table_name = 'staging.inventory'),
			'1900-01-01'::date
		);

 		delete from staging.inventory;
		
		insert into
			staging.inventory
			(inventory_id,
			film_id,
			store_id,
			last_update,
			deleted)
		select
			inventory_id,
			film_id,
			store_id,
			last_update,
			deleted
		from film_src.inventory 
		where deleted >= last_update_dt
			or last_update >= last_update_dt;

		INSERT INTO 
		staging.last_update
			(table_name,
			update_dt)
		values
			('staging.inventory',
			now());
		
	end;
$$ language plpgsql; 

create or replace procedure staging.rental_load()
as $$
	begin
 		truncate table staging.rental;
		
		insert
		into
			staging.rental
			(rental_id,
			rental_date,
			inventory_id,
			customer_id,
			return_date,
			staff_id,
			last_update)
		select
			rental_id,
			rental_date,
			inventory_id,
			customer_id,
			return_date,
			staff_id,
			last_update
		from film_src.rental;
		
	end;
$$ language plpgsql; 

drop procedure if exists staging.payment_load();
create procedure staging.payment_load()
as $$
	begin
 		truncate table staging.payment;
		
		insert
		into
			staging.payment
			(payment_id,
			customer_id,
			staff_id,
			rental_id,
			amount,
			payment_date)
		select
			payment_id,
			customer_id,
			staff_id,
			rental_id,
			amount,
			payment_date
		from film_src.payment;
		
	end;
$$ language plpgsql; 

drop procedure if exists staging.staff_load();
create procedure staging.staff_load()
as $$
	declare
		last_update_dt timestamp;
	begin
		last_update_dt = coalesce(
			(select max(update_dt)
			from staging.last_update
			where table_name = 'staging.staff'),
			'1900-01-01'::date);
		
 		delete from staging.staff;
		
		insert into
		staging.staff
			(staff_id,
			first_name,
			last_name,
			store_id,
			last_update,
			deleted)
		select staff_id,
			first_name,
			last_name,
			store_id,
			last_update,
			deleted
		from film_src.staff
		where last_update >= last_update_dt
			or deleted >= last_update_dt;

		INSERT INTO staging.last_update
			(table_name, 
			update_dt)
		VALUES('staging.staff', now());
		
	end;
$$ language plpgsql; 

drop procedure if exists staging.address_load();
create procedure staging.address_load()
as $$
	begin
 		truncate table staging.address;
		
		insert
		into
		staging.address
			(address_id,
			address,
			district,
			city_id)
		select
			address_id,
			address,
			district,
			city_id
		from film_src.address;
		
	end;
$$ language plpgsql; 

drop procedure if exists staging.city_load();
create procedure staging.city_load()
as $$
	begin
 		truncate table staging.city;
		
		insert
		into
		staging.city
		(city_id,
		city)
		select
			city_id,
			city
		from film_src.city;
		
	end;
$$ language plpgsql; 

drop procedure if exists staging.store_load();
create procedure staging.store_load()
as $$
	begin
 		truncate table staging.store;
		
		insert
		into
		staging.store
		(store_id,
		address_id)
		select
			store_id,
			address_id
		from film_src.store;
		
	end;
$$ language plpgsql; 

--создание таблиц core слоя

drop table if exists core.fact_payment;
drop table if exists core.fact_rental;
drop table if exists core.dim_date;
drop table if exists core.dim_inventory;
drop table if exists core.dim_staff;
drop table if exists core.dim_date;

create table core.dim_date
(
  dim_date_pk INT primary key,
  date_actual DATE not null,
  epoch BIGINT not null,
  day_suffix VARCHAR(4) not null,
  day_name VARCHAR(11) not null,
  day_of_week INT not null,
  day_of_month INT not null,
  day_of_quarter INT not null,
  day_of_year INT not null,
  week_of_month INT not null,
  week_of_year INT not null,
  week_of_year_iso CHAR(10) not null,
  month_actual INT not null,
  month_name VARCHAR(8) not null,
  month_name_abbreviated CHAR(3) not null,
  quarter_actual INT not null,
  quarter_name VARCHAR(9) not null,
  year_actual INT not null,
  first_day_of_week DATE not null,
  last_day_of_week DATE not null,
  first_day_of_month DATE not null,
  last_day_of_month DATE not null,
  first_day_of_quarter DATE not null,
  last_day_of_quarter DATE not null,
  first_day_of_year DATE not null,
  last_day_of_year DATE not null,
  mmyyyy CHAR(6) not null,
  mmddyyyy CHAR(10) not null,
  weekend_indr BOOLEAN not null
);

create index dim_date_date_actual_idx
  on
core.dim_date(date_actual);


create table core.dim_inventory(
	inventory_pk serial primary key,
	inventory_id int not null, -- unique,
	film_id int not null,
	title varchar(255)  not null,
	rental_duration int2 not null,
	rental_rate numeric(4,2) not null,
	length int2,
	rating varchar(10),
	effective_date_from timestamp not null,
	effective_date_to timestamp not null,
	is_active boolean not null
);

create table core.dim_staff(
	staff_pk serial primary key,
	staff_id int not null, -- unique,
	first_name varchar(45) not null,
	last_name varchar(45) not null,
	address varchar(50) not null,
	district varchar(20) not null,
	city_name varchar(50) not null,
	effective_date_from timestamp not null,
	effective_date_to timestamp not null,
	is_active boolean not null
);	

create table core.fact_payment(
	payment_pk serial primary key,
	payment_id int not null,
	amount numeric(5,2) not null,
	payment_date_fk int not null references core.dim_date(dim_date_pk),
	inventory_fk int not null references core.dim_inventory(inventory_pk),
	staff_fk int not null references core.dim_staff(staff_pk)
);

create table core.fact_rental(
	rental_pk serial primary key,
	rental_id int not null,
	inventory_fk integer not null references core.dim_inventory(inventory_pk),
	staff_fk integer not null references core.dim_staff(staff_pk),
	rental_date_fk integer not null references core.dim_date(dim_date_pk),
	return_date_fk integer null references core.dim_date(dim_date_pk),
	cnt int2 not null,
	amount  numeric(7,2)
);

create or replace procedure core.load_date(sdate date, nm int)
as $$
	begin
		SET lc_time = 'ru_RU';

		INSERT INTO core.dim_date
		SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS dim_date_pk,
		       datum AS date_actual,
		       EXTRACT(EPOCH FROM datum) AS epoch,
		       TO_CHAR(datum, 'fmDDth') AS day_suffix,
		       TO_CHAR(datum, 'TMDay') AS day_name,
		       EXTRACT(ISODOW FROM datum) AS day_of_week,
		       EXTRACT(DAY FROM datum) AS day_of_month,
		       datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
		       EXTRACT(DOY FROM datum) AS day_of_year,
		       TO_CHAR(datum, 'W')::INT AS week_of_month,
		       EXTRACT(WEEK FROM datum) AS week_of_year,
		       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
		       EXTRACT(MONTH FROM datum) AS month_actual,
		       TO_CHAR(datum, 'TMMonth') AS month_name,
		       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
		       EXTRACT(QUARTER FROM datum) AS quarter_actual,
		       CASE
		           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
		           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
		           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
		           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
		           END AS quarter_name,
		       EXTRACT(YEAR FROM datum) AS year_actual,
		       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
		       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
		       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
		       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
		       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
		       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
		       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
		       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
		       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
		       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
		       CASE
		           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
		           ELSE FALSE
		           END AS weekend_indr
		FROM (SELECT sdate + SEQUENCE.DAY AS datum
		      FROM GENERATE_SERIES(0, nm-1) AS SEQUENCE (DAY)
		      ORDER BY SEQUENCE.DAY) DQ
		ORDER BY 1;
	end;
$$ language plpgsql;

create or replace procedure core.load_inventory()
as $$
	begin
--		delete from core.dim_inventory
--		where inventory_id in (
--			select inventory_id
--			from staging.inventory
--			where deleted is not null
--		);
		--  помечаем удаленные записи
		update core.dim_inventory i
		set is_active = false,
			effective_date_to = si.deleted
		from staging.inventory si
		where i.inventory_id = si.inventory_id
			and si.deleted is not null
			and i.is_active is true;

		-- Получаем список идентификаторов новых компакт дисков
		create temporary table new_inventory_id_list on commit drop as
		select i.inventory_id
		from staging.inventory i 
			left join core.dim_inventory di using(inventory_id)
		where di.inventory_id is null;

		--добавляем новые компакт диски в измерение core.dim_inventory		
		insert into
			core.dim_inventory
			(inventory_id,
			film_id,
			title,
			rental_duration,
			rental_rate,
			length,
			rating,
			effective_date_from,
			effective_date_to,
			is_active)
		select i.inventory_id,
			i.film_id,
			f.title, 
			f.rental_duration,
			f.rental_rate, 
			f.length, 
			f.rating,
			'1900-01-01'::date as effective_date_from,
			coalesce(i.deleted, '9999-01-01'::date) as effective_date_to,
			true as is_active
		from staging.inventory i
			join staging.film f using(film_id)
			join new_inventory_id_list idl using(inventory_id);		
--		where deleted is null
--		on conflict (inventory_id) do update
--		set film_id = excluded.film_id,
--			title = excluded.title,
--			rental_duration = excluded.rental_duration,
--			rental_rate = excluded.rental_rate,
--			length = excluded.length,
--			rating = excluded.rating;
		
		--помечаем измененные компакт диски не активными
		update core.dim_inventory i
		set is_active = false,
			effective_date_to = si.last_update
		from staging.inventory si
			left join new_inventory_id_list idl using(inventory_id)
		where idl.inventory_id is null
			and si.deleted is null
			and i.inventory_id = si.inventory_id
			and i.is_active is true;
	
		--по измененным компакт дискам добавляем актуальные записи
		insert into
			core.dim_inventory
			(inventory_id,
			film_id,
			title,
			rental_duration,
			rental_rate,
			length,
			rating,
			effective_date_from,
			effective_date_to,
			is_active)
		select i.inventory_id,
			f.film_id,
			f.title, 
			f.rental_duration,
			f.rental_rate, 
			f.length, 
			f.rating,
			i.last_update as effective_date_from,
			'9999-01-01'::date as effective_date_to,
			true as is_active
		from staging.inventory i
			join staging.film f using(film_id)
			left join new_inventory_id_list idl using(inventory_id)
		where idl.inventory_id is null
			and i.deleted is null;	
	end;
$$ language plpgsql;

create or replace procedure core.load_staff()
as $$
	begin
--		delete from core.dim_staff
--		where staff_id in 
--			(select staff_id
--			from staging.staff
--			where deleted is not null);
--		Помечаем удаленные записи
		update core.dim_staff ds
		set is_active = false,
			effective_date_to = s.deleted
		from staging.staff s
		where ds.staff_id = s.staff_id
			and s.deleted is not null
			and ds.is_active is true;

--		Получаем список идентификаторов новых сотрудников
		create temporary table new_staff_id_list on commit drop as
		select s.staff_id
		from staging.staff s
			left join core.dim_staff ds using(staff_id)
		where ds.staff_id is null;

--		добавляем новых сотрудников в измерение core.dim_staff	
		insert
			into
			core.dim_staff(
			staff_id,
			first_name,
			last_name,
			address,
			district,
			city_name,
			effective_date_from,
			effective_date_to,
			is_active)
		select s.staff_id,
			s.first_name,
			s.last_name,
			a.address,
			a.district,
			c.city,
			'1900-01-01'::date as effective_date_from,
			coalesce(s.deleted, '9999-01-01'::date) as effective_date_to,
			true as is_active
		from staging.staff s
			join staging.store sr using(store_id)
			join staging.address a using(address_id)
			join staging.city c using(city_id)
			join new_staff_id_list nst using(staff_id);
--		where deleted is null
--		on conflict (staff_id) do update
--		set first_name = excluded.first_name,
--			last_name = excluded.last_name,
--			address = excluded.address,
--			district = excluded.district,
--			city_name = excluded.city_name;

--		помечаем записи с измененными сотрудниками неактивными
		update core.dim_staff ds
		set is_active = false,
			effective_date_to = s.last_update
		from staging.staff s
			left join new_staff_id_list nsil using(staff_id)
		where s.staff_id = ds.staff_id
			and ds.is_active = true
			and nsil.staff_id is null
			and s.deleted is null;

--		по измененным сотрудникам добавляем актуальные записи
		insert
			into
			core.dim_staff(
			staff_id,
			first_name,
			last_name,
			address,
			district,
			city_name,
			effective_date_from,
			effective_date_to,
			is_active)
		select s.staff_id,
			s.first_name,
			s.last_name,
			a.address,
			a.district,
			c.city,
			s.last_update as effective_date_from,
			'9999-01-01'::date as effective_date_to,
			true as is_active
		from staging.staff s
			join staging.store sr using(store_id)
			join staging.address a using(address_id)
			join staging.city c using(city_id)
			left join new_staff_id_list nst using(staff_id)
		where nst.staff_id is null
			and s.deleted is null;

	end;
$$ language plpgsql;

create or replace procedure core.load_payment()
as $$
	begin
		delete from core.fact_payment;

		INSERT INTO core.fact_payment
			(payment_id,
			amount,
			payment_date_fk,
			inventory_fk,
			staff_fk)
		select p.payment_id,
			p.amount,
			dd.dim_date_pk as payment_date_fk,
			i.inventory_pk,
			s.staff_pk
		from staging.payment p
			join staging.rental r using(rental_id)
			join core.dim_inventory i 
				on r.inventory_id = i.inventory_id
				and p.payment_date between i.effective_date_from and i.effective_date_to
			join core.dim_staff s
				 on p.staff_id = s.staff_id
				and p.payment_date between s.effective_date_from and s.effective_date_to
			join core.dim_date dd on dd.date_actual = p.payment_date::date;
		
	end;
$$ language plpgsql;

create or replace procedure core.load_rental()
as $$
	begin
		delete from core.fact_rental;

		INSERT INTO core.fact_rental
			(rental_id,
			inventory_fk,
			staff_fk,
			rental_date_fk,
			return_date_fk,
			cnt,
			amount)
		select 
			r.rental_id,
			di.inventory_pk as inventory_fk,
			ds.staff_pk as staff_fk,
			dd1.dim_date_pk as rental_date_fk,
			dd2.dim_date_pk as return_date_fk,
			count(1) as cnt,
			sum(p.amount) as amount
		from staging.rental r
			join core.dim_inventory di 
				on r.inventory_id = di.inventory_id
				and r.rental_date between di.effective_date_from and di.effective_date_to
			join core.dim_staff ds 
				on r.staff_id = ds.staff_id
				and r.rental_date between ds.effective_date_from and ds.effective_date_to
			join core.dim_date dd1 on dd1.date_actual = r.rental_date::date
			left join core.dim_date dd2 on dd2.date_actual = r.return_date::date
			left join staging.payment p using(rental_id)
		group by r.rental_id,
			di.inventory_pk,
			ds.staff_pk,
			dd1.dim_date_pk,
			dd2.dim_date_pk;
	end;
$$ language plpgsql;

create or replace procedure core.fact_delete()
as $$
	begin
		delete from core.fact_payment;

		delete from core.fact_rental;
	end;
$$ language plpgsql;

--Создавние data mart слоя

DROP TABLE if exists report.sales_date;
create table report.sales_date (
	date_title varchar(20) not null,
	amount numeric(7,2) not null,
	date_sort integer not null
);

create or replace procedure report.sales_date_calc()
as $$
	begin
		delete from report.sales_date;

		INSERT INTO report.sales_date
			(date_title,  --'1 сентября 2025'
			amount,
			date_sort)
		SELECT dd.day_of_month || ' ' || dd.month_name || ' ' || dd.year_actual as date_titla,
			sum(fp.amount) as amount,
			dd.dim_date_pk as date_sort
		FROM core.dim_date dd
			join core.fact_payment fp on dd.dim_date_pk = fp.payment_date_fk
		GROUP BY dd.day_of_month || ' ' || dd.month_name || ' ' || dd.year_actual,
			dd.dim_date_pk;
	end;
$$ language plpgsql;

DROP TABLE if exists report.sales_film;
create table report.sales_film(
	film_title varchar(255) not null,
	amount numeric(7,2) not null
);

create or replace procedure report.sales_film_calc()
as $$
	begin
		delete from report.sales_film;

		INSERT INTO report.sales_film
			(film_title,
			amount)
		select di.title,
			sum(fp.amount) as amount
		from core.dim_inventory di
			join core.fact_payment fp on di.inventory_pk=fp.inventory_fk
		group by di.title;
	end;
$$ language plpgsql;

call report.sales_film_calc();

------

create or replace procedure core.full_load()
as $$
	begin
		call staging.film_load();
		call staging.inventory_load();
		call staging.rental_load();
		call staging.payment_load();
		call staging.staff_load();
		call staging.address_load();
		call staging.city_load();
		call staging.store_load();
		
		call core.fact_delete();
		call core.load_inventory();
		call core.load_staff();
		call core.load_payment();
		call core.load_rental();

--		call report.sales_date_calc();
--		call report.sales_film_calc();
	end;
$$ language plpgsql;

call core.load_date('2007-01-01'::date, 7000);
call core.full_load();
--
--
--select * from core.dim_staff; 
--select * from staging.staff s;
--select * from staging.last_update;
--
--select * from core.dim_inventory di order by inventory_id desc;  --film_id = 2
--select count(*) from core.dim_inventory di;  --4591
--select * from core.fact_rental fr order by rental_id desc;  
--select * from core.dim_staff ;
