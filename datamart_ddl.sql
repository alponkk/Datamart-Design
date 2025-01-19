-- DROP SCHEMA data_mart_design;

CREATE SCHEMA data_mart_design AUTHORIZATION postgres;

-- DROP SEQUENCE data_mart_design.fact_stocks_fact_id_seq;

CREATE SEQUENCE data_mart_design.fact_stocks_fact_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;-- data_mart_design.dim_activities definition

-- Drop table

-- DROP TABLE data_mart_design.dim_activities;

CREATE TABLE data_mart_design.dim_activities (
	activity_id int8 NOT NULL,
	activity_name varchar(255) NULL,
	CONSTRAINT dim_activities_pkey PRIMARY KEY (activity_id)
);


-- data_mart_design.dim_entities definition

-- Drop table

-- DROP TABLE data_mart_design.dim_entities;

CREATE TABLE data_mart_design.dim_entities (
	entity_id int8 NOT NULL,
	entity_name varchar(255) NULL,
	entity_tag varchar(255) NULL,
	province varchar(255) NULL,
	regency varchar(255) NULL,
	CONSTRAINT dim_entities_pkey PRIMARY KEY (entity_id)
);


-- data_mart_design.dim_materials definition

-- Drop table

-- DROP TABLE data_mart_design.dim_materials;

CREATE TABLE data_mart_design.dim_materials (
	material_id int8 NOT NULL,
	material_name varchar(255) NULL,
	vaccine_status int4 NULL,
	stockcount_status int4 NULL,
	addremove_status int4 NULL,
	openvial_status int4 NULL,
	CONSTRAINT dim_materials_pkey PRIMARY KEY (material_id)
);


-- data_mart_design.fact_stocks definition

-- Drop table

-- DROP TABLE data_mart_design.fact_stocks;

CREATE TABLE data_mart_design.fact_stocks (
	fact_id serial4 NOT NULL,
	entity_id int8 NULL,
	material_id int8 NULL,
	activity_id int8 NULL,
	"date" date NOT NULL,
	on_hand_stock int4 NULL,
	CONSTRAINT fact_stocks_pkey PRIMARY KEY (fact_id),
	CONSTRAINT unique_fact UNIQUE (entity_id, material_id, activity_id, date),
	CONSTRAINT fk_activity FOREIGN KEY (activity_id) REFERENCES data_mart_design.dim_activities(activity_id),
	CONSTRAINT fk_entity FOREIGN KEY (entity_id) REFERENCES data_mart_design.dim_entities(entity_id),
	CONSTRAINT fk_material FOREIGN KEY (material_id) REFERENCES data_mart_design.dim_materials(material_id)
);