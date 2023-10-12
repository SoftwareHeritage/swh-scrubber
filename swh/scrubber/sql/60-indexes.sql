-------------------------------------
-- Shared tables
-------------------------------------

-- datastore

create unique index datastore_pkey on datastore(id);
alter table datastore add primary key using index datastore_pkey;

create unique index datastore_package_class_instance on datastore(package, class, instance);

-------------------------------------
-- Checker config
-------------------------------------

create unique index check_config_pkey on check_config(id);
create unique index check_config_unicity_idx on check_config(datastore, object_type, nb_partitions, check_hashes, check_references);
alter table check_config add primary key using index check_config_pkey;

-------------------------------------
-- Checkpointing/progress tracking
-------------------------------------

create unique index checked_partition_pkey on checked_partition(config_id, partition_id);
alter table checked_partition add primary key using index checked_partition_pkey;

-------------------------------------
-- Inventory of objects with issues
-------------------------------------

-- corrupt_object

alter table corrupt_object add constraint corrupt_object_config_fkey foreign key (config_id) references check_config(id) not valid;
alter table corrupt_object validate constraint corrupt_object_config_fkey;

create unique index corrupt_object_pkey on corrupt_object(id, config_id);
alter table corrupt_object add primary key using index corrupt_object_pkey;


-- missing_object

alter table missing_object add constraint missing_object_config_fkey foreign key (config_id) references check_config(id) not valid;
alter table missing_object validate constraint missing_object_config_fkey;

create unique index missing_object_pkey on missing_object(id, config_id);
alter table missing_object add primary key using index missing_object_pkey;


-- missing_object_reference

alter table missing_object_reference add constraint missing_object_reference_config_fkey foreign key (config_id) references check_config(id) not valid;
alter table missing_object_reference validate constraint missing_object_reference_config_fkey;

create unique index missing_object_reference_missing_id_reference_id_config on missing_object_reference(missing_id, reference_id, config_id);
create unique index missing_object_reference_reference_id_missing_id_config on missing_object_reference(reference_id, missing_id, config_id);

-------------------------------------
-- Issue resolution
-------------------------------------

-- object_origin

create unique index object_origin_pkey on object_origin (object_id, origin_url);
create index object_origin_by_origin on object_origin (origin_url, object_id);

-- FIXME: not valid, because corrupt_object(id) is not unique
-- alter table object_origin add constraint object_origin_object_fkey foreign key (object_id) references corrupt_object(id) not valid;
-- alter table object_origin validate constraint object_origin_object_fkey;

-- fixed_object

create unique index fixed_object_pkey on fixed_object(id);
alter table fixed_object add primary key using index fixed_object_pkey;
