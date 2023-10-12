-- SWH Scrubber DB schema upgrade
-- from_version: 6
-- to_version: 7
-- description: Replace datastore column by a config_id one in missing_object,
--              corrupt_object and missing_object_reference


drop index check_config_unicity_idx;

alter table check_config
  add column   check_hashes boolean not null default TRUE,
  add column   check_references boolean not null default TRUE;

create unique index check_config_unicity_idx
    on check_config(datastore, object_type, nb_partitions, check_hashes, check_references);

--- First, we look if there are datastores used by several check_config entries
--- (for a given object type); If there are, we cannot automatically upgrade
--- the DB, since we cannot choose the config_id to use in missing_object,
--- corrupt_object and missing_object_reference tables (in place of datastore)
create temporary table invalid_configs (
  datastore integer,
  object_type text,
  n_cfg bigint,
  n_rows bigint
  );

with
  m as (
    select * from (values ('swh:1:rev:', 'revision'),
	                      ('swh:1:rel:', 'release'),
	                      ('swh:1:dir:', 'directory'),
						  ('swh:1:cnt:', 'content'))
						  as m (prefix, object_type)
						  ),
  dup_cfg as (
    select count(*) as n, datastore, object_type
  	from check_config
  	group by 2, 3
  	having (count(*) > 1)
  ),
  mo as (
    select id, substring(id, 0, 11) as prefix, datastore
	from missing_object
	union
    select reference_id as id, substring(reference_id, 0, 11) as prefix, datastore
	from missing_object_reference
	union
    select id, substring(id, 0, 11) as prefix, datastore
	from corrupt_object
	)

insert into invalid_configs
select mo.datastore, m.object_type, dup_cfg.n as n_configs, count(mo.id)
from mo
  inner join m on ( mo.prefix = m.prefix)
  inner join dup_cfg on (mo.datastore=dup_cfg.datastore)

group by 1, 2, 3;


select count(*)>0 as found_invalid from invalid_configs \gset

select * from invalid_configs;

\if :found_invalid
\warn 'Found datastores used by several config check sessions.'
\warn 'Sorry, you need to sort this by hand...'
--- Did not find an elegant way of stopping the migration script here...
-- so let's generate a syntax error...
fail
\else
\echo 'Seems each datastore is used in only one config_check, let''s continue'
\endif


---  Now we should be ok to do the actual migration:
---  1. add config_id columns to the xxx_object tables
---  2. fill the column (there should be only one possible config_id per row now)
---  3. drop the datastore column

create temporary table swhid_map (prefix text, object_type object_type);
insert into swhid_map values
  ('swh:1:rev:', 'revision'),
  ('swh:1:rel:', 'release'),
  ('swh:1:dir:', 'directory'),
  ('swh:1:cnt:', 'content'),
  ('swh:1:snp:', 'snapshot')
;

alter table corrupt_object
add column config_id int;

update corrupt_object as upd
set config_id = cc.id
from check_config as cc
inner join swhid_map on (swhid_map.object_type = cc.object_type)
where substring(upd.id, 0, 11) = swhid_map.prefix
  and cc.datastore = upd.datastore
;

alter table missing_object_reference
add column config_id int;

update missing_object_reference as upd
set config_id = cc.id
from check_config as cc
inner join swhid_map on (swhid_map.object_type = cc.object_type)
where substring(upd.reference_id, 0, 11) = swhid_map.prefix
  and cc.datastore = upd.datastore
;

alter table missing_object
add column config_id int;

update missing_object as upd
set config_id = mor.config_id
from missing_object_reference as mor
where upd.id = mor.missing_id and upd.datastore = mor.datastore
;

-- now we can remove the datastore column for these tables

alter table corrupt_object
drop column datastore;
alter table missing_object
drop column datastore;
alter table missing_object_reference
drop column datastore;

-- and restore indexes and foreign key validation

alter table corrupt_object add constraint corrupt_object_config_fkey foreign key (config_id) references check_config(id) not valid;
alter table corrupt_object validate constraint corrupt_object_config_fkey;

create unique index corrupt_object_pkey on corrupt_object(id, config_id);
alter table corrupt_object add primary key using index corrupt_object_pkey;

alter table missing_object add constraint missing_object_config_fkey foreign key (config_id) references check_config(id) not valid;
alter table missing_object validate constraint missing_object_config_fkey;

create unique index missing_object_pkey on missing_object(id, config_id);
alter table missing_object add primary key using index missing_object_pkey;

alter table missing_object_reference add constraint missing_object_reference_config_fkey foreign key (config_id) references check_config(id) not valid;
alter table missing_object_reference validate constraint missing_object_reference_config_fkey;

create unique index missing_object_reference_missing_id_reference_id_config on missing_object_reference(missing_id, reference_id, config_id);
create unique index missing_object_reference_reference_id_missing_id_config on missing_object_reference(reference_id, missing_id, config_id);
