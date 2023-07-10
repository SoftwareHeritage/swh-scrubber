-- SWH Scrubber DB schema upgrade
-- from_version: 5
-- to_version: 6
-- description: Introduce the check_config table, create a new version of the
--              checked_partition table and fill it, remove the old one.


create table check_config
(
  id					serial not null,
  datastore             int not null,
  object_type           object_type not null,
  nb_partitions         bigint not null,
  name                  text,
  comment               text
);

comment on table check_config is 'Configuration of a checker for a given object type from a given datastore.';
comment on column check_config.datastore is 'The datastore this checker config is about.';
comment on column check_config.object_type is 'The type of checked objects.';
comment on column check_config.nb_partitions is 'Number of partitions the set of objects is split into.';

insert into check_config (datastore, object_type, nb_partitions)
  select distinct datastore, object_type, nb_partitions
  from checked_partition;


create unique index concurrently check_config_pkey on check_config(id);
alter table check_config add primary key using index check_config_pkey;

alter table checked_partition rename to old_checked_partition;

create table checked_partition
(
  config_id             int not null,
  partition_id          bigint not null,
  start_date            timestamptz,
  end_date              timestamptz
);

comment on table checked_partition is 'Each row represents a range of objects in a datastore that were fetched, checksummed, and checked at some point in the past. The whole set of objects of the given type is split into config.nb_partitions and partition_id is a value from 0 to config.nb_partitions-1.';
comment on column checked_partition.config_id is 'The check configuration this partition concerns.';
comment on column checked_partition.partition_id is 'Index of the partition to fetch';
comment on column checked_partition.start_date is 'Date the last scrub started for this partition.';
comment on column checked_partition.end_date is 'Date the last scrub ended of this partition.';

insert into checked_partition
  select CC.id, CP.partition_id, CP.last_date, CP.last_date
  from old_checked_partition as CP
  inner join check_config as CC using (datastore, object_type, nb_partitions);

drop table old_checked_partition cascade;

create unique index concurrently checked_partition_pkey on checked_partition(config_id, partition_id);
alter table checked_partition add primary key using index checked_partition_pkey;
