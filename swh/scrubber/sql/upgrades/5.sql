-- SWH Scrubber DB schema upgrade
-- from_version: 4
-- to_version: 5
-- description: Replace checked_range with checked_partition


-- Was corrupted in prod, so we lost the existing data anyway
drop table if exists checked_range;

DO $$ BEGIN
    create type object_type as enum ('content', 'directory', 'revision', 'release', 'snapshot', 'extid', 'raw_extrinsic_metadata');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

create table checked_partition
(
  datastore             int not null,
  object_type           object_type not null,
  partition_id          bigint not null,
  nb_partitions         bigint not null,
  last_date             timestamptz not null
);

comment on table checked_partition is 'Each row represents a range of objects in a datastore that were fetched, checksummed, and checked at some point in the past. The whole set of objects of the given type is split into nb_partitions and partition_id is a value from 0 to nb_partitions-1.';
comment on column checked_partition.object_type is 'The type of tested objects.';
comment on column checked_partition.partition_id is 'Index of the partition to fetch';
comment on column checked_partition.nb_partitions is 'Number of partitions the set of objects is split into.';
comment on column checked_partition.last_date is 'Date the last scrub of this partition *started*.';

create unique index concurrently checked_partition_pkey on checked_partition(datastore, object_type, nb_partitions, partition_id);
alter table checked_partition add primary key using index checked_partition_pkey;
