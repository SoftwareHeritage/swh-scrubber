-------------------------------------
-- Shared definitions
-------------------------------------

create domain swhid as text check (value ~ '^swh:[0-9]+:.*');

create table datastore
(
  id                bigserial not null,
  package           datastore_type not null,
  class             text,
  instance          text
);

comment on table datastore is 'Each row identifies a data store being scrubbed';
comment on column datastore.id is 'Internal identifier of the datastore';
comment on column datastore.package is 'Name of the component using this datastore (storage/journal/objstorage)';
comment on column datastore.class is 'For datastores with multiple backends, name of the backend (postgresql/cassandra for storage, kafka for journal, pathslicer/azure/winery/... for objstorage)';
comment on column datastore.instance is 'Human-readable way to uniquely identify the datastore; eg. its URL or DSN.';


-------------------------------------
-- Checkpointing/progress tracking
-------------------------------------

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

-------------------------------------
-- Inventory of objects with issues
-------------------------------------

create table corrupt_object
(
  id                    swhid not null,
  datastore             int not null,
  object                bytea not null,
  first_occurrence      timestamptz not null default now()
);

comment on table corrupt_object is 'Each row identifies an object that was found to be corrupt';
comment on column corrupt_object.datastore is 'Datastore the corrupt object was found in.';
comment on column corrupt_object.object is 'Corrupt object, as found in the datastore (possibly msgpack-encoded, using the journal''s serializer)';
comment on column corrupt_object.first_occurrence is 'Moment the object was found to be corrupt for the first time';


create table missing_object
(
  id                    swhid not null,
  datastore             int not null,
  first_occurrence      timestamptz not null default now()
);

comment on table missing_object is 'Each row identifies an object that are missing but referenced by another object (aka "holes")';
comment on column missing_object.datastore is 'Datastore where the hole is.';
comment on column missing_object.first_occurrence is 'Moment the object was found to be corrupt for the first time';

create table missing_object_reference
(
  missing_id            swhid not null,
  reference_id          swhid not null,
  datastore             int not null,
  first_occurrence      timestamptz not null default now()
);

comment on table missing_object_reference is 'Each row identifies an object that points to an object that does not exist (aka a "hole")';
comment on column missing_object_reference.missing_id is 'SWHID of the missing object.';
comment on column missing_object_reference.reference_id is 'SWHID of the object referencing the missing object.';
comment on column missing_object_reference.datastore is 'Datastore where the referencing object is.';
comment on column missing_object_reference.first_occurrence is 'Moment the object was found to reference a missing object';


-------------------------------------
-- Issue resolution
-------------------------------------

create table object_origin
(
  object_id             swhid not null,
  origin_url            text not null,
  last_attempt          timestamptz  -- NULL if not tried yet
);

comment on table object_origin is 'Maps objects to origins they might be found in.';

create table fixed_object
(
  id                    swhid not null,
  object                bytea not null,
  method                text,
  recovery_date         timestamptz not null default now()
);

comment on table fixed_object is 'Each row identifies an object that was found to be corrupt, along with the original version of the object';
comment on column fixed_object.object is 'The recovered object itself, as a msgpack-encoded dict';
comment on column fixed_object.recovery_date is 'Moment the object was recovered.';
comment on column fixed_object.method is 'How the object was recovered. For example: "from_origin", "negative_utc", "capitalized_revision_parent".';
