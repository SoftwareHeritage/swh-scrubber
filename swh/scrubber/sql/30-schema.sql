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

create table object_origin
(
  object_id             swhid not null,
  origin_url            text not null,
  last_attempt          timestamptz  -- NULL if not tried yet
);

comment on table object_origin is 'Maps objects to origins they might be found in.';
