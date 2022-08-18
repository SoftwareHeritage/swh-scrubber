-- SWH Scrubber DB schema upgrade
-- from_version: 2
-- to_version: 3
-- description: Add missing_object

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



alter table missing_object add constraint missing_object_datastore_fkey foreign key (datastore) references datastore(id) not valid;
alter table missing_object validate constraint missing_object_datastore_fkey;

create unique index concurrently missing_object_pkey on missing_object(id, datastore);
alter table missing_object add primary key using index missing_object_pkey;

alter table missing_object_reference add constraint missing_object_reference_datastore_fkey foreign key (datastore) references datastore(id) not valid;
alter table missing_object_reference validate constraint missing_object_reference_datastore_fkey;

create unique index concurrently missing_object_reference_missing_id_reference_id_datastore on missing_object_reference(missing_id, reference_id, datastore);
create unique index concurrently missing_object_reference_reference_id_missing_id_datastore on missing_object_reference(reference_id, missing_id, datastore);
