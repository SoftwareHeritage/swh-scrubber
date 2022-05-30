-- SWH Scrubber DB schema upgrade
-- from_version: 1
-- to_version: 2
-- description: Add fixed_objects

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

-- fixed_object

create unique index concurrently fixed_object_pkey on fixed_object(id);
alter table fixed_object add primary key using index fixed_object_pkey;
