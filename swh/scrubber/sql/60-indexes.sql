-- datastore

create unique index concurrently datastore_pkey on datastore(id);
alter table datastore add primary key using index datastore_pkey;

create unique index concurrently datastore_package_class_instance on datastore(package, class, instance);


-- corrupt_object

alter table corrupt_object add constraint corrupt_object_datastore_fkey foreign key (datastore) references datastore(id) not valid;
alter table corrupt_object validate constraint corrupt_object_datastore_fkey;

create unique index concurrently corrupt_object_pkey on corrupt_object(id, datastore);
alter table corrupt_object add primary key using index corrupt_object_pkey;

-- object_origin

create unique index concurrently object_origin_pkey on object_origin (object_id, origin_url);
create index concurrently object_origin_by_origin on object_origin (origin_url, object_id);

-- FIXME: not valid, because corrupt_object(id) is not unique
-- alter table object_origin add constraint object_origin_object_fkey foreign key (object_id) references corrupt_object(id) not valid;
-- alter table object_origin validate constraint object_origin_object_fkey;

-- fixed_object

create unique index concurrently fixed_object_pkey on fixed_object(id);
alter table fixed_object add primary key using index fixed_object_pkey;
