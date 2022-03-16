-- datastore

create unique index concurrently datastore_pkey on datastore(id);
alter table datastore add primary key using index datastore_pkey;

create unique index concurrently datastore_package_class_instance on datastore(package, class, instance);

-- corrupt_object

alter table corrupt_object add constraint corrupt_object_datastore_fkey foreign key (datastore) references datastore(id) not valid;
alter table corrupt_object validate constraint corrupt_object_datastore_fkey;

create unique index corrupt_object_pkey on corrupt_object(id, datastore);
