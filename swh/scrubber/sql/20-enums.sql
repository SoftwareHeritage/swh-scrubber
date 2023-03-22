create type datastore_type as enum ('storage', 'journal', 'objstorage');
create type object_type as enum ('content', 'directory', 'revision', 'release', 'snapshot', 'extid', 'raw_extrinsic_metadata');
