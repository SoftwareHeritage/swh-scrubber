-- SWH Scrubber DB schema upgrade
-- from_version: 3
-- to_version: 4
-- description: Add checked_range


create table checked_range
(
  datastore             int not null,
  range_start           swhid not null,
  range_end             swhid not null,
  last_date             timestamptz not null
);

comment on table checked_range is 'Each row represents a range of objects in a datastore that were fetched, checksummed, and checked at some point in the past.';
comment on column checked_range.range_start is 'First SWHID of the range that was checked (inclusive, possibly non-existent).';
comment on column checked_range.range_end is 'Last SWHID of the range that was checked (inclusive, possibly non-existent).';
comment on column checked_range.last_date is 'Date the last scrub of that range *started*.';

create unique index concurrently checked_range_pkey on checked_range(datastore, range_start, range_end);
alter table checked_range add primary key using index checked_range_pkey;
