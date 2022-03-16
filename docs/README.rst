Software Heritage - Datastore Scrubber
======================================

Tools to periodically checks data integrity in swh-storage and swh-objstorage,
reports errors, and (try to) fix them.

This is a work in progress; some of the components described below do not
exist yet (cassandra storage checker, objstorage checker, recovery, and reinjection)

The Scrubber package is made of the following parts:


Checking
--------

Highly parallel processes continuously read objects from a data store,
compute checksums, and write any failure in a database, along with the data of
the corrupt object.

There is one "checker" for each datastore package: storage (postgresql and cassandra),
journal (kafka), and objstorage.


Recovery
--------

Then, from time to time, jobs go through the list of known corrupt objects,
and try to recover the original objects, through various means:

* Brute-forcing variations until they match their checksum
* Recovering from another data store
* As a last resort, recovering from known origins, if any


Reinjection
-----------

Finally, when an original object is recovered, it is reinjected in the original
data store, replacing the corrupt one.
