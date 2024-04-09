Software Heritage - Datastore Scrubber
======================================

Tools to periodically checks data integrity in ``swh-storage``, ``swh-objstorage``
and ``swh-journal``, reports errors, and (try to) fix them.


The Scrubber package is made of the following parts:


Checking
--------

Highly parallel processes continuously read objects from a data store,
compute checksums, and write any failure in a database, along with the data of
the corrupt object.

There is one "checker" for each datastore package: storage (postgresql and cassandra),
journal (kafka), and object storage (any backends).

The journal is "crawled" using its native streaming; others are crawled by range,
reusing swh-storage's backfiller utilities, and checkpointed from time to time
to the scrubber's database (in the ``checked_range`` table).

Storage
+++++++

For the storage checker, a checking configuration must be created before being
able to spawn a number of checkers.

A new configuration is created using the ``swh scrubber check init`` tool:

.. code-block:: console

   $ swh scrubber check init storage --object-type snapshot --nb-partitions 65536 --name chk-snp
   Created configuration chk-snp [2] for checking snapshot in datastore storage postgresql

.. note::

   A configuration file is expected, as for most ``swh`` tools.
   This file must have a ``scrubber`` section with the configuration of
   the scrubber database. For storage checking operations, this
   configuration file must also have a ``storage`` configuration section.
   See the `swh-storage documentation`_ for more details on this. A
   typical configuration file could look like:

   .. code-block:: yaml

      scrubber:
        cls: postgresql
        db: postgresql://localhost/postgres?host=/tmp/tmpk9b4wkb5&port=9824

      storage:
        cls: postgresql
        db: service=swh
        objstorage:
          cls: noop

.. note::

   The configuration section ``scrubber_db`` has been renamed as
   ``scrubber`` in ``swh-scrubber`` version 2.0.0

One (or more) checking worker can then be spawned by using the ``swh scrubber
check run`` command:

.. code-block:: console

   $ swh scrubber check run chk-snp
   [...]


Object storage
++++++++++++++

As with the storage checker, a checking configuration must be created before
being able to spawn a number of checkers.

A new configuration is created using the ``swh scrubber check init`` tool:

.. code-block:: console

   $ swh scrubber check init objstorage --object-type content --nb-partitions 65536 --name check-contents
   Created configuration check-contents [3] for checking content in datastore objstorage remote

.. note::

   A configuration file is expected, as for most ``swh`` tools.
   This file must have a ``scrubber`` section with the configuration of
   the scrubber database. For object storage checking operations, this
   configuration file must have:

   - a ``storage`` configuration section if content ids are read from it (default)
   - a ``journal`` configuration section if content ids are read from a kafka content
     topic (require to use flag ``--use-journal`` of the ``swh scrubber check run``
     command)
   - an ``objstorage`` configuration section targeting the object storage to check

   See the `swh-storage documentation`_, `swh-objstorage documentation`_ and
   `swh-journal documentation`_ for more details on this. A typical configuration
   file could look like:

   .. code-block:: yaml

      scrubber:
        cls: postgresql
        db: postgresql://localhost/postgres?host=/tmp/tmpk9b4wkb5&port=9824

      storage:
        cls: postgresql
        db: service=swh
        objstorage:
          cls: noop

      journal:
         cls: kafka
         brokers:
            - broker1.journal.softwareheritage.org:9093
            - broker2.journal.softwareheritage.org:9093
            - broker3.journal.softwareheritage.org:9093
            - broker4.journal.softwareheritage.org:9093
         group_id: swh.scrubber
         prefix: swh.journal.objects
         on_eof: stop

      objstorage:
        cls: remote
        url: https://objstorage.softwareheritage.org/

By default, an object storage checker detects missing and corrupted contents.
To disable detection of missing contents, use the ``--no-check-references``
option of the ``swh check init`` command.
To disable detection of corrupted contents, use the ``--no-check-hashes``
option of the ``swh check init`` command.

One (or more) checking worker can then be spawned by using the ``swh scrubber
check run`` command:

- if the content ids must be read from a storage instance

.. code-block:: console

   $ swh scrubber check run check-contents
   [...]

- if the content ids must be read from a kafka content topic of ``swh-journal``

.. code-block:: console

   $ swh scrubber check run check-contents --use-journal
   [...]

Journal
+++++++

As with the other checkers, a checking configuration must be created before being
able to spawn a number of checkers.

A new configuration is created using the ``swh scrubber check init`` tool:

.. code-block:: console

   $ swh scrubber check init journal --object-type directory --name check-dirs-journal
   Created configuration check-dirs-journal [4] for checking directory in datastore journal kafka

.. note::

   A configuration file is expected, as for most ``swh`` tools.
   This file must have a ``scrubber`` section with the configuration of
   the scrubber database. For journal checking operations, this
   configuration file must also have a ``journal`` configuration section.

   See the `swh-journal documentation`_ for more details on this.
   A typical configuration file could look like:

   .. code-block:: yaml

      scrubber:
        cls: postgresql
        db: postgresql://localhost/postgres?host=/tmp/tmpk9b4wkb5&port=9824

      journal:
         cls: kafka
         brokers:
            - broker1.journal.softwareheritage.org:9093
            - broker2.journal.softwareheritage.org:9093
            - broker3.journal.softwareheritage.org:9093
            - broker4.journal.softwareheritage.org:9093
         group_id: swh.scrubber
         prefix: swh.journal.objects
         on_eof: stop

One (or more) checking worker can then be spawned by using the ``swh scrubber
check run`` command:

.. code-block:: console

   $ swh scrubber check run check-dirs-journal
   [...]

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

.. _`swh-storage documentation`: https://docs.softwareheritage.org/devel/swh-storage/index.html
.. _`swh-objstorage documentation`: https://docs.softwareheritage.org/devel/swh-objstorage/index.html
.. _`swh-journal documentation`: https://docs.softwareheritage.org/devel/swh-journal/index.html
