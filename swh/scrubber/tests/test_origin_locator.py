# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from unittest.mock import MagicMock

import pytest

from swh.graph.http_naive_client import NaiveClient as NaiveGraphClient
from swh.model.model import Origin
from swh.model.swhids import CoreSWHID
from swh.scrubber.origin_locator import OriginLocator


@pytest.mark.parametrize("insert", [False, True])
def test_no_objects(scrubber_db, corrupt_object, insert):
    if insert:
        scrubber_db.corrupt_object_add(
            corrupt_object.id, corrupt_object.config, corrupt_object.object_
        )

    graph = MagicMock()
    storage = MagicMock()
    locator = OriginLocator(
        db=scrubber_db,
        graph=graph,
        storage=storage,
        # this range does not contain the object above
        start_object=CoreSWHID.from_string("swh:1:dir:00" + "00" * 19),
        end_object=CoreSWHID.from_string("swh:1:dir:60" + "00" * 19),
    )

    locator.run()

    assert graph.method_calls == []
    assert storage.method_calls == []

    with scrubber_db.conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM object_origin")
        assert cur.fetchone() == (0,)


def test_object_not_in_graph(scrubber_db, corrupt_object):
    scrubber_db.corrupt_object_add(
        corrupt_object.id, corrupt_object.config, corrupt_object.object_
    )

    graph = NaiveGraphClient(nodes=[], edges=[])
    storage = MagicMock()
    locator = OriginLocator(
        db=scrubber_db,
        graph=graph,
        storage=storage,
        start_object=CoreSWHID.from_string("swh:1:dir:" + "00" * 20),
        end_object=CoreSWHID.from_string("swh:1:dir:" + "00" * 20),
    )

    locator.run()

    assert storage.method_calls == []

    with scrubber_db.conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM object_origin")
        assert cur.fetchone() == (0,)


def test_origin_not_in_storage(scrubber_db, swh_storage, corrupt_object, caplog):
    scrubber_db.corrupt_object_add(
        corrupt_object.id, corrupt_object.config, corrupt_object.object_
    )

    origin = Origin(url="http://example.org")

    graph = NaiveGraphClient(
        nodes=[corrupt_object.id, origin.swhid()],
        edges=[(origin.swhid(), corrupt_object.id)],
    )
    locator = OriginLocator(
        db=scrubber_db,
        graph=graph,
        storage=swh_storage,
        start_object=CoreSWHID.from_string("swh:1:dir:" + "00" * 20),
        end_object=CoreSWHID.from_string("swh:1:dir:" + "ff" * 20),
    )

    with caplog.at_level(logging.ERROR, logger="swh.scrubber.origin_locator"):
        locator.run()

    with scrubber_db.conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM object_origin")
        assert cur.fetchone() == (0,)

    assert any(
        f"{origin.swhid()} found in graph but missing" in record[2]
        for record in caplog.record_tuples
    )


def test_two_origins(scrubber_db, corrupt_object, swh_storage):
    scrubber_db.corrupt_object_add(
        corrupt_object.id, corrupt_object.config, corrupt_object.object_
    )

    origin1 = Origin(url="http://example.org")
    origin2 = Origin(url="http://example.com")
    swh_storage.origin_add([origin1, origin2])

    graph = NaiveGraphClient(
        nodes=[corrupt_object.id, origin1.swhid(), origin2.swhid()],
        edges=[
            (origin1.swhid(), corrupt_object.id),
            (origin2.swhid(), corrupt_object.id),
        ],
    )
    locator = OriginLocator(
        db=scrubber_db,
        graph=graph,
        storage=swh_storage,
        start_object=CoreSWHID.from_string("swh:1:dir:" + "00" * 20),
        end_object=CoreSWHID.from_string("swh:1:dir:" + "ff" * 20),
    )

    locator.run()

    with scrubber_db.conn.cursor() as cur:
        cur.execute("SELECT object_id, origin_url FROM object_origin")
        assert set(cur) == {
            (str(corrupt_object.id), origin1.url),
            (str(corrupt_object.id), origin2.url),
        }


def test_many_origins(scrubber_db, corrupt_object, swh_storage):
    scrubber_db.corrupt_object_add(
        corrupt_object.id, corrupt_object.config, corrupt_object.object_
    )

    origins = [Origin(url=f"http://example.org/{i}") for i in range(1000)]
    swh_storage.origin_add(origins)

    graph = NaiveGraphClient(
        nodes=[corrupt_object.id] + [origin.swhid() for origin in origins],
        edges=[(origin.swhid(), corrupt_object.id) for origin in origins],
    )
    locator = OriginLocator(
        db=scrubber_db,
        graph=graph,
        storage=swh_storage,
        start_object=CoreSWHID.from_string("swh:1:dir:" + "00" * 20),
        end_object=CoreSWHID.from_string("swh:1:dir:" + "ff" * 20),
    )

    locator.run()

    with scrubber_db.conn.cursor() as cur:
        cur.execute("SELECT object_id, origin_url FROM object_origin")
        rows = set(cur)
        assert rows <= {(str(corrupt_object.id), origin.url) for origin in origins}
        assert len(rows) == 100
