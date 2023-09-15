# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Callable, Optional

import psycopg2

from swh.model.swhids import CoreSWHID

from .db import CorruptObject, ScrubberDb


def iter_corrupt_objects(
    db: ScrubberDb,
    start_object: CoreSWHID,
    end_object: CoreSWHID,
    origin_url: Optional[str],
    cb: Callable[[CorruptObject, psycopg2.extensions.cursor], None],
) -> None:
    """Fetches objects and calls ``cb`` on each of them.

    objects are fetched with an update lock, with the same transaction as ``cb``,
    which is automatically committed after ``cb`` runs."""
    while True:
        with db.conn, db.cursor() as cur:
            if origin_url:
                corrupt_objects = list(
                    db.corrupt_object_grab_by_origin(
                        cur, origin_url, start_object, end_object
                    )
                )
            else:
                corrupt_objects = list(
                    db.corrupt_object_grab_by_id(cur, start_object, end_object)
                )
            if corrupt_objects and corrupt_objects[0].id == start_object:
                # TODO: don't needlessly fetch duplicate objects
                del corrupt_objects[0]
            if not corrupt_objects:
                # Nothing more to do
                break
            for corrupt_object in corrupt_objects:
                cb(corrupt_object, cur)
            db.conn.commit()  # XXX: is this redundant with db.conn.__exit__?

        start_object = corrupt_objects[-1].id
