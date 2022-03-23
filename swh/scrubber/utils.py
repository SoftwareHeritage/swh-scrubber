# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Callable

import psycopg2

from swh.model.swhids import CoreSWHID

from .db import CorruptObject, ScrubberDb


def iter_corrupt_objects(
    db: ScrubberDb,
    start_object: CoreSWHID,
    end_object: CoreSWHID,
    cb: Callable[[CorruptObject, psycopg2.extensions.cursor], None],
) -> None:
    while True:
        with db.conn, db.cursor() as cur:
            corrupt_objects = db.corrupt_object_grab(cur, start_object, end_object,)
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
