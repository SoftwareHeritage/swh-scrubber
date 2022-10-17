# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from swh.model import swhids
from swh.scrubber.db import Datastore, ScrubberDb

DATASTORE = Datastore(package="storage", cls="postgresql", instance="service=swh-test")
SNP_SWHID1 = swhids.CoreSWHID.from_string(
    "swh:1:snp:5000000000000000000000000000000000000000"
)
SNP_SWHID2 = swhids.CoreSWHID.from_string(
    "swh:1:snp:e000000000000000000000000000000000000000"
)
DATE = datetime.datetime(2022, 10, 4, 12, 1, 23, tzinfo=datetime.timezone.utc)


def test_checked_range_insert(scrubber_db: ScrubberDb):
    scrubber_db.checked_range_upsert(DATASTORE, SNP_SWHID1, SNP_SWHID2, DATE)

    assert list(scrubber_db.checked_range_iter(DATASTORE)) == [
        (SNP_SWHID1, SNP_SWHID2, DATE)
    ]


def test_checked_range_update(scrubber_db: ScrubberDb):
    scrubber_db.checked_range_upsert(DATASTORE, SNP_SWHID1, SNP_SWHID2, DATE)

    date2 = DATE + datetime.timedelta(days=1)
    scrubber_db.checked_range_upsert(DATASTORE, SNP_SWHID1, SNP_SWHID2, date2)

    assert list(scrubber_db.checked_range_iter(DATASTORE)) == [
        (SNP_SWHID1, SNP_SWHID2, date2)
    ]

    date3 = DATE + datetime.timedelta(days=-1)
    scrubber_db.checked_range_upsert(DATASTORE, SNP_SWHID1, SNP_SWHID2, date3)

    assert list(scrubber_db.checked_range_iter(DATASTORE)) == [
        (SNP_SWHID1, SNP_SWHID2, date2)  # newest date wins
    ]


def test_checked_range_get(scrubber_db: ScrubberDb):
    assert (
        scrubber_db.checked_range_get_last_date(DATASTORE, SNP_SWHID1, SNP_SWHID2)
        is None
    )

    scrubber_db.checked_range_upsert(DATASTORE, SNP_SWHID1, SNP_SWHID2, DATE)

    assert (
        scrubber_db.checked_range_get_last_date(DATASTORE, SNP_SWHID1, SNP_SWHID2)
        == DATE
    )
