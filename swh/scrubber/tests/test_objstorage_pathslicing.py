# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from .objstorage_checker_tests import *  # noqa

# Use postgreql storage and an objstorage with pathslicing backend to run
# the tests


def swh_objstorage_config(tmpdir):
    return {
        "cls": "pathslicing",
        "root": str(tmpdir),
        "slicing": "0:2/2:4/4:6",
        "compression": "gzip",
    }
