from pytest import fixture

from hedgineer.collect import generate_security_master
from hedgineer.globals import ATTRIBUTE_PRIORITY, TEST_AUDIT_TRAIL_2
from hedgineer.io import to_arrow


@fixture
def sm():
    return generate_security_master(TEST_AUDIT_TRAIL_2, ATTRIBUTE_PRIORITY)


@fixture
def sm_arrow(sm):
    return to_arrow(sm)


def test_case_1(sm):
    pass
