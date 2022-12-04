
from diff import compare, DiffType


def test_empty_are_equal():
    assert {} == compare({}, {})


def test_not_equal():
    assert compare({'value': 0}, {'value': 1}) == {'values_changed': {"root['value']": {'new_value': 1, 'old_value': 0}}}


def test_approximately_equal_ignore_fraction():
    assert compare({'value': 1.52}, {'value': 1.57}) == {}


def test_one_significant_figure_difference():
    assert compare({'value': 1.52}, {'value': 1.57}, DiffType.ONE_DECIMAL_PLACE_PRECISION) == {'values_changed': {"root['value']": {'new_value': 1.57, 'old_value': 1.52}}}


def test_order_does_not_matter():
    assert compare([1, 2, 3], [3, 2, 1]) == {}