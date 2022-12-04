
from enum import Enum
from deepdiff import DeepDiff


class DiffType(Enum):
    ROUND_TO_NEAREST_INTEGER = 0
    ONE_DECIMAL_PLACE_PRECISION = 1
    TWO_DECIMAL_PLACE_PRECISION = 2


def compare(avro1, avro2, diff_type=DiffType(DiffType.ROUND_TO_NEAREST_INTEGER)):
    return DeepDiff(avro1, avro2, significant_digits=diff_type.value, ignore_order=True)
