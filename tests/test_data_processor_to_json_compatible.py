import datetime
import math
import uuid
from collections import deque
from decimal import Decimal

import pytest
from bson import ObjectId

from zmongo_toolbag.data_processor import DataProcessor


class DummyModelDump:
    def __init__(self):
        self.value = ObjectId("65918f0678e24c0001f3e5b1")

    def model_dump(self):
        return {
            "kind": "model_dump",
            "value": self.value,
        }


class DummyDictModel:
    def __init__(self):
        self.when = datetime.datetime(2024, 1, 1, 12, 30, 0)

    def dict(self):
        return {
            "kind": "dict_model",
            "when": self.when,
        }


class DummyObject:
    def __init__(self):
        self.name = "Alice"
        self._private = "hidden"
        self.created = datetime.date(2024, 1, 2)


class CircularNode:
    def __init__(self, name):
        self.name = name
        self.other = None


def test_to_json_compatible_handles_primitives():
    assert DataProcessor.to_json_compatible(None) is None
    assert DataProcessor.to_json_compatible(True) is True
    assert DataProcessor.to_json_compatible(123) == 123
    assert DataProcessor.to_json_compatible("hello") == "hello"


def test_to_json_compatible_handles_float_edge_cases():
    assert DataProcessor.to_json_compatible(1.25) == 1.25
    assert DataProcessor.to_json_compatible(float("nan")) == "NaN"
    assert DataProcessor.to_json_compatible(float("inf")) == "Infinity"
    assert DataProcessor.to_json_compatible(float("-inf")) == "-Infinity"


def test_to_json_compatible_handles_objectid_uuid_decimal_and_datetime():
    oid = ObjectId("65918f0678e24c0001f3e5b1")
    uid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    dt = datetime.datetime(2024, 1, 1, 12, 0, 0)
    d = datetime.date(2024, 1, 2)
    t = datetime.time(14, 30, 15)

    assert DataProcessor.to_json_compatible(oid) == str(oid)
    assert DataProcessor.to_json_compatible(uid) == str(uid)
    assert DataProcessor.to_json_compatible(dt) == dt.isoformat()
    assert DataProcessor.to_json_compatible(d) == d.isoformat()
    assert DataProcessor.to_json_compatible(t) == t.isoformat()
    assert DataProcessor.to_json_compatible(Decimal("5")) == 5
    assert DataProcessor.to_json_compatible(Decimal("5.25")) == 5.25


def test_to_json_compatible_handles_bytes_variants():
    assert DataProcessor.to_json_compatible(b"hello") == "hello"
    assert DataProcessor.to_json_compatible(bytearray(b"world")) == "world"
    assert DataProcessor.to_json_compatible(memoryview(b"abc")) == "abc"

    binary = b"\xff\xfe\xfd"
    result = DataProcessor.to_json_compatible(binary)
    assert isinstance(result, dict)
    assert result["__type__"] == "bytes"
    assert result["encoding"] == "base64"
    assert isinstance(result["data"], str)
    assert result["data"]


def test_to_json_compatible_handles_exception_objects():
    exc = ValueError("bad value")
    result = DataProcessor.to_json_compatible(exc)

    assert result["__type__"] == "ValueError"
    assert result["message"] == "bad value"
    assert result["args"] == ["bad value"]


def test_to_json_compatible_handles_nested_mixed_structures():
    oid = ObjectId("65918f0678e24c0001f3e5b1")
    payload = {
        "id": oid,
        "items": [1, Decimal("2.5"), datetime.date(2024, 1, 1)],
        "tags": {"b", "a"},
        "queue": deque([b"x", b"y"]),
        "inner": {
            "uuid": uuid.UUID("12345678-1234-5678-1234-567812345678"),
        },
    }

    result = DataProcessor.to_json_compatible(payload)

    assert result["id"] == str(oid)
    assert result["items"] == [1, 2.5, "2024-01-01"]
    assert result["tags"] == ["a", "b"]
    assert result["queue"] == ["x", "y"]
    assert result["inner"]["uuid"] == "12345678-1234-5678-1234-567812345678"


def test_to_json_compatible_stringifies_non_string_dict_keys():
    payload = {
        1: "1",
        2: "2",
        None: "nothing",
        ObjectId("65918f0678e24c0001f3e5b1"): "oid-value",
    }

    result = DataProcessor.to_json_compatible(payload)

    assert result["1"] == "1"
    assert result["2"] == "2"
    assert result["None"] == "nothing"
    assert result["65918f0678e24c0001f3e5b1"] == "oid-value"


def test_to_json_compatible_handles_model_dump_objects():
    result = DataProcessor.to_json_compatible(DummyModelDump())

    assert result == {
        "kind": "model_dump",
        "value": "65918f0678e24c0001f3e5b1",
    }


def test_to_json_compatible_handles_dict_objects():
    result = DataProcessor.to_json_compatible(DummyDictModel())

    assert result == {
        "kind": "dict_model",
        "when": "2024-01-01T12:30:00",
    }


def test_to_json_compatible_handles_plain_objects_via___dict__():
    result = DataProcessor.to_json_compatible(DummyObject())

    assert result == {
        "name": "Alice",
        "created": "2024-01-02",
    }
    assert "_private" not in result


def test_to_json_compatible_handles_circular_references():
    a = CircularNode("a")
    b = CircularNode("b")
    a.other = b
    b.other = a

    result = DataProcessor.to_json_compatible(a)

    assert result["name"] == "a"
    assert result["other"]["name"] == "b"
    assert result["other"]["other"]["__circular_reference__"] == "CircularNode"


def test_to_json_compatible_respects_max_depth():
    nested = {"a": {"b": {"c": {"d": 1}}}}

    result = DataProcessor.to_json_compatible(nested, max_depth=2)

    assert result["a"]["b"]["c"] == {"__truncated__": "max_depth_exceeded:2"}


@pytest.mark.parametrize(
    "value, expected",
    [
        ((1, 2, 3), [1, 2, 3]),
        ({3, 1, 2}, [1, 2, 3]),
        (frozenset([2, 1]), [1, 2]),
        (deque([1, 2]), [1, 2]),
    ],
)
def test_to_json_compatible_handles_sequence_like_inputs(value, expected):
    assert DataProcessor.to_json_compatible(value) == expected