import os
import sys
import unittest
import datetime
import uuid
from collections import deque
from decimal import Decimal

from bson import ObjectId

# Match project import behavior
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

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


class TestDataProcessorToJsonCompatible(unittest.TestCase):
    def test_to_json_compatible_handles_primitives(self):
        self.assertIsNone(DataProcessor.to_json_compatible(None))
        self.assertIs(DataProcessor.to_json_compatible(True), True)
        self.assertEqual(DataProcessor.to_json_compatible(123), 123)
        self.assertEqual(DataProcessor.to_json_compatible("hello"), "hello")

    def test_to_json_compatible_handles_float_edge_cases(self):
        self.assertEqual(DataProcessor.to_json_compatible(1.25), 1.25)
        self.assertEqual(DataProcessor.to_json_compatible(float("nan")), "NaN")
        self.assertEqual(DataProcessor.to_json_compatible(float("inf")), "Infinity")
        self.assertEqual(DataProcessor.to_json_compatible(float("-inf")), "-Infinity")

    def test_to_json_compatible_handles_objectid_uuid_decimal_and_datetime(self):
        oid = ObjectId("65918f0678e24c0001f3e5b1")
        uid = uuid.UUID("12345678-1234-5678-1234-567812345678")
        dt = datetime.datetime(2024, 1, 1, 12, 0, 0)
        d = datetime.date(2024, 1, 2)
        t = datetime.time(14, 30, 15)

        self.assertEqual(DataProcessor.to_json_compatible(oid), str(oid))
        self.assertEqual(DataProcessor.to_json_compatible(uid), str(uid))
        self.assertEqual(DataProcessor.to_json_compatible(dt), dt.isoformat())
        self.assertEqual(DataProcessor.to_json_compatible(d), d.isoformat())
        self.assertEqual(DataProcessor.to_json_compatible(t), t.isoformat())
        self.assertEqual(DataProcessor.to_json_compatible(Decimal("5")), 5)
        self.assertEqual(DataProcessor.to_json_compatible(Decimal("5.25")), 5.25)

    def test_to_json_compatible_handles_bytes_variants(self):
        self.assertEqual(DataProcessor.to_json_compatible(b"hello"), "hello")
        self.assertEqual(DataProcessor.to_json_compatible(bytearray(b"world")), "world")
        self.assertEqual(DataProcessor.to_json_compatible(memoryview(b"abc")), "abc")

        binary = b"\xff\xfe\xfd"
        result = DataProcessor.to_json_compatible(binary)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["__type__"], "bytes")
        self.assertEqual(result["encoding"], "base64")
        self.assertIsInstance(result["data"], str)
        self.assertTrue(result["data"])

    def test_to_json_compatible_handles_exception_objects(self):
        exc = ValueError("bad value")
        result = DataProcessor.to_json_compatible(exc)

        self.assertEqual(result["__type__"], "ValueError")
        self.assertEqual(result["message"], "bad value")
        self.assertEqual(result["args"], ["bad value"])

    def test_to_json_compatible_handles_nested_mixed_structures(self):
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

        self.assertEqual(result["id"], str(oid))
        self.assertEqual(result["items"], [1, 2.5, "2024-01-01"])
        self.assertEqual(result["tags"], ["a", "b"])
        self.assertEqual(result["queue"], ["x", "y"])
        self.assertEqual(result["inner"]["uuid"], "12345678-1234-5678-1234-567812345678")

    def test_to_json_compatible_stringifies_non_string_dict_keys(self):
        payload = {
            1: "1",
            2: "2",
            None: "nothing",
            ObjectId("65918f0678e24c0001f3e5b1"): "oid-value",
        }

        result = DataProcessor.to_json_compatible(payload)

        self.assertEqual(result["1"], "1")
        self.assertEqual(result["2"], "2")
        self.assertEqual(result["None"], "nothing")
        self.assertEqual(result["65918f0678e24c0001f3e5b1"], "oid-value")

    def test_to_json_compatible_handles_model_dump_objects(self):
        result = DataProcessor.to_json_compatible(DummyModelDump())

        self.assertEqual(
            result,
            {
                "kind": "model_dump",
                "value": "65918f0678e24c0001f3e5b1",
            },
        )

    def test_to_json_compatible_handles_dict_objects(self):
        result = DataProcessor.to_json_compatible(DummyDictModel())

        self.assertEqual(
            result,
            {
                "kind": "dict_model",
                "when": "2024-01-01T12:30:00",
            },
        )

    def test_to_json_compatible_handles_plain_objects_via___dict__(self):
        result = DataProcessor.to_json_compatible(DummyObject())

        self.assertEqual(
            result,
            {
                "name": "Alice",
                "created": "2024-01-02",
            },
        )
        self.assertNotIn("_private", result)

    def test_to_json_compatible_handles_circular_references(self):
        a = CircularNode("a")
        b = CircularNode("b")
        a.other = b
        b.other = a

        result = DataProcessor.to_json_compatible(a)

        self.assertEqual(result["name"], "a")
        self.assertEqual(result["other"]["name"], "b")
        self.assertEqual(result["other"]["other"]["__circular_reference__"], "CircularNode")

    def test_to_json_compatible_respects_max_depth(self):
        nested = {"a": {"b": {"c": {"d": 1}}}}

        result = DataProcessor.to_json_compatible(nested, max_depth=2)

        self.assertEqual(result["a"]["b"]["c"], {"__truncated__": "max_depth_exceeded:2"})

    def test_to_json_compatible_handles_sequence_like_inputs(self):
        cases = [
            ((1, 2, 3), [1, 2, 3]),
            ({3, 1, 2}, [1, 2, 3]),
            (frozenset([2, 1]), [1, 2]),
            (deque([1, 2]), [1, 2]),
        ]

        for value, expected in cases:
            with self.subTest(value=value, expected=expected):
                self.assertEqual(DataProcessor.to_json_compatible(value), expected)


if __name__ == "__main__":
    unittest.main()