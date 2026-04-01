# tests/test_safe_result.py
import asyncio
import json
from typing import Any

import pytest
from quart import Quart, jsonify

from zmongo_toolbag.data_processor import DataProcessor
from zmongo_toolbag.safe_result import SafeResult


# Adjust this import to your real module path if needed.



@pytest.fixture
def app():
    return Quart(__name__)


@pytest.fixture(autouse=True)
def patch_to_json_compatible(monkeypatch):
    """
    Patch the exact DataProcessor object used by the imported safe_result module.
    """

    def fake_to_json_compatible(value: Any):
        if isinstance(value, dict):
            return {k: fake_to_json_compatible(v) for k, v in value.items()}
        if isinstance(value, list):
            return [fake_to_json_compatible(v) for v in value]
        if isinstance(value, tuple):
            return [fake_to_json_compatible(v) for v in value]
        if isinstance(value, set):
            return [fake_to_json_compatible(v) for v in sorted(value, key=str)]
        if isinstance(value, Exception):
            return str(value)
        if hasattr(value, "__class__") and value.__class__.__name__ == "DummyObjectId":
            return f"oid:{value.value}"
        return value

    monkeypatch.setattr(
        DataProcessor,
        "to_json_compatible",
        staticmethod(fake_to_json_compatible),
    )


class DummyObjectId:
    def __init__(self, value: str):
        self.value = value


def test_ok_preserves_raw_data_and_json_safe_data():
    raw = {"_id": DummyObjectId("abc123"), "name": "john"}

    result = SafeResult.ok(raw)

    assert result.success is True
    assert result._raw_data is raw
    assert result.original() == raw
    assert result.data == {"_id": "oid:abc123", "name": "john"}
    assert result.message == "OK"
    assert result.status_code == 200


def test_fail_preserves_data_and_stringifies_exception_error():
    exc = ValueError("bad things")
    payload = {"a": 1}

    result = SafeResult.fail(exc, payload, status_code=422)

    assert result.success is False
    assert result.data == {"a": 1}
    assert result.original() == payload
    assert result.error == "bad things"
    assert result.message == "bad things"
    assert result.status_code == 422


def test_fail_allows_explicit_message_override():
    result = SafeResult.fail("raw error", {"x": 1}, message="Custom failure", status_code=409)

    assert result.success is False
    assert result.error == "raw error"
    assert result.message == "Custom failure"
    assert result.status_code == 409
    assert result.data == {"x": 1}


def test_to_dict_and_model_dump_match():
    result = SafeResult.ok({"x": 1}, message="done", status_code=201)

    expected = {
        "success": True,
        "data": {"x": 1},
        "message": "done",
        "error": None,
        "status_code": 201,
    }

    assert result.to_dict() == expected
    assert result.model_dump() == expected


def test_to_json_serializes_payload():
    result = SafeResult.ok({"x": 1}, message="done", status_code=201)

    parsed = json.loads(result.to_json())

    assert parsed["success"] is True
    assert parsed["data"] == {"x": 1}
    assert parsed["message"] == "done"
    assert parsed["status_code"] == 201


def test_bool_and_repr():
    ok_result = SafeResult.ok({"x": 1})
    fail_result = SafeResult.fail("nope")

    assert bool(ok_result) is True
    assert bool(fail_result) is False

    text = repr(ok_result)
    assert "SafeResult(success=True" in text
    assert "data={'x': 1}" in text
    assert "message=OK" in text


def test_original_applies_keymap_shallow():
    raw = {
        "old_name": "Alice",
        "__keymap": {
            "old_name": "new_name",
        },
    }

    result = SafeResult.ok(raw)

    assert result.original() == {"new_name": "Alice"}


def test_original_applies_keymap_recursively():
    raw = {
        "child": {
            "legacy": 123,
            "__keymap": {
                "legacy": "modern"
            },
        },
        "items": [
            {
                "old": "x",
                "__keymap": {"old": "new"},
            }
        ],
    }

    result = SafeResult.ok(raw)

    assert result.original() == {
        "child": {"modern": 123},
        "items": [{"new": "x"}],
    }


def test_to_response_returns_quart_json_response(app):
    async def runner():
        result = SafeResult.ok({"x": 1}, message="done", status_code=202)

        async with app.app_context():
            response, status = result.to_response()

            assert status == 202
            body = await response.get_json()
            assert body == {
                "success": True,
                "data": {"x": 1},
                "message": "done",
                "error": None,
                "status_code": 202,
            }

    asyncio.run(runner())


def test_to_response_override_status(app):
    async def runner():
        result = SafeResult.ok({"x": 1}, status_code=202)

        async with app.app_context():
            response, status = result.to_response(override_status=299)

            assert status == 299
            body = await response.get_json()
            assert body["status_code"] == 202
            assert body["success"] is True

    asyncio.run(runner())


def test_from_quart_response_with_standard_success_payload(app):
    async def runner():
        async with app.app_context():
            response = jsonify({
                "success": True,
                "data": {"x": 1},
                "message": "done",
                "error": None,
                "status_code": 207,
            })
            response.status_code = 207

            result = await SafeResult.from_quart_response(response)

        assert result.success is True
        assert result.data == {"x": 1}
        assert result.message == "done"
        assert result.error is None
        assert result.status_code == 207

    asyncio.run(runner())


def test_from_quart_response_with_tuple_input(app):
    async def runner():
        async with app.app_context():
            response = jsonify({
                "success": False,
                "data": {"x": 1},
                "message": "bad",
                "error": "boom",
                "status_code": 409,
            })

            result = await SafeResult.from_quart_response((response, 409))

        assert result.success is False
        assert result.data == {"x": 1}
        assert result.message == "bad"
        assert result.error == "boom"
        assert result.status_code == 409

    asyncio.run(runner())


def test_from_quart_response_with_non_safe_result_json(app):
    async def runner():
        async with app.app_context():
            response = jsonify({"plain": "payload"})
            response.status_code = 203

            result = await SafeResult.from_quart_response(response)

        assert result.success is True
        assert result.data == {"plain": "payload"}
        assert result.message == "Parsed response (203)"
        assert result.status_code == 203

    asyncio.run(runner())


def test_from_quart_response_uses_http_code_when_payload_has_no_status(app):
    async def runner():
        async with app.app_context():
            response = jsonify({"plain": "payload"})
            response.status_code = 503

            result = await SafeResult.from_quart_response(response)

        assert result.success is False
        assert result.status_code == 503
        assert result.message == "Parsed response (503)"

    asyncio.run(runner())


class BrokenResponse:
    status_code = 500

    async def get_data(self, as_text=True):
        return "{not valid json"


def test_from_quart_response_invalid_json_returns_failure_result():
    async def runner():
        result = await SafeResult.from_quart_response(BrokenResponse())

        assert result.success is False
        assert result.status_code == 500
        assert result.message == "Response parsing failed"
        assert result.error is not None

    asyncio.run(runner())


def test_log_returns_self(caplog):
    result = SafeResult.ok({"x": 1}, message="logged")

    with caplog.at_level("INFO"):
        returned = result.log(prefix="PREFIX")

    assert returned is result
    assert "PREFIX [200] logged" in caplog.text


def test_ensure_success_path():
    result = SafeResult.ensure(False, data={"x": 1}, message="OK", status_code=201)

    assert result.success is False
    assert result.data == {"x": 1}
    assert result.message == "OK"
    assert result.status_code == 201


def test_ensure_failure_path():
    result = SafeResult.ensure(False, message="Condition failed", data={"x": 1}, status_code=418)

    assert result.success is False
    assert result.error == "Condition failed"
    assert result.message == "Condition failed"
    assert result.data == {"x": 1}
    assert result.status_code == 418