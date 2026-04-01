import os
import time
from pathlib import Path

import pytest
from bson import ObjectId
from dotenv import load_dotenv

from zmongo_toolbag.zmongo import ZMongo

# Load test env first if present, then fallback to standard env
TEST_ENV = Path('.env.tests')
if TEST_ENV.exists():
    load_dotenv(TEST_ENV)
else:
    load_dotenv()


pytestmark = pytest.mark.integration


def _unique_db_name() -> str:
    return f"zmongo_integration_{int(time.time() * 1000)}_{os.getpid()}"


@pytest.fixture(scope="session")
def mongo_uri() -> str:
    return os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")


@pytest.fixture()
def zmongo_instance(mongo_uri: str):
    db_name = _unique_db_name()
    zm = ZMongo(
        uri=mongo_uri,
        db_name=db_name,
        cache_enabled=True,
        cache_ttl_seconds=60,
        run_sync_timeout_seconds=30,
    )
    try:
        ping_result = zm.ping()
        assert ping_result.success, f"Mongo ping failed: {ping_result.to_dict()}"
        yield zm
    finally:
        try:
            if not zm._closed:
                zm.drop_database()
        finally:
            zm.close()


@pytest.fixture()
def sample_coll() -> str:
    return "users"


def _extract_document(result):
    assert result.success, result.to_dict()
    return result.data["document"]


def _extract_documents(result):
    assert result.success, result.to_dict()
    return result.data["documents"]


def test_ping_real_mongo(zmongo_instance: ZMongo):
    result = zmongo_instance.ping()

    assert result.success, result.to_dict()
    assert result.data["ok"] is True
    assert result.data["database"] == zmongo_instance.db_name
    assert isinstance(result.data["latency_seconds"], float)


def test_insert_one_and_find_one_by_object_id_string(zmongo_instance: ZMongo, sample_coll: str):
    insert_result = zmongo_instance.insert_one(sample_coll, {"name": "Alice", "role": "admin"})
    assert insert_result.success, insert_result.to_dict()

    inserted_id = insert_result.data["inserted_id"]
    assert inserted_id is not None

    find_result = zmongo_instance.find_one(sample_coll, {"_id": str(inserted_id)})
    doc = _extract_document(find_result)

    assert doc is not None
    assert doc["_id"] == inserted_id
    assert doc["name"] == "Alice"
    assert doc["role"] == "admin"
    assert find_result.data["cache_hit"] is False


def test_find_one_cache_hit_and_invalidation_after_update(zmongo_instance: ZMongo, sample_coll: str):
    insert_result = zmongo_instance.insert_one(sample_coll, {"name": "Bob", "status": "new"})
    inserted_id = insert_result.data["inserted_id"]
    query = {"_id": str(inserted_id)}

    first = zmongo_instance.find_one(sample_coll, query, cache=True)
    second = zmongo_instance.find_one(sample_coll, query, cache=True)

    first_doc = _extract_document(first)
    second_doc = _extract_document(second)

    assert first.data["cache_hit"] is False
    assert second.data["cache_hit"] is True
    assert first_doc["status"] == "new"
    assert second_doc["status"] == "new"

    update_result = zmongo_instance.update_one(sample_coll, query, {"status": "updated"})
    assert update_result.success, update_result.to_dict()
    assert update_result.data["matched_count"] == 1

    third = zmongo_instance.find_one(sample_coll, query, cache=True)
    third_doc = _extract_document(third)

    assert third.data["cache_hit"] is False
    assert third_doc["status"] == "updated"


def test_find_many_cache_hit_and_count(zmongo_instance: ZMongo, sample_coll: str):
    docs = [
        {"name": "u1", "team": "red", "rank": 2},
        {"name": "u2", "team": "red", "rank": 1},
        {"name": "u3", "team": "blue", "rank": 3},
    ]
    insert_result = zmongo_instance.insert_many(sample_coll, docs)
    assert insert_result.success, insert_result.to_dict()
    assert insert_result.data["inserted_count"] == 3

    first = zmongo_instance.find_many(
        sample_coll,
        {"team": "red"},
        sort=[("rank", 1)],
        limit=10,
        cache=True,
    )
    second = zmongo_instance.find_many(
        sample_coll,
        {"team": "red"},
        sort=[("rank", 1)],
        limit=10,
        cache=True,
    )

    first_docs = _extract_documents(first)
    second_docs = _extract_documents(second)

    assert first.data["cache_hit"] is False
    assert second.data["cache_hit"] is True
    assert first.data["count"] == 2
    assert [d["name"] for d in first_docs] == ["u2", "u1"]
    assert [d["name"] for d in second_docs] == ["u2", "u1"]


def test_count_documents_cache_and_invalidation(zmongo_instance: ZMongo, sample_coll: str):
    zmongo_instance.insert_many(
        sample_coll,
        [
            {"group": "A"},
            {"group": "A"},
            {"group": "B"},
        ],
    )

    first = zmongo_instance.count_documents(sample_coll, {"group": "A"}, cache=True)
    second = zmongo_instance.count_documents(sample_coll, {"group": "A"}, cache=True)

    assert first.success, first.to_dict()
    assert second.success, second.to_dict()
    assert first.data["count"] == 2
    assert first.data["cache_hit"] is False
    assert second.data["cache_hit"] is True

    zmongo_instance.insert_one(sample_coll, {"group": "A"})
    third = zmongo_instance.count_documents(sample_coll, {"group": "A"}, cache=True)

    assert third.success, third.to_dict()
    assert third.data["count"] == 3
    assert third.data["cache_hit"] is False


def test_update_many_real(zmongo_instance: ZMongo, sample_coll: str):
    zmongo_instance.insert_many(
        sample_coll,
        [
            {"segment": "trial", "active": False},
            {"segment": "trial", "active": False},
            {"segment": "paid", "active": False},
        ],
    )

    result = zmongo_instance.update_many(sample_coll, {"segment": "trial"}, {"active": True})

    assert result.success, result.to_dict()
    assert result.data["matched_count"] == 2
    assert result.data["modified_count"] == 2

    verify = zmongo_instance.find_many(sample_coll, {"segment": "trial"}, sort=[("_id", 1)])
    docs = _extract_documents(verify)
    assert len(docs) == 2
    assert all(d["active"] is True for d in docs)


def test_delete_one_and_delete_many(zmongo_instance: ZMongo, sample_coll: str):
    insert_result = zmongo_instance.insert_many(
        sample_coll,
        [
            {"kind": "temp", "n": 1},
            {"kind": "temp", "n": 2},
            {"kind": "keep", "n": 3},
        ],
    )
    inserted_ids = insert_result.data["inserted_ids"]

    delete_one_result = zmongo_instance.delete_one(sample_coll, {"_id": str(inserted_ids[0])})
    assert delete_one_result.success, delete_one_result.to_dict()
    assert delete_one_result.data["deleted_count"] == 1

    delete_many_result = zmongo_instance.delete_many(sample_coll, {"kind": "temp"})
    assert delete_many_result.success, delete_many_result.to_dict()
    assert delete_many_result.data["deleted_count"] == 1

    remaining = zmongo_instance.find_many(sample_coll, {}, sort=[("n", 1)])
    docs = _extract_documents(remaining)
    assert [d["kind"] for d in docs] == ["keep"]


def test_insert_or_update_existing_and_upsert(zmongo_instance: ZMongo, sample_coll: str):
    inserted = zmongo_instance.insert_one(sample_coll, {"username": "jane", "score": 10})
    inserted_id = inserted.data["inserted_id"]

    update_existing = zmongo_instance.insert_or_update(
        sample_coll,
        {"_id": inserted_id},
        {"score": 20, "level": 3},
    )
    assert update_existing.success, update_existing.to_dict()
    assert update_existing.data["modified_count"] == 1

    fetched = zmongo_instance.find_one(sample_coll, {"_id": str(inserted_id)})
    doc = _extract_document(fetched)
    assert doc["score"] == 20
    assert doc["level"] == 3

    upsert_new = zmongo_instance.insert_or_update(
        sample_coll,
        {"username": "new_user"},
        {"score": 5},
    )
    assert upsert_new.success, upsert_new.to_dict()
    assert upsert_new.data["upserted_id"] is not None

    new_doc = zmongo_instance.find_one(sample_coll, {"username": "new_user"})
    found = _extract_document(new_doc)
    assert found["score"] == 5


def test_save_value_field_path_update_and_json_parse(zmongo_instance: ZMongo, sample_coll: str):
    inserted = zmongo_instance.insert_one(sample_coll, {"title": "Doc 1"})
    inserted_id = inserted.data["inserted_id"]

    result = zmongo_instance.save_value(
        sample_coll,
        '{"summary": "hello", "tokens": 12}',
        query={"_id": str(inserted_id)},
        field_path="responses.analysis",
        parse_json_strings=True,
    )

    assert result.success, result.to_dict()
    assert result.data["operation"] == "updated_existing"

    fetched = zmongo_instance.find_one(sample_coll, {"_id": str(inserted_id)})
    doc = _extract_document(fetched)

    assert doc["responses"]["analysis"]["summary"] == "hello"
    assert doc["responses"]["analysis"]["tokens"] == 12


def test_save_value_insert_new_without_query(zmongo_instance: ZMongo, sample_coll: str):
    result = zmongo_instance.save_value(
        sample_coll,
        {"name": "CreatedViaSave", "kind": "generated"},
        upsert=True,
    )

    assert result.success, result.to_dict()
    assert result.data["operation"] == "inserted_new"
    inserted_id = result.data["inserted_id"]
    assert inserted_id is not None

    fetched = zmongo_instance.find_one(sample_coll, {"_id": str(inserted_id)})
    doc = _extract_document(fetched)
    assert doc["name"] == "CreatedViaSave"
    assert doc["kind"] == "generated"


def test_save_value_rejects_missing_query_when_upsert_false(zmongo_instance: ZMongo, sample_coll: str):
    result = zmongo_instance.save_value(sample_coll, {"x": 1}, upsert=False)

    assert result.success is False
    assert result.status_code == 400
    assert "Cannot determine target document" in result.message


def test_list_collections_and_drop_database(zmongo_instance: ZMongo, sample_coll: str):
    zmongo_instance.insert_one(sample_coll, {"x": 1})
    collections = zmongo_instance.list_collections()

    assert collections.success, collections.to_dict()
    assert sample_coll in collections.data["collections"]
    assert collections.data["count"] >= 1

    temp_db = _unique_db_name()
    temp = ZMongo(uri=zmongo_instance.uri, db_name=temp_db)
    try:
        temp.insert_one("temp_coll", {"x": 1})
        drop_result = temp.drop_database()
        assert drop_result.success, drop_result.to_dict()
        assert drop_result.data["dropped_database"] == temp_db
    finally:
        temp.close()


def test_sync_timestamp_returns_expected_shape(zmongo_instance: ZMongo):
    result = zmongo_instance.sync_timestamp()

    assert result.success, result.to_dict()
    assert "latency_seconds" in result.data
    assert "offset_seconds" in result.data
    # localTime may not be present on all deployments, but key should still exist
    assert "server_time" in result.data

def test_closed_instance_refuses_operations(zmongo_instance: ZMongo, sample_coll: str):
    zmongo_instance.close()

    with pytest.raises(RuntimeError, match="ZMongo instance is closed"):
        zmongo_instance.insert_one(sample_coll, {"x": 1})