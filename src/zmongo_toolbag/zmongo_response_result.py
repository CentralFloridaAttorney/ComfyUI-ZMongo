from typing import Dict, Any, NamedTuple


class ZMongoResponseResult(NamedTuple):
    """
    A structured response object for ZMongo database operations (like delete_one).

    This structure is designed to be easily checked by test scripts and provides
    a clear separation between status (success) and payload (data).
    """
    success: bool
    data: Dict[str, Any]


# --- Example Usage for the 'deleted' case (as seen in your code) ---

# Example of a successful return:
success_result = ZMongoResponseResult(
    success=True,
    data={"deleted_count": 1}
)

# Example of the error return you provided:
try:
    # Simulate an error
    raise Exception("Connection timed out")
except Exception as e:
    error_result = ZMongoResponseResult(
        success=False,
        data={"deleted_count": 0, "error": str(e)}
    )

print(f"Success Result: {success_result}")
print(f"Error Result: {error_result}")