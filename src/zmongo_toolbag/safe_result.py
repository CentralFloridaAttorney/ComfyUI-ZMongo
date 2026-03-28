import logging
import json
from typing import Any, Dict, Optional, Union
from quart import jsonify, Response
from .data_processing import DataProcessor

logger = logging.getLogger(__name__)


class SafeResult:
    """
    Unified, BSON-safe, Quart-compatible response wrapper for ZLegal Codex.
    """

    def __init__(
            self,
            success: bool,
            data: Optional[Any] = None,
            message: Optional[str] = None,
            error: Optional[Union[str, Dict[str, Any]]] = None,
            status_code: Optional[int] = None,
            _raw_data: Optional[Any] = None,  # Internal storage for original objects
    ):
        self.success = success

        # Store raw data for .original() restoration (ObjectIds, etc.)
        self._raw_data = _raw_data if _raw_data is not None else data

        # Processed data for JSON serialization
        self.data = DataProcessor.to_json_compatible(data)

        self.message = message or ("Success" if success else "Error")
        self.error = DataProcessor.to_json_compatible(error)
        self.status_code = status_code or (200 if success else 400)

        logger.debug(
            f"[SafeResult.__init__] success={self.success}, "
            f"status={self.status_code}, message={self.message}"
        )

    # ------------------------------------------------------------------
    # ✅ Constructors
    # ------------------------------------------------------------------
    @classmethod
    def ok(
            cls,
            data: Optional[Any] = None,
            message: Optional[str] = "OK",
            status_code: int = 200,
    ) -> "SafeResult":
        # Pass data as _raw_data to preserve ObjectIds
        return cls(True, data=data, message=message, status_code=status_code, _raw_data=data)

    @classmethod
    def fail(
            cls,
            error: Optional[Union[str, Dict[str, Any], Exception]] = "Error",
            data: Optional[Any] = None,
            status_code: int = 400,
            message: Optional[str] = None,
    ) -> "SafeResult":
        """
        Supports positional args matching tests: fail(error_msg, data_dict)
        """
        err_val = str(error) if isinstance(error, Exception) else error
        msg_val = message or (str(err_val) if isinstance(err_val, str) else "Error")

        return cls(
            False,
            data=data,
            message=msg_val,
            error=err_val,
            status_code=status_code,
            _raw_data=data
        )

    # ------------------------------------------------------------------
    # ✅ Data Restoration (Missing in original file)
    # ------------------------------------------------------------------
    def original(self) -> Any:
        """
        Returns the original (raw) data, restoring ObjectIds and applying
        '__keymap' transformations if present.
        """
        return self._apply_keymap(self._raw_data)

    def _apply_keymap(self, obj: Any) -> Any:
        """Recursively apply __keymap field renaming to the object."""
        if isinstance(obj, list):
            return [self._apply_keymap(item) for item in obj]

        if isinstance(obj, dict):
            # Check for keymap
            keymap = obj.get("__keymap")
            new_obj = {}
            for k, v in obj.items():
                if k == "__keymap":
                    continue

                # Recursive process value
                val = self._apply_keymap(v)

                # Remap key if needed
                if keymap and k in keymap:
                    new_obj[keymap[k]] = val
                else:
                    new_obj[k] = val
            return new_obj

        return obj

    # ------------------------------------------------------------------
    # ✅ Serialization & Representation
    # ------------------------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        """Convert to a JSON-safe dict."""
        return {
            "success": self.success,
            "data": self.data,
            "message": self.message,
            "error": self.error,
            "status_code": self.status_code,
        }

    def model_dump(self) -> Dict[str, Any]:
        """Alias for to_dict, expected by tests."""
        return self.to_dict()

    def to_json(self) -> str:
        """Return JSON string representation."""
        return json.dumps(self.to_dict(), default=str)

    def __repr__(self):
        # Format matches test expectation: SafeResult(success=True ...)
        return f"SafeResult(success={self.success}, data={self.data}, message={self.message}, error={self.error})"

    def __bool__(self):
        return self.success

    # ------------------------------------------------------------------
    # ✅ Quart Integration
    # ------------------------------------------------------------------
    def to_response(self, override_status: Optional[int] = None):
        code = override_status or self.status_code
        return jsonify(self.to_dict()), code

    @classmethod
    async def from_quart_response(cls, resp: Union[Response, tuple]) -> "SafeResult":
        try:
            if isinstance(resp, tuple):
                resp_obj, code = resp
            else:
                resp_obj = resp
                code = getattr(resp, "status_code", 200)

            body = await resp_obj.get_data(as_text=True)
            parsed = json.loads(body) if body else {}

            if isinstance(parsed, dict) and "success" in parsed:
                return cls(
                    success=parsed.get("success", False),
                    data=parsed.get("data"),
                    message=parsed.get("message"),
                    error=parsed.get("error"),
                    status_code=parsed.get("status_code", code),
                )

            return cls(
                success=(200 <= code < 300),
                data=parsed,
                message=f"Parsed response ({code})",
                status_code=code,
            )

        except Exception as e:
            return cls.fail("Response parsing failed", error=str(e), status_code=500)

    # ------------------------------------------------------------------
    # 🧠 Utilities
    # ------------------------------------------------------------------
    def log(self, prefix: str = "") -> "SafeResult":
        msg = f"{prefix} [{self.status_code}] {self.message}"
        if self.success:
            logger.info(f"[SafeResult.log] ✅ {msg}")
        else:
            logger.error(f"[SafeResult.log] ❌ {msg} | {self.error}")
        return self

    @classmethod
    def ensure(cls, condition: bool, message: str = "Condition failed", **kwargs) -> "SafeResult":
        if condition:
            return cls.ok(**kwargs)
        return cls.fail(error=message, **kwargs)