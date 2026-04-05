import json
import logging
from typing import Any, Dict, Optional, Union

from .data_processor import DataProcessor

logger = logging.getLogger(__name__)


class SafeResult:
    """
    Framework-agnostic, JSON-safe result envelope for ZMongo core services.

    Design goals:
    - Keep the core contract independent from Quart/ComfyUI/frontend concerns.
    - Normalize payloads through DataProcessor for BSON/JSON safety.
    - Preserve access to the original raw payload when needed.
    - Provide a predictable success/failure structure for all core operations.

    Standard fields:
    - success: bool
    - data: JSON-compatible normalized payload
    - message: human-readable status
    - error: JSON-compatible error payload
    - status_code: integer status code
    """

    DEFAULT_SUCCESS_MESSAGE = "OK"
    DEFAULT_FAILURE_MESSAGE = "Error"

    def __init__(
        self,
        success: bool,
        data: Optional[Any] = None,
        message: Optional[str] = None,
        error: Optional[Union[str, Dict[str, Any], Exception]] = None,
        status_code: Optional[int] = None,
        *,
        raw_data: Optional[Any] = None,
    ) -> None:
        self.success: bool = bool(success)
        self._raw_data: Any = raw_data if raw_data is not None else data
        self.data: Any = DataProcessor.to_json_compatible(data)
        self.message: str = self._resolve_message(
            success=self.success,
            message=message,
            error=error,
        )
        self.error: Any = self._normalize_error(error)
        self.status_code: int = int(
            status_code if status_code is not None else (200 if self.success else 400)
        )

        logger.debug(
            "[SafeResult.__init__] success=%s status_code=%s message=%s",
            self.success,
            self.status_code,
            self.message,
        )

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------
    @classmethod
    def ok(
        cls,
        data: Optional[Any] = None,
        message: Optional[str] = None,
        status_code: int = 200,
    ) -> "SafeResult":
        return cls(
            success=True,
            data=data,
            message=message or cls.DEFAULT_SUCCESS_MESSAGE,
            status_code=status_code,
            raw_data=data,
        )

    @classmethod
    def fail(
        cls,
        error: Optional[Union[str, Dict[str, Any], Exception]] = None,
        data: Optional[Any] = None,
        status_code: int = 400,
        message: Optional[str] = None,
    ) -> "SafeResult":
        normalized_error = cls._normalize_error_static(error)
        resolved_message = cls._resolve_message_static(
            success=False,
            message=message,
            error=normalized_error,
        )
        return cls(
            success=False,
            data=data,
            message=resolved_message,
            error=normalized_error,
            status_code=status_code,
            raw_data=data,
        )

    @classmethod
    def ensure(
        cls,
        condition: bool,
        message: str = "Condition failed",
        **kwargs,
    ) -> "SafeResult":
        """
        If condition is True, return success.
        If condition is False, return failure with the provided message as the error.

        Existing caller behavior is preserved:
        - On success, kwargs['message'] is honored if provided, otherwise "OK".
        - On failure, the explicit `message` argument becomes the error payload.
        """
        if condition:
            success_message = kwargs.pop("message", cls.DEFAULT_SUCCESS_MESSAGE)
            return cls.ok(message=success_message, **kwargs)
        return cls.fail(error=message, **kwargs)

    @classmethod
    def from_exception(
        cls,
        exc: Exception,
        *,
        data: Optional[Any] = None,
        status_code: int = 500,
        message: Optional[str] = None,
        operation: Optional[str] = None,
    ) -> "SafeResult":
        error_payload: Dict[str, Any] = {
            "error_type": exc.__class__.__name__,
            "error": str(exc),
        }
        if operation:
            error_payload["operation"] = operation

        return cls.fail(
            error=error_payload,
            data=data,
            status_code=status_code,
            message=message or str(exc),
        )

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "SafeResult":
        if not isinstance(payload, dict):
            return cls.fail(
                error="from_dict expected a dictionary payload",
                status_code=500,
                message="Invalid SafeResult payload",
            )

        return cls(
            success=bool(payload.get("success", False)),
            data=payload.get("data"),
            message=payload.get("message"),
            error=payload.get("error"),
            status_code=int(payload.get("status_code", 200 if payload.get("success") else 400)),
            raw_data=payload.get("data"),
        )

    # ------------------------------------------------------------------
    # Accessors / conversion
    # ------------------------------------------------------------------
    def original(self) -> Any:
        """
        Return the original raw payload, with optional key restoration if a __keymap
        convention is present in the raw data structure.
        """
        return self._apply_keymap(self._raw_data)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "data": self.data,
            "message": self.message,
            "error": self.error,
            "status_code": self.status_code,
        }

    def model_dump(self) -> Dict[str, Any]:
        return self.to_dict()

    def to_json(self, *, indent: Optional[int] = None, sort_keys: bool = False) -> str:
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            indent=indent,
            sort_keys=sort_keys,
            default=str,
        )

    def copy(
        self,
        *,
        success: Optional[bool] = None,
        data: Any = None,
        message: Optional[str] = None,
        error: Any = None,
        status_code: Optional[int] = None,
        keep_raw_data: bool = True,
    ) -> "SafeResult":
        new_data = self.data if data is None else data
        new_error = self.error if error is None else error
        new_success = self.success if success is None else success
        new_message = self.message if message is None else message
        new_status = self.status_code if status_code is None else status_code

        return SafeResult(
            success=new_success,
            data=new_data,
            message=new_message,
            error=new_error,
            status_code=new_status,
            raw_data=self._raw_data if keep_raw_data else new_data,
        )

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------
    def log(self, prefix: str = "") -> "SafeResult":
        msg = f"{prefix} [{self.status_code}] {self.message}".strip()
        if self.success:
            logger.info("[SafeResult.log] %s", msg)
        else:
            logger.error("[SafeResult.log] %s | %s", msg, self.error)
        return self

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_error_static(
        error: Optional[Union[str, Dict[str, Any], Exception]]
    ) -> Any:
        if error is None:
            return None

        if isinstance(error, Exception):
            return {
                "error_type": error.__class__.__name__,
                "error": str(error),
            }

        return DataProcessor.to_json_compatible(error)

    def _normalize_error(
        self,
        error: Optional[Union[str, Dict[str, Any], Exception]],
    ) -> Any:
        return self._normalize_error_static(error)

    @staticmethod
    def _resolve_message_static(
        *,
        success: bool,
        message: Optional[str],
        error: Any,
    ) -> str:
        if message:
            return str(message)

        if success:
            return SafeResult.DEFAULT_SUCCESS_MESSAGE

        if isinstance(error, str) and error.strip():
            return error

        if isinstance(error, dict):
            err_text = error.get("error")
            if isinstance(err_text, str) and err_text.strip():
                return err_text

        return SafeResult.DEFAULT_FAILURE_MESSAGE

    def _resolve_message(
        self,
        *,
        success: bool,
        message: Optional[str],
        error: Any,
    ) -> str:
        return self._resolve_message_static(
            success=success,
            message=message,
            error=self._normalize_error_static(error),
        )

    def _apply_keymap(self, obj: Any) -> Any:
        """
        Rebuild keys from a __keymap convention when callers stored normalized keys.
        This preserves compatibility with prior behavior.
        """
        if isinstance(obj, list):
            return [self._apply_keymap(item) for item in obj]

        if isinstance(obj, dict):
            keymap = obj.get("__keymap")
            restored: Dict[str, Any] = {}

            for key, value in obj.items():
                if key == "__keymap":
                    continue

                restored_key = keymap[key] if isinstance(keymap, dict) and key in keymap else key
                restored[restored_key] = self._apply_keymap(value)

            return restored

        return obj

    # ------------------------------------------------------------------
    # Dunder methods
    # ------------------------------------------------------------------
    def __bool__(self) -> bool:
        return self.success

    def __repr__(self) -> str:
        return (
            f"SafeResult(success={self.success}, "
            f"status_code={self.status_code}, "
            f"message={self.message!r}, "
            f"data={self.data!r}, "
            f"error={self.error!r})"
        )