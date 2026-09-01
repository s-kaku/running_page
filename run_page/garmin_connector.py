from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import re
import tempfile
import time
from collections.abc import Callable, Iterable
from enum import StrEnum
from io import BytesIO
from pathlib import Path
from typing import Never, Protocol, TypeVar, cast

from garminconnect import (
    Garmin as GarminApi,
    GarminConnectAuthenticationError as UpstreamAuthenticationError,
    GarminConnectConnectionError as UpstreamConnectionError,
    GarminConnectInvalidFileFormatError as UpstreamInvalidFileFormatError,
    GarminConnectTooManyRequestsError as UpstreamRateLimitError,
)

from garmin_device_adaptor import process_garmin_data

logger = logging.getLogger(__name__)

type GarminActivity = dict[str, object]
type GarminActivitySummary = dict[str, object]

ResultT = TypeVar("ResultT")

RATE_LIMIT_RETRY_DELAYS_SECONDS: tuple[float, ...] = (30.0, 120.0)
UPLOAD_RETRY_DELAYS_SECONDS: tuple[float, ...] = (2.0, 5.0, 10.0)
REQUIRED_TOKEN_KEYS: tuple[str, ...] = (
    "di_token",
    "di_refresh_token",
    "di_client_id",
)


class GarminDomain(StrEnum):
    GLOBAL = "COM"
    CHINA = "CN"


class GarminActivityScope(StrEnum):
    ALL = "all"
    RUNNING = "running"


class GarminFileType(StrEnum):
    GPX = "gpx"
    TCX = "tcx"
    FIT = "fit"


class GarminDeviceMode(StrEnum):
    ORIGINAL = "original"
    FAKE_GARMIN = "fake_garmin"


class StravaActivityData(Protocol):
    filename: str
    content: Iterable[bytes]


class GarminConnectorError(RuntimeError):
    """Base error for the Garmin connector."""


class GarminTokenStoreError(GarminConnectorError):
    """Raised when the token store cannot be loaded or persisted."""


class GarminAuthenticationError(GarminConnectorError):
    """Raised when Garmin rejects the stored credentials."""


class GarminRateLimitError(GarminConnectorError):
    """Raised after Garmin rate-limit retries are exhausted."""


class GarminConnectionError(GarminConnectorError):
    """Raised when Garmin cannot be reached after upstream retries."""


class GarminResponseError(GarminConnectorError):
    """Raised when Garmin returns an unexpected response shape."""


class GarminUploadError(GarminConnectorError):
    """Raised when an activity cannot be prepared or uploaded."""


DOWNLOAD_FORMATS: dict[GarminFileType, GarminApi.ActivityDownloadFormat] = {
    GarminFileType.GPX: GarminApi.ActivityDownloadFormat.GPX,
    GarminFileType.TCX: GarminApi.ActivityDownloadFormat.TCX,
    GarminFileType.FIT: GarminApi.ActivityDownloadFormat.ORIGINAL,
}
API_ERROR_STATUS_PATTERN = re.compile(r"\bAPI Error (?P<status>\d{3})\b")


def _looks_like_inline_token(value: str) -> bool:
    stripped_value = value.strip()
    if stripped_value.startswith(("{", "[")):
        return True
    if len(stripped_value) < 64:
        return False
    try:
        decoded_value = base64.b64decode(stripped_value, validate=True)
    except (binascii.Error, ValueError):
        return False
    return decoded_value.lstrip().startswith((b"{", b"["))


def validate_token_store(token_store: Path) -> Path:
    """Validate a native DI OAuth token file without exposing token values."""
    if _looks_like_inline_token(str(token_store)):
        raise GarminTokenStoreError(
            "Garmin token input must be a private JSON file path, not inline token "
            "data. Decode GARMIN_TOKEN_B64 before running the sync. Garth token "
            "JSON is not compatible."
        )

    normalized_token_store = token_store.expanduser()
    for path_part in (normalized_token_store, *normalized_token_store.parents):
        try:
            if path_part.is_symlink():
                raise GarminTokenStoreError(
                    f"Garmin token store path must not contain symlinks: "
                    f"{normalized_token_store}; symlink={path_part}. "
                    "Use a private directory under your home directory instead."
                )
        except OSError as error:
            raise GarminTokenStoreError(
                f"Garmin token store path cannot be checked safely: "
                f"{normalized_token_store}; path={path_part}; error={error}"
            ) from error

    try:
        raw_tokens = normalized_token_store.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise GarminTokenStoreError(
            f"Garmin token store does not exist: {normalized_token_store}. "
            "Generate a python-garminconnect token before running the sync."
        ) from error
    except OSError as error:
        raise GarminTokenStoreError(
            f"Garmin token store cannot be read: {normalized_token_store}; "
            f"error={error}"
        ) from error

    try:
        parsed_tokens: object = json.loads(raw_tokens)
    except json.JSONDecodeError as error:
        raise GarminTokenStoreError(
            f"Garmin token store is not valid JSON: {normalized_token_store}; "
            f"line={error.lineno}; column={error.colno}"
        ) from error

    if not isinstance(parsed_tokens, dict):
        raise GarminTokenStoreError(
            f"Garmin token store must contain a JSON object: {normalized_token_store}"
        )

    token_values = cast(dict[object, object], parsed_tokens)
    invalid_keys = [
        key
        for key in REQUIRED_TOKEN_KEYS
        if not isinstance(token_values.get(key), str) or not token_values.get(key)
    ]
    if invalid_keys:
        raise GarminTokenStoreError(
            f"Garmin token store is missing native DI OAuth fields: "
            f"{normalized_token_store}; invalid_fields={','.join(invalid_keys)}. "
            "Garth tokens are not compatible."
        )

    return normalized_token_store


def _response_context(error: BaseException) -> tuple[int | None, str | None]:
    response: object = getattr(error, "response", None)
    status_value: object = getattr(response, "status_code", None)
    body_value: object = getattr(response, "text", None)
    status = status_value if isinstance(status_value, int) else None
    if status is None:
        status_match = API_ERROR_STATUS_PATTERN.search(str(error))
        if status_match is not None:
            status = int(status_match.group("status"))
    body = body_value[:500] if isinstance(body_value, str) else None
    return status, body


def _error_message(operation: str, error: BaseException) -> str:
    status, body = _response_context(error)
    context = [
        f"operation={operation}",
        f"error_type={type(error).__name__}",
        f"error={error}",
    ]
    if status is not None:
        context.append(f"status={status}")
    if body:
        context.append(f"response_body={body}")
    return "; ".join(context)


def _raise_upstream_error(operation: str, error: BaseException) -> Never:
    message = _error_message(operation, error)
    status, _ = _response_context(error)
    if isinstance(error, UpstreamAuthenticationError) or status in {401, 403}:
        raise GarminAuthenticationError(message) from error
    if isinstance(error, UpstreamRateLimitError) or status == 429:
        raise GarminRateLimitError(message) from error
    if isinstance(error, UpstreamInvalidFileFormatError):
        raise GarminUploadError(message) from error
    if isinstance(error, UpstreamConnectionError):
        raise GarminConnectionError(message) from error
    raise TypeError(f"Unsupported Garmin error type: {type(error).__name__}") from error


def _call_with_rate_limit_retry(
    operation: str,
    call: Callable[[], ResultT],
    retry_delays_seconds: tuple[float, ...],
) -> ResultT:
    for attempt in range(len(retry_delays_seconds) + 1):
        try:
            return call()
        except UpstreamRateLimitError as error:
            if attempt == len(retry_delays_seconds):
                _raise_upstream_error(operation, error)
            delay_seconds = retry_delays_seconds[attempt]
            logger.warning(
                "Garmin rate limit retry scheduled",
                extra={
                    "operation": operation,
                    "attempt": attempt + 1,
                    "max_attempts": len(retry_delays_seconds) + 1,
                    "delay_seconds": delay_seconds,
                    "status": 429,
                },
            )
            time.sleep(delay_seconds)
        except UpstreamAuthenticationError as error:
            _raise_upstream_error(operation, error)
        except UpstreamInvalidFileFormatError as error:
            _raise_upstream_error(operation, error)
        except UpstreamConnectionError as error:
            _raise_upstream_error(operation, error)

    raise RuntimeError(f"Garmin retry loop exited unexpectedly: operation={operation}")


def _call_credential_login(call: Callable[[], object]) -> None:
    try:
        call()
    except (
        UpstreamAuthenticationError,
        UpstreamRateLimitError,
        UpstreamConnectionError,
    ) as error:
        _raise_upstream_error("credential_login", error)


def _call_upload_with_transient_retry(
    operation: str,
    call: Callable[[], ResultT],
    retry_delays_seconds: tuple[float, ...],
) -> ResultT:
    for attempt in range(len(retry_delays_seconds) + 1):
        try:
            return call()
        except UpstreamConnectionError as error:
            status, _ = _response_context(error)
            if status == 429:
                raise UpstreamRateLimitError(
                    _error_message(operation, error)
                ) from error
            if status is not None and status < 500:
                _raise_upstream_error(operation, error)
            if attempt == len(retry_delays_seconds):
                _raise_upstream_error(operation, error)
            delay_seconds = retry_delays_seconds[attempt]
            logger.warning(
                "Garmin upload retry scheduled",
                extra={
                    "operation": operation,
                    "attempt": attempt + 1,
                    "max_attempts": len(retry_delays_seconds) + 1,
                    "delay_seconds": delay_seconds,
                    "status": status,
                },
            )
            time.sleep(delay_seconds)

    raise RuntimeError(
        f"Garmin upload retry loop exited unexpectedly: operation={operation}"
    )


def _activity_list(response: object, operation: str) -> list[GarminActivity]:
    if not isinstance(response, list):
        raise GarminResponseError(
            f"Garmin returned an unexpected activity response: "
            f"operation={operation}; response_type={type(response).__name__}"
        )

    activities: list[GarminActivity] = []
    for index, activity in enumerate(response):
        if not isinstance(activity, dict) or not all(
            isinstance(key, str) for key in activity
        ):
            raise GarminResponseError(
                f"Garmin returned an invalid activity item: "
                f"operation={operation}; index={index}; "
                f"item_type={type(activity).__name__}"
            )
        activities.append(cast(GarminActivity, dict(activity)))
    return activities


def _activity_summary(response: object, operation: str) -> GarminActivitySummary:
    if not isinstance(response, dict) or not all(
        isinstance(key, str) for key in response
    ):
        raise GarminResponseError(
            f"Garmin returned an unexpected activity summary: "
            f"operation={operation}; response_type={type(response).__name__}"
        )
    return cast(GarminActivitySummary, dict(response))


def _processed_activity_bytes(processed_activity: object, source: Path) -> bytes:
    if isinstance(processed_activity, bytes):
        return processed_activity
    if isinstance(processed_activity, BytesIO):
        return processed_activity.getvalue()
    raise GarminUploadError(
        f"Processed Garmin activity has an unsupported type: "
        f"source={source}; result_type={type(processed_activity).__name__}"
    )


def _create_api(
    email: str | None,
    password: str | None,
    domain: GarminDomain,
    prompt_mfa: Callable[[], str] | None,
) -> GarminApi:
    return GarminApi(
        email=email,
        password=password,
        is_cn=domain is GarminDomain.CHINA,
        prompt_mfa=prompt_mfa,
        return_on_mfa=False,
        retry_attempts=3,
        retry_min_wait=1.0,
        retry_max_wait=10.0,
        verify_login=True,
    )


def generate_token_json(
    email: str,
    password: str,
    domain: GarminDomain,
    prompt_mfa: Callable[[], str],
) -> str:
    """Create native DI OAuth tokens through python-garminconnect login."""
    api = _create_api(email, password, domain, prompt_mfa)
    _call_credential_login(lambda: api.login())
    return api.client.dumps()


class GarminConnector:
    """Deep adapter around python-garminconnect and native DI OAuth tokens."""

    def __init__(
        self,
        token_store: Path,
        domain: GarminDomain,
        activity_scope: GarminActivityScope,
    ) -> None:
        self._token_store = validate_token_store(token_store)
        self._activity_type = (
            "running" if activity_scope is GarminActivityScope.RUNNING else None
        )
        self._lock = asyncio.Lock()
        self._api = _create_api(None, None, domain, None)
        _call_with_rate_limit_retry(
            "login",
            lambda: self._api.login(str(self._token_store)),
            RATE_LIMIT_RETRY_DELAYS_SECONDS,
        )
        self._persist_tokens()

    def _persist_tokens(self) -> None:
        try:
            self._api.client.dump(str(self._token_store))
        except (OSError, ValueError) as error:
            raise GarminTokenStoreError(
                f"Garmin token store cannot be persisted: {self._token_store}; "
                f"error={error}"
            ) from error

    def _call_and_persist(
        self,
        operation: str,
        call: Callable[[], ResultT],
    ) -> ResultT:
        result = _call_with_rate_limit_retry(
            operation,
            call,
            RATE_LIMIT_RETRY_DELAYS_SECONDS,
        )
        self._persist_tokens()
        return result

    def _call_upload_and_persist(
        self,
        operation: str,
        call: Callable[[], ResultT],
    ) -> ResultT:
        result = _call_with_rate_limit_retry(
            operation,
            lambda: _call_upload_with_transient_retry(
                operation,
                call,
                UPLOAD_RETRY_DELAYS_SECONDS,
            ),
            RATE_LIMIT_RETRY_DELAYS_SECONDS,
        )
        self._persist_tokens()
        return result

    async def _run(
        self,
        operation: str,
        call: Callable[[], ResultT],
    ) -> ResultT:
        async with self._lock:
            return await asyncio.to_thread(self._call_and_persist, operation, call)

    async def _run_upload(
        self,
        operation: str,
        call: Callable[[], ResultT],
    ) -> ResultT:
        async with self._lock:
            return await asyncio.to_thread(
                self._call_upload_and_persist,
                operation,
                call,
            )

    async def get_activities(
        self,
        start: int,
        limit: int,
    ) -> list[GarminActivity]:
        operation = f"get_activities(start={start},limit={limit})"
        response: object = await self._run(
            operation,
            lambda: self._api.get_activities(start, limit, self._activity_type),
        )
        return _activity_list(response, operation)

    async def get_activity_summary(
        self,
        activity_id: str,
    ) -> GarminActivitySummary:
        operation = f"get_activity(activity_id={activity_id})"
        response: object = await self._run(
            operation,
            lambda: self._api.get_activity(activity_id),
        )
        return _activity_summary(response, operation)

    async def download_activity(
        self,
        activity_id: str,
        file_type: GarminFileType,
    ) -> bytes:
        operation = (
            f"download_activity(activity_id={activity_id},file_type={file_type.value})"
        )
        response: object = await self._run(
            operation,
            lambda: self._api.download_activity(
                activity_id,
                DOWNLOAD_FORMATS[file_type],
            ),
        )
        if not isinstance(response, bytes):
            raise GarminResponseError(
                f"Garmin returned an unexpected download response: "
                f"operation={operation}; response_type={type(response).__name__}"
            )
        return response

    async def upload_activity(self, activity_path: Path) -> None:
        if not activity_path.is_file():
            raise GarminUploadError(
                f"Garmin activity file does not exist: {activity_path}"
            )
        operation = f"upload_activity(file={activity_path.name})"
        logger.info(
            "Uploading Garmin activity",
            extra={"operation": operation, "activity_path": str(activity_path)},
        )
        await self._run_upload(
            operation,
            lambda: self._api.upload_activity(str(activity_path)),
        )

    async def upload_activities_files(self, files: Iterable[str]) -> None:
        for file in files:
            await self.upload_activity(Path(file))

    async def upload_strava_activities(
        self,
        activities: Iterable[StravaActivityData],
        device_mode: GarminDeviceMode,
    ) -> None:
        for activity in activities:
            await self._upload_strava_activity(activity, device_mode)

    async def _upload_strava_activity(
        self,
        activity: StravaActivityData,
        device_mode: GarminDeviceMode,
    ) -> None:
        filename = Path(activity.filename).name
        if not filename:
            raise GarminUploadError("Strava activity has an empty filename")

        with tempfile.TemporaryDirectory(prefix="running-page-garmin-") as directory:
            activity_path = Path(directory) / filename
            with activity_path.open("wb") as activity_file:
                for chunk_index, chunk in enumerate(activity.content):
                    if not isinstance(chunk, bytes):
                        raise GarminUploadError(
                            f"Strava activity contains a non-bytes chunk: "
                            f"file={filename}; chunk_index={chunk_index}; "
                            f"chunk_type={type(chunk).__name__}"
                        )
                    activity_file.write(chunk)

            with activity_path.open("rb") as activity_file:
                processed_activity: object = process_garmin_data(
                    activity_file,
                    device_mode is GarminDeviceMode.FAKE_GARMIN,
                )
            activity_path.write_bytes(
                _processed_activity_bytes(processed_activity, activity_path)
            )
            await self.upload_activity(activity_path)
