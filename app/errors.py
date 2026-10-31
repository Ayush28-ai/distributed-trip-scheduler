"""Error framework. Every error response has the shape {"error": code, "message": text}.
Responses never echo request bodies (payloads may hold phone numbers or addresses),
and unexpected errors return a generic message with a request id; details go to logs."""
import logging
import uuid

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.log_config import log_event

log = logging.getLogger("api")


class DomainError(Exception):
    status_code = 400
    code = "bad_request"

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class NotFoundError(DomainError):
    status_code = 404
    code = "not_found"


class ConflictError(DomainError):
    status_code = 409
    code = "conflict"


def _body(code: str, message: str, **extra) -> dict:
    return {"error": code, "message": message, **extra}


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(DomainError)
    async def _domain(_: Request, exc: DomainError):
        return JSONResponse(_body(exc.code, exc.message), status_code=exc.status_code)

    @app.exception_handler(HTTPException)
    async def _http(_: Request, exc: HTTPException):
        code = {401: "unauthorized", 403: "forbidden", 404: "not_found"}.get(exc.status_code, "error")
        return JSONResponse(_body(code, str(exc.detail)), status_code=exc.status_code,
                            headers=getattr(exc, "headers", None))

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError):
        # field path + message only; the submitted values are deliberately not returned
        details = [{"field": ".".join(str(p) for p in e.get("loc", []) if p != "body"),
                    "message": e.get("msg", "invalid")} for e in exc.errors()]
        return JSONResponse(_body("validation_error", "request is invalid", details=details),
                            status_code=422)

    @app.exception_handler(Exception)
    async def _unexpected(request: Request, exc: Exception):
        request_id = uuid.uuid4().hex[:12]
        log_event(log, "unhandled_error", level=logging.ERROR, request_id=request_id,
                  path=request.url.path, error_type=type(exc).__name__)
        return JSONResponse(_body("internal_error", "unexpected error", request_id=request_id),
                            status_code=500)
