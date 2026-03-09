import json
import logging
import os
from collections import OrderedDict
from typing import Any

import httpx
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, field_validator

from src.agent import ask as agent_ask, ask_streaming
from src.db import get_pool, release_pool, set_db_url
from src import vault

load_dotenv()

logger = logging.getLogger("cricket-agent")

app = FastAPI(title="Cricket Agent API")

allowed_origins = os.environ.get("ALLOWED_ORIGINS", "http://localhost:5173").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Mode switching (M7)
# ---------------------------------------------------------------------------
current_mode: int = int(os.environ.get("CRICKET_MODE", "3"))

SECURITY_AGENT_URL: str = os.environ.get(
    "SECURITY_AGENT_URL", "http://agent-identity-lens:8001"
)

# ---------------------------------------------------------------------------
# LRU query cache (M5)
# ---------------------------------------------------------------------------
_CACHE_MAX = 100
_query_cache: OrderedDict[str, Any] = OrderedDict()


def _cache_get(key: str) -> Any | None:
    if key in _query_cache:
        _query_cache.move_to_end(key)
        return _query_cache[key]
    return None


def _cache_put(key: str, value: Any) -> None:
    if key in _query_cache:
        _query_cache.move_to_end(key)
    else:
        if len(_query_cache) >= _CACHE_MAX:
            _query_cache.popitem(last=False)
    _query_cache[key] = value


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reader_url() -> str:
    db_url = os.environ["DATABASE_URL"]
    return os.environ.get(
        "READER_DATABASE_URL",
        db_url.replace("postgresql://postgres:postgres@", "postgresql://cricket_reader:readonlypass@"),
    )


async def _fire_webhook() -> None:
    """POST to security agent /api/refresh. Fire-and-forget."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(f"{SECURITY_AGENT_URL}/api/refresh")
    except Exception:
        logger.warning("Webhook to security agent failed", exc_info=True)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class AskRequest(BaseModel):
    question: str
    history: list = []


class AskResponse(BaseModel):
    answer: str
    queries: list[str]


class ModeRequest(BaseModel):
    mode: int

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, v: int) -> int:
        if v not in (1, 2, 3):
            raise ValueError("mode must be 1, 2, or 3")
        return v


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.post("/api/ask", response_model=AskResponse)
def ask(body: AskRequest, background_tasks: BackgroundTasks):
    try:
        result = agent_ask(
            body.question,
            _reader_url(),
            history=body.history,
            cache_get=_cache_get,
            cache_put=_cache_put,
        )
        background_tasks.add_task(_fire_webhook)
        return AskResponse(answer=result["answer"], queries=result["queries"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/ask/stream")
async def ask_stream(body: AskRequest, background_tasks: BackgroundTasks):
    """SSE streaming endpoint for /api/ask. Streams tokens as they arrive."""

    async def event_generator():
        queries: list[str] = []
        try:
            for event in ask_streaming(
                body.question,
                _reader_url(),
                history=body.history,
                cache_get=_cache_get,
                cache_put=_cache_put,
            ):
                if event["type"] == "token":
                    yield f"data: {json.dumps({'type': 'token', 'content': event['content']})}\n\n"
                elif event["type"] == "query":
                    queries.append(event["sql"])
                    yield f"data: {json.dumps({'type': 'query', 'sql': event['sql']})}\n\n"
            yield f"data: {json.dumps({'type': 'done', 'queries': queries})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'detail': str(e)})}\n\n"

    background_tasks.add_task(_fire_webhook)
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/mode")
def set_mode(body: ModeRequest):
    global current_mode
    if body.mode == 2:
        creds = vault.initialize_mode2()
    elif body.mode == 1:
        creds = vault.initialize_mode1()
    else:
        vault.clear()
        set_db_url(None)
        release_pool()
        current_mode = body.mode
        return {"mode": current_mode, "status": "ok"}

    db_url = (
        f"postgresql://{creds.db_username}:{creds.db_password}"
        f"@postgres:5432/cricket"
    )
    set_db_url(db_url)
    release_pool()
    current_mode = body.mode
    return {"mode": current_mode, "status": "ok"}


@app.get("/credentials")
def get_credentials():
    import time
    creds = vault.get_current_creds()
    if current_mode in (1, 2) and creds is not None:
        elapsed = time.time() - creds.fetched_at
        token_remaining = max(0, int(creds.token_ttl - elapsed))
        db_remaining = max(0, int(creds.db_ttl - elapsed))
        auth_source = "spiffe_jwt_svid" if current_mode == 1 else "vault_approle"
        db_source = "vault_database_15m" if current_mode == 1 else "vault_database_1h"
        result = {
            "mode": current_mode,
            "credentials": [
                {"name": "VAULT_TOKEN", "type": "vault_token", "ttl": token_remaining, "source": auth_source},
                {"name": "DB_USERNAME", "type": "vault_dynamic", "value": creds.db_username, "ttl": db_remaining, "source": db_source},
                {"name": "ANTHROPIC_API_KEY", "type": "vault_kv", "ttl": None, "source": "vault_kv"},
            ],
        }
        svid_info = vault.get_svid_info()
        if svid_info:
            expires_in = max(0, int(svid_info["expires_at"] - time.time()))
            result["svid"] = {
                "spiffe_id": svid_info["spiffe_id"],
                "trust_domain": svid_info["trust_domain"],
                "expires_in": expires_in,
                "ttl_total": svid_info["ttl_total"],
            }
        return result
    # Mode 3 (default): env vars, no TTL
    return {
        "mode": current_mode,
        "credentials": [
            {"name": "DATABASE_URL", "type": "env_var", "ttl": None},
            {"name": "ANTHROPIC_API_KEY", "type": "env_var", "ttl": None},
        ],
    }


# ---------------------------------------------------------------------------
# Static files — mount AFTER all API routes so /api/* takes precedence.
# Only mount if the static directory exists (i.e., in Docker build).
# ---------------------------------------------------------------------------
_static_dir = os.path.join(os.path.dirname(__file__), "..", "static")
if os.path.isdir(_static_dir):
    app.mount("/", StaticFiles(directory=_static_dir, html=True), name="static")


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------
@app.on_event("shutdown")
def shutdown():
    release_pool()
