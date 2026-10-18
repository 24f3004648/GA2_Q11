from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from core import (
    DEFAULT_LIMIT,
    OrderStore,
    RateLimiter,
    paginate,
)

app = FastAPI()
store = OrderStore()
limiter = RateLimiter()


# Registered first => inner layer. CORS is added after it, so CORS wraps the
# 429 responses too and the browser can read Retry-After.
@app.middleware("http")
async def rate_limit(request: Request, call_next):
    if request.method == "OPTIONS":  # CORS preflights are not counted
        return await call_next(request)
    client_id = request.headers.get("x-client-id") or (
        request.client.host if request.client else "anonymous"
    )
    allowed, retry_after = limiter.check(client_id)
    if not allowed:
        return JSONResponse(
            status_code=429,
            content={"detail": "rate limit exceeded"},
            headers={"Retry-After": str(retry_after)},
        )
    return await call_next(request)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["Retry-After", "Idempotent-Replayed"],
)


@app.post("/orders", status_code=201)
async def create_order(
    request: Request,
    response: Response,
    idempotency_key: Optional[str] = Header(default=None),
):
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {"value": body}
    order, replayed = store.create(idempotency_key, body)
    if replayed:
        response.headers["Idempotent-Replayed"] = "true"
    return order  # a replay returns the original order and the same 201


@app.get("/orders")
def list_orders(limit: int = Query(DEFAULT_LIMIT, ge=1), cursor: Optional[str] = None):
    try:
        return paginate(limit, cursor)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid cursor")


@app.get("/")
def root():
    return {"status": "ok"}
