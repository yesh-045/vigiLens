from pathlib import Path
import os

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import httpx
from fastapi.responses import JSONResponse

STATIC_DIR = Path(__file__).parent / "static"
VIGILENS_API_BASE = os.getenv(
    "VIGILENS_API_BASE",
    "http://localhost:8001",
).rstrip("/")

app = FastAPI(title="vigilens-demo-console")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


async def _forward_json_post(path: str, payload: dict) -> JSONResponse:
    upstream = f"{VIGILENS_API_BASE}{path}"
    try:
        async with httpx.AsyncClient(timeout=20.0, trust_env=False) as client:
            response = await client.post(upstream, json=payload)
    except httpx.RequestError as exc:
        return JSONResponse(
            status_code=502,
            content={
                "detail": f"Failed to reach Vigilens API at {upstream}: {exc}",
            },
        )

    try:
        content = response.json()
    except ValueError:
        content = {"detail": response.text}
    return JSONResponse(status_code=response.status_code, content=content)


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/api/streams/submit")
async def submit_proxy(request: Request):
    payload = await request.json()
    return await _forward_json_post("/streams/submit", payload)


@app.post("/api/query")
async def query_proxy(request: Request):
    payload = await request.json()
    return await _forward_json_post("/query", payload)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("vigilens.apps.demo.app:app", host="0.0.0.0", port=8010, reload=True)
