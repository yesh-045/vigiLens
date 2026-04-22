from vigilens.apps.api.stream import stream_router
from vigilens.apps.api.query import query_router
from fastapi import FastAPI
from vigilens.core.config import settings
import logging
from contextlib import asynccontextmanager
from vigilens.core.db import init_db_async

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_db_async()
    yield
    # Shutdown

app = FastAPI(title="vigilens-api", root_path=settings.vigilens_api_root_path, lifespan=lifespan)


app.include_router(stream_router)
app.include_router(query_router)


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
