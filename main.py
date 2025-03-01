import os

import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from api.produce import produce_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(produce_router, prefix="/api/kafka", tags=["Kafka produce"])


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
