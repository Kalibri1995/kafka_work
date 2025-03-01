import os

import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from api.consumer import consumer_router
from api.produce import produce_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(produce_router, prefix="/api/kafka/produce", tags=["Kafka produce"])
app.include_router(consumer_router, prefix="/api/kafka/consume", tags=["Kafka consume"])


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
