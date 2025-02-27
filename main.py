import os

import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# app.include_router(share_router, prefix="/share", tags=["Share"])


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)