from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import routers


def create_app():
    app = FastAPI()

    app.include_router(routers.a16_10bat_1.router)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["*"]
    )

    return app


app = create_app()
