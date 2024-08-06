# pylint: disable=unused-argument
from fastapi import APIRouter, Request


observability_router = APIRouter()


@observability_router.get("/health/alive")
def alive(request: Request):
    return "I'm alive!"


@observability_router.get("/health/ready")
def ready(request: Request):
    return "I'm ready!"
