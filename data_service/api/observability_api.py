from fastapi import APIRouter


observability_router = APIRouter()


@observability_router.get('/health/alive')
def alive():
    return "I'm alive!"


@observability_router.get('/health/ready')
def ready():
    return "I'm ready!"
