from fastapi import APIRouter, Request


observability_router = APIRouter()


@observability_router.get('/health/alive')
def alive(
        request: Request  # needed for json_logging.get_correlation_id to work correctly
):
    return "I'm alive!"


@observability_router.get('/health/ready')
def ready(
        request: Request  # needed for json_logging.get_correlation_id to work correctly
):
    return "I'm ready!"
