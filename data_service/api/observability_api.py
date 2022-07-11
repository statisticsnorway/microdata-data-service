from fastapi import APIRouter, Request


observability_router = APIRouter()


@observability_router.get('/health/alive')
def alive(
        # needed for json_logging.get_correlation_id to work correctly
        request: Request  # pylint: disable=unused-argument
):
    return "I'm alive!"


@observability_router.get('/health/ready')
def ready(
        # needed for json_logging.get_correlation_id to work correctly
        request: Request  # pylint: disable=unused-argument
):
    return "I'm ready!"
