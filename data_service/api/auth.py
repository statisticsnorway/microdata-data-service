import logging
import os

import jwt
from fastapi import HTTPException, status
from jwt import PyJWKClient
from jwt.exceptions import (
    InvalidSignatureError, ExpiredSignatureError,
    InvalidAudienceError, DecodeError
)

jwks_client = PyJWKClient(os.environ['JWKS_URL'], lifespan=3000)


def get_jwks_aud() -> str:
    return "datastore-qa" if os.environ['STACK'] == 'qa' else "datastore"


def get_signing_key(jwt_token: str):
    signing_key = jwks_client.get_signing_key_from_jwt(jwt_token)
    return signing_key.key


def authorize_user(authorization_header) -> str:
    log = logging.getLogger(__name__)

    if os.environ.get('JWT_AUTH', 'true') == 'false':
        log.info('Auth toggled off. Returning "default" as user_id.')
        return 'default'
    if authorization_header is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized. No token was provided"
        )

    try:
        jwt_token = authorization_header.removeprefix('Bearer ')
        signing_key = get_signing_key(jwt_token)

        decoded_jwt = jwt.decode(
            jwt_token,
            signing_key,
            algorithms=["RS256", "RS512"],
            audience=get_jwks_aud()
        )
        user_id = decoded_jwt.get("sub")
        if user_id in [None, '']:
            raise NoUserError("No valid user_id")
        return user_id

    except (InvalidSignatureError, ExpiredSignatureError, InvalidAudienceError,
            NoUserError, DecodeError, ValueError, AttributeError) as e:
        log.error(f"{e}")
        raise HTTPException(  # pylint: disable=raise-missing-from
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Unauthorized {e}"
        )

    except (KeyError, Exception) as e:
        log.error(f"Internal Server Error: {e}")
        raise HTTPException(  # pylint: disable=raise-missing-from
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal Server Error {e}"
        )


class NoUserError(Exception):
    pass
