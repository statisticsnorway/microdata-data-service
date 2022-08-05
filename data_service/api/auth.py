import os
import logging
import jwt
from jwt.exceptions import (
    InvalidSignatureError, ExpiredSignatureError,
    InvalidAudienceError, DecodeError
)
from fastapi import HTTPException, status


def authorize_user(authorization_header):
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
        JWT_token = authorization_header.removeprefix('Bearer ')
        public_key = os.environ['JWT_PUBLIC_KEY']

        decoded_jwt = jwt.decode(
            JWT_token, public_key, algorithms=["RS256"], audience="datastore"
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
