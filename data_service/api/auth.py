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
    try:
        JWT_token = authorization_header.removeprefix('Bearer ')
        public_key = os.environ.get('JWT_PUBLIC_KEY')
        decoded_jwt = jwt.decode(
            JWT_token, public_key, algorithms=["RS256"], audience="datastore"
        )
        user_id = decoded_jwt.get("user_id")
        if user_id in [None, '']:
            raise NoUserError("No valid user_id")
        return user_id

    except (InvalidSignatureError, ExpiredSignatureError, InvalidAudienceError,
            NoUserError, DecodeError, ValueError) as e:
        log.info(f"{e.value()}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized"
        )

    except Exception as e:
        log.error(f"Internal Server Error: {e.value()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal Server Error"
        )


class NoUserError(Exception):
    pass