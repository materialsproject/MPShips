import fakeredis
import hashlib
import io
import json
import logging
import os
import pandas as pd
import plotly
import redis
import warnings

from mp_web.settings import SETTINGS

logger = logging.getLogger(__name__)


class redis_store:
    """Save data to Redis using the hashed contents as the key.
    Serialize Pandas DataFrames as memory-efficient Parquet files.

    Otherwise, attempt to serialize the data as JSON, which may have a
    lossy conversion back to its original type. For example, numpy arrays will
    be deserialized as regular Python lists.

    Connect to Redis with the environment variable `REDIS_URL` if available.
    Otherwise, use FakeRedis, which is only suitable for development and
    will not scale across multiple processes.
    """

    try:
        r = redis.StrictRedis.from_url(
            os.environ.get("REDIS_URL") or SETTINGS.REDIS_ADDRESS
        )
    except ValueError:
        warnings.warn("Using FakeRedis - Not suitable for Production Use.")
        r = fakeredis.FakeStrictRedis()

    @staticmethod
    def _hash(serialized_obj: bytes) -> str:
        return hashlib.sha512(serialized_obj).hexdigest()

    @staticmethod
    def save(value):
        if isinstance(value, pd.DataFrame):
            buffer = io.BytesIO()
            value.to_parquet(buffer, compression="gzip")
            buffer.seek(0)
            df_as_bytes = buffer.read()
            hash_key = redis_store._hash(df_as_bytes)
            obj_type = "pd.DataFrame"
            serialized_value = df_as_bytes
        else:
            serialized_value = json.dumps(
                value, cls=plotly.utils.PlotlyJSONEncoder
            ).encode("utf-8")
            hash_key = redis_store._hash(serialized_value)
            obj_type = "json-serialized"

        redis_store.r.set(f"_dash_aio_components_value_{hash_key}", serialized_value)
        redis_store.r.set(f"_dash_aio_components_type_{hash_key}", obj_type)
        return hash_key

    @staticmethod
    def load(hash_key):
        data_type = redis_store.r.get(f"_dash_aio_components_type_{hash_key}")
        serialized_value = redis_store.r.get(f"_dash_aio_components_value_{hash_key}")
        try:
            return (
                pd.read_parquet(io.BytesIO(serialized_value))
                if data_type == b"pd.DataFrame"
                else json.loads(serialized_value)
            )
        except Exception as e:
            logger.error(f"{e}\nERROR LOADING {data_type} (hash {hash_key})")
            raise e
