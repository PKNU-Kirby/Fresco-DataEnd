import pandas as pd
import time
from db import engine

_cached_df = None
_last_loaded = 0
CACHE_TTL = 60

def get_all_ingredients():
    global _cached_df, _last_loaded
    now = time.time()

    if _cached_df is None or now - _last_loaded > CACHE_TTL:
        query = """
            SELECT i.id as ingredientId,
                i.name as ingredientName,
                i.updatedAt,
                c.id as categoryId,
                c.name as categoryName
            FROM ingredients i
            JOIN categories c ON i.categoryId = c.id
        """
        _cached_df = pd.read_sql(query, engine)
        _last_loaded = now

    return _cached_df
