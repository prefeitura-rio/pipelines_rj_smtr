# -*- coding: utf-8 -*-
def create_capture_redis_key(
    dataset_id: str,
    table_id: str,
    mode: str,
) -> str:
    """
    Create the name for the Redis key that controls incremental captures

    Args:
        dataset_id (str): Source table's dataset_id
        table_id (str): Source table's table_id
        mode (str): prod or dev

    Returns:
        str: The redis key

    """
    assert mode in ("prod", "staging"), f"mode must be prod or staging, received {mode}"
    return f"{mode}.{dataset_id}.{table_id}"
