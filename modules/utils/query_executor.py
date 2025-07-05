from typing import Any, Dict, List, Optional, Tuple, Union

from loguru import logger
from psycopg2.extensions import connection as pg_connection
from psycopg2.extras import DictCursor, DictRow

from modules.common.exceptions import DatabaseError


def execute_query(
    connection: pg_connection,
    query: str,
    params: Optional[Union[Tuple[Any, ...], Dict[str, Any], List[Any]]] = None,
) -> Union[List[DictRow], int]:
    logger.debug(f"Executing query: {query}")
    if params:
        logger.debug(f"With parameters: {params}")

    try:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(query, params)

            # SELECT 또는 RETURNING 쿼리인 경우 결과 반환
            if query.strip().upper().startswith("SELECT") or "RETURNING" in query.upper():
                result = cursor.fetchall()
                connection.commit()
                return result

            # 그 외 쿼리인 경우 영향받은 행의 수 반환
            connection.commit()
            return cursor.rowcount

    except Exception as e:
        connection.rollback()
        logger.error(f"Query failed: {str(e)}")
        raise DatabaseError(f"Database operation failed: {str(e)}") from e
