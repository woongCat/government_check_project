def execute_query(connection, query, params=None):
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            if query.strip().upper().startswith("SELECT"):
                return cursor.fetchall()
            else:
                connection.commit()
    except Exception as e:
        connection.rollback()
        raise e