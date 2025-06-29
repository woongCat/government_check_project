from modules.utils.query_executor import execute_query


class BaseLoader:
    def __init__(self, connection):
        self.connection = connection

    def _execute_query(self, query, params=None):
        return execute_query(self.connection, query, params)

    def close(self):
        if self.connection:
            self.connection.close()
        