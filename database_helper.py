"""
Database helper classes for SQLite3 and MySQL databases
"""

import sqlite3
import threading

import mysql.connector


class Sqlite3():
    """Class that implements methods for working with a SQLite3 database"""
    locks = {}

    def __init__(self, database):
        """Initializes a SQLite3 database helper object

        Args:
            database (str): Database name
        """
        self.database = database
        self.db_con = None
        self.db_cur = None
        if database not in self.locks:
            self.locks[database] = threading.Lock()

    def open(self):
        """Opens the database"""
        self.db_con = sqlite3.connect(self.database)
        self.db_cur = self.db_con.cursor()

    def close(self):
        """Closes the database"""
        self.db_cur.close()
        self.db_con.close()

    def insert(self, *, table, details):
        """Inserts the passed details into the specified table

        Args:
            table (str): Name of the table
            details (dict): Column names and values to insert into the table.
                Dictionary keys must exactly match column names in the table
        Returns:
            int: ID of the newly inserted row
        """
        with self.locks[self.database]:
            self.open()
            try:
                placeholders = ', '.join(['?'] * len(details))
                columns = ', '.join(details)
                values = list(details.values())
                sql_statement = (f'INSERT INTO {table} ({columns}) VALUES '
                                f'({placeholders})')
                self.db_cur.execute(sql_statement, values)
                self.db_con.commit()
                row_id = self.db_cur.lastrowid
                return row_id
            finally:
                self.close()

    def update(self, *, table, details, key_name, key_value):
        """Updates the specified table with the passed details

        Args:
            table (str): Name of the table to be updated
            details (dict): Column names and values to update in the table.
                Dictionary keys must exactly match column names in the table
            key_name (str): Column name for the WHERE specification
            key_value: Variable type value for the WHERE specification
        """
        with self.locks[self.database]:
            self.open()
            try:
                statement = ', '.join(f'{key} = ?' for key in details)
                values = list(details.values())
                values.append(key_value)
                sql_statement = (f'UPDATE {table} SET {statement} WHERE '
                                f'{key_name} = ?')
                self.db_cur.execute(sql_statement, values)
                self.db_con.commit()
            finally:
                self.close()

    def select(self, *, table, columns, key_name=None, key_value=None,
               order_column=None, direction=None, limit=None):
        """Selects values from the passed column(s) from the specified table
        with optional constraints

        Args:
            table (str): Name of the table to be updated
            columns (list): Table column names to select
            key_name (str): Column name for the WHERE specification
            key_value: Variable type value for the WHERE specification
            order_column (str): Column name to order by
            direction (str): Order direction; should be ASC or DESC
            limit (int): Number of rows to select
        Returns:
            list: List of sqlite3.Row objects returned by the query
        """
        with self.locks[self.database]:
            self.open()
            try:
                columns_text = ', '.join(f'{name}' for name in columns)
                sql_statement = f'SELECT {columns_text} FROM {table}'

                sql_value = None
                if key_name is not None and key_value is not None:
                    sql_statement += f' WHERE {key_name} = ?'
                    sql_value = [key_value]

                if order_column is not None and direction is not None:
                    sql_statement += f' ORDER BY {order_column} {direction}'

                if limit is not None:
                    sql_statement += f' LIMIT {limit}'

                if sql_value is not None:
                    self.db_cur.execute(sql_statement, sql_value)
                else:
                    self.db_cur.execute(sql_statement)

                selected_values = self.db_cur.fetchall()
                return selected_values
            finally:
                self.close()

    def execute_sql(self, sql_str):
        """Exectues the passed SQL string. This method is useful for creating
        or dropping a table in the database

        Args:
            sql_str (str): SQL string to execute
        """
        with self.locks[self.database]:
            self.open()
            try:
                self.db_cur.execute(sql_str)
                self.db_con.commit()
            finally:
                self.close()


class MySql():
    """Class that implements methods for working with a MySQL database"""
    locks = {}

    def __init__(self, host, database, user, password):
        """Initializes a MySQL database helper object

        Args:
            host (str): Host name
            database (str): Database name
            user (str): Username
            password (str): Password
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.db_con = None
        self.db_cur = None
        if database not in self.locks:
            self.locks[database] = threading.Lock()

        # Ensure the database has been created
        self._create_database()

    def open(self):
        """Opens the database"""
        self.db_con = mysql.connector.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            passwd=self.password
        )
        self.db_cur = self.db_con.cursor()

    def close(self):
        """Closes the database"""
        self.db_cur.close()
        self.db_con.close()

    def insert(self, *, table, details):
        """Inserts the passed details into the specified table

        Args:
            table (str): Name of the table
            details (dict): Column names and values to insert into the table.
                Dictionary keys must exactly match column names in the table
        Returns:
            row_id (int): ID of the newly inserted row
        """
        with self.locks[self.database]:
            self.open()
            try:
                placeholders = ', '.join(['%s'] * len(details))
                columns = ', '.join(details)
                values = list(details.values())
                sql_statement = (f'INSERT INTO {table} ({columns}) VALUES '
                                f'({placeholders})')
                self.db_cur.execute(sql_statement, values)
                self.db_con.commit()
                row_id = self.db_cur.lastrowid
                return row_id
            finally:
                self.close()

    def update(self, *, table, details, key_name, key_value):
        """Updates the specified table with the passed details

        Args:
            table (str): Name of the table to be updated
            details (dict): Column names and values to update in the table.
                Dictionary keys must exactly match column names in the table
            key_name (str): Column name for the WHERE specification
            key_value: Variable type value for the WHERE specification
        """
        with self.locks[self.database]:
            self.open()
            try:
                statement = ', '.join(f'{key} = %s' for key in details)
                values = list(details.values())
                values.append(key_value)
                sql_statement = (f'UPDATE {table} SET {statement} WHERE '
                                f'{key_name} = %s')
                self.db_cur.execute(sql_statement, values)
                self.db_con.commit()
            finally:
                self.close()

    def select(self, *, table, columns, key_name=None, key_value=None,
               order_column=None, direction=None, limit=None):
        """Selects values from the passed column(s) from the specified table
        with optional constraints

        Args:
            table (str): Name of the table to be updated
            columns (list): Table column names to select
            key_name (str): Column name for the WHERE specification
            key_value: Variable type value for the WHERE specification
            order_column (str): Column name to order by
            direction (str): Order direction; should be ASC or DESC
            limit (int): Number of rows to select
        Returns:
            selected_values (list of tuples): Values returned by the sql query
        """
        with self.locks[self.database]:
            self.open()
            try:
                sql_value = None
                columns_text = ', '.join(f'{name}' for name in columns)
                sql_statement = f'SELECT {columns_text} FROM {table}'

                sql_value = None
                if key_name is not None and key_value is not None:
                    sql_statement += f' WHERE {key_name} = %s'
                    sql_value = [key_value]

                if order_column is not None and direction is not None:
                    sql_statement += f' ORDER BY {order_column} {direction}'

                if limit is not None:
                    sql_statement += f' LIMIT {limit}'

                if sql_value is not None:
                    self.db_cur.execute(sql_statement, sql_value)
                else:
                    self.db_cur.execute(sql_statement)

                selected_values = self.db_cur.fetchall()
                return selected_values
            finally:
                self.close()

    def execute_sql(self, sql_str):
        """Exectues the passed SQL string. This method is useful for creating
        or dropping a table in the database

        Args:
            sql_str (str): SQL string to execute
        """
        with self.locks[self.database]:
            self.open()
            try:
                self.db_cur.execute(sql_str)
                self.db_con.commit()
            finally:
                self.close()

    def _create_database(self):
        """Creates the database if it does not already exist"""
        with self.locks[self.database]:
            server_con = mysql.connector.connect(
                host=self.host,
                user=self.user,
                passwd=self.password,
            )
            server_cur = server_con.cursor()
            server_cur.execute(
                f'CREATE DATABASE IF NOT EXISTS {self.database}'
            )
            server_cur.close()
            server_con.close()
