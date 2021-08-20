# database-helper
Python classes that simplify working with SQLite3 and MySQL databases by making all database calls thread-safe and always properly closing the database connection.


# Usage
```python
from datetime import datetime
from database_helper import SQLite3

def example():
    """An example function"""
    database = SQLite3('my_db_name.db')

    create_table_sql = (
        """CREATE TABLE IF NOT EXISTS my_table
           id INTEGER PRIMARY KEY NOT NULL,
           column1 TEXT NOT NULL,
           column2 TEXT NOT NULL,
           column3 TEXT NOT NULL,
           column4 TEXT NOT NULL,
           entry_time DATETIME NOT NULL);"""
    )
    database.execute_sql(create_table_sql)

    # Insert some details
    some_details = {
        'column1': 'this',
        'column2': 'is',
        'column3': 'an',
        'column4': 'example',
        'entry_time': datetime.now().isoformat(sep=' ', timespec='seconds')
    }
    row_id = database.insert(table='my_table', details=some_details)

    # Query the database
    output = database.select(table='my_table', columns=['column4'], key_name='id', key_value=row_id)
    print(output[0][0])  # example

    # Update the database
    new_details = {
        'column4': 'illustration',
        'entry_time': datetime.now().isoformat(sep=' ', timespec='seconds')
    }
    database.update(table='my_table', details=new_details, key_name='id', key_value=row_id)

    # Verify the update worked
    new_output = database.select(table='my_table', columns=['column4'], key_name='id', key_value=row_id)
    print(new_output[0][0])  # illustration'
```
