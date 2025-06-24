import pyodbc

connection_string = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=jxc9629.database.windows.net;"
    f"DATABASE=earthquake_db;"
    f"UID=username@jxc9629;"
    f"PWD=Athene2257;"
    "Encrypt=yes;TrustServerCertificate=no; Connection Timeout=30;"
)

with pyodbc.connect(connection_string) as conn:
    cursor = conn.cursor()

    cursor.execute("SELECT TOP 1 * FROM earthquakes_511610")
    tables = cursor.fetchall()
    for table in tables:
        print(table)

    # cursor.execute("SELECT TOP 10 * FROM earthquakes WHERE mag >= 6 ORDER BY mag DESC")
    # tables = cursor.fetchall()
    # for table in tables:
    #     print(table)

    # cursor.execute("SELECT * FROM earthquakes WHERE id = '654321'")
    # tables = cursor.fetchall()
    # for table in tables:
    #     print(table)