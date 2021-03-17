import pandas as pd
import re
import psycopg2

varchar_type = 'varchar'
int_type = 'int8'
float_type = 'float'
timestamp_type = 'timestamptz'

def retrieve_data():
    csv_frame = pd.read_csv('.\data_v1.csv', sep=';')

    print(csv_frame)

    conn = psycopg2.connect(
                        host="localhost",
                        database="evaluatordb",
                        user="evaluator",
                        password="comparison419")

    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS data;')

    query = """
            CREATE TABLE data (
            """

    column_types = []
    for i in range(len(csv_frame.columns)):
        column_type = varchar_type
        val = csv_frame[csv_frame.columns[i]][i]
        if is_int(val):
            column_type = int_type
        if is_float(val):
            column_type = float_type
        elif is_timestamp(val):
            column_type = timestamp_type
        query += """
                """ + csv_frame.columns[i] + ' ' + column_type
##        if i == 0:
##            query += """ primary key,
##            """
        if i < len(csv_frame.columns) - 1:
            query += """,
            """
        else:
            query += """
            """
        column_types.append(column_type)
    query += """
            );
            """
    cursor.execute(query)

    names_list = ''
    for i in range(len(csv_frame.columns) - 1):
        names_list += csv_frame.columns[i] + ','
    names_list += csv_frame.columns[len(csv_frame.columns) - 1]

    for row in csv_frame.itertuples():
        values_list = ''
        for i in range(1, len(row) - 1):
            if column_types[i - 1] == timestamp_type or column_types[i - 1] == varchar_type:
                values_list += '\'' + str(row[i]) + '\','
            else:
                values_list += str(row[i]) + ','
        if column_types[len(column_types) - 1] == timestamp_type or column_types[len(column_types) - 1] == varchar_type:
            values_list += '\'' + str(row[len(row) - 1]) + '\''
        else:
            values_list += str(row[len(row) - 1])
        cursor.execute("""
                    INSERT INTO data(""" + names_list + """)
                    VALUES (""" + values_list + """);
                    """
                    )

    conn.commit()

def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def is_float(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def is_timestamp(s):
    return re.match('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d*\+\d{2}', s)

retrieve_data()
