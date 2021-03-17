import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
from matplotlib import pyplot as plt
from pathlib import Path

conn = psycopg2.connect(
                        host="localhost",
                        database="evaluatordb",
                        user="evaluator",
                        password="comparison419")
query = "select * from data;"
sql_frame = sqlio.read_sql_query(query, conn)

col_first_date = "first_date"
col_last_date = "last_date"
col_amount = "amount"
col_value_1 = "value_1"
col_value_2 = "value_2"

col_categories = []

for col in sql_frame.columns:
    if col.startswith("category_"):
        col_categories.append(col)

def render_single_and_double_graphs():
    for category in col_categories:
        print(category)
        category_frame = get_categories(category)
        for index, row in category_frame.iterrows():
            label = str(row[category])
            x, y = get_slice(category, label)
            render_graph(category, label, x, y)
            for category_2 in col_categories:
                if category_2 != category:
                    category_2_frame = get_categories(category_2)
                    for index_2, row_2 in category_2_frame.iterrows():
                        label_2 = str(row_2[category_2])
                        x, y = get_double_slice(category, label, category_2, label_2)
                        render_double_graph(category, label, category_2, label_2, x, y)

def get_categories(category):
    query = "select distinct " + category + " from data;"
    return sqlio.read_sql_query(query, conn)

def get_slice(category, label):
    x = []
    y = []
    print("  " + label)
    query = "select * from data where " + category + "=\'" + label + "\' order by " + col_first_date + ";"
    slice_frame = sqlio.read_sql_query(query, conn)
    for index, row in slice_frame.iterrows():
        x.append(row[col_first_date])
        y.append(row[col_value_1])
    return x, y

def get_double_slice(category, label, category_2, label_2):
    x = []
    y = []
    query = "select * from data where " + category + "=\'" + label + "\' and " + category_2 + "=\'" + label_2 + "\' order by " + col_first_date + ";"
    slice_frame = sqlio.read_sql_query(query, conn)
    for index, row in slice_frame.iterrows():
        x.append(row[col_first_date])
        y.append(row[col_value_1])
    return x, y

def render_graph(category, label, x, y):
    if len(x) > 1:
        plot_x_y(x, y)
        Path("graphs/" + category).mkdir(parents=True, exist_ok=True)
        plt.savefig("graphs/" + category + "/" + label + ".png")

def render_double_graph(category, label, category_2, label_2, x, y):
    if len(x) > 1:
        plot_x_y(x, y)
        Path("graphs/" + category + "__" + category_2).mkdir(parents=True, exist_ok=True)
        plt.savefig("graphs/" + category + "__" + category_2 + "/" + label + "__" + label_2 + ".png")

def plot_x_y(x, y):
    plt.clf()
    plt.plot(x, y)
    plt.xticks(rotation = 45)
    plt.gcf().subplots_adjust(bottom=0.2)

render_single_and_double_graphs()
