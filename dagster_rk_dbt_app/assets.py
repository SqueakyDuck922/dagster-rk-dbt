import dagster as dg

import pandas as pd

import pyodbc


@dg.asset
def processed_data():
    ## Read data from the CSV
    df = pd.read_csv("data/sample_data.csv")

    ## Add an age_group column based on the value of age
    df["age_group"] = pd.cut(
        df["age"], bins=[0, 30, 40, 100], labels=["Young", "Middle", "Senior"]
    )

    ## Save processed data
    df.to_csv("data/processed_data.csv", index=False)
    return "Data loaded successfully"


@dg.asset
def products():
    """Reads a flat file and returns it as a DataFrame."""
    file_path = "data/products.csv"
    df = pd.read_csv(file_path)


    connString = "Driver={ODBC Driver 18 for SQL Server};Server=127.0.0.1,1007;Database=securities_masterC2;uid=sa;pwd=Badger99;TrustServerCertificate=yes"  
    conn =  pyodbc.connect(connString) 


    try:

        # SQLCommand = "select top 10 * from engine_log"

        # to create table:
        # create table test_table(col1 int)

        SQLCommand = """
        insert into test_table(col1)
        values(666)
        """


        cursor = conn.cursor()
        cursor.execute(SQLCommand)

         # could potentially load in csv something like below, where df is from csv as above

        #  with pyodbc.connect(connection_string) as conn:
        #     cursor = conn.cursor()
        #     table_name = "your_table_name"
        #     context.log.info(f"Loading data into table: {table_name}")

        #     # Insert data into the table
        #     for _, row in df.iterrows():
        #         cursor.execute(
        #             f"INSERT INTO {table_name} (column1, column2, column3) VALUES (?, ?, ?)",
        #             row["column1"], row["column2"], row["column3"]
        #         )
        #     conn.commit()


    except Exception as e:
        # context.log.error(f"Error loading data to SQL Server: {e}")
        raise


    finally:          
        conn.commit()

    