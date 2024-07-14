"""The script for loading the saved csv file into duckdb database"""

import duckdb

# Create an in-memory database
con = duckdb.connect()

# This is a special syntax in duckdb, I do not know if
# other ORM libraries of python or in-memory databases supports this feature
con.execute(
    "CREATE TABLE pokedex_table AS SELECT * FROM read_csv('scraped_pokedex.csv', header=True);"
    )

# The following line codes below works the same as the code above
# but I find the code above a lot more efficient in this use-case
# since I am not adding any calculated or aggregated columns and just
# simply loading the contents of a csv file into a database.

# Create a table with the correct schema (adjust column names and types as needed)
# con.execute("""
# CREATE TABLE my_table (
#     column1 INTEGER,
#     column2 VARCHAR,
#     column3 FLOAT
# );
# """)

# Copy data from the CSV file
# con.execute("COPY my_table FROM 'your_csv_file.csv' (HEADER TRUE, DELIMITER ',');")
