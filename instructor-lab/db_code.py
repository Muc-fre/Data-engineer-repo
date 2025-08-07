import sqlite3
import pandas as pd

conn=sqlite3.connect('STAFF.db')

table_name = 'INSTRUCTOR'
attribute_list=['ID','FNAME','LNAME','CITY','CCODE']

file_path='/home/project/INSTRUCTOR.csv'
df=pd.read_csv(file_path,names=attribute_list)

df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print('Table is ready')

# Now, run the following tasks for data retrieval on the created database.
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Viewing only FNAME column of data.
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Viewing the total number of entries in the table.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Now try appending some data to the table. Consider the following.

data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)

#Now use the following statement to append the data to the INSTRUCTOR table.

data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully')


conn.close()