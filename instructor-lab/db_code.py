import sqlite3
import pandas as pd

conn=sqlite3.connect('STAFF.db')

table_name = 'INSTRUCTOR'
attribute_list=['ID','FNAME','LNAME','CITY','CCODE']


file_path='/home/project/instructor-lab/INSTRUCTOR.csv'
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

# assignment

table_name2='Departments'
attribute_list2=['DEPT_ID','DEP_NAME','MANAGER_ID','LOC_ID']

csv_path = "/home/project/instructor-lab/Departments.csv"
df1=pd.read_csv(csv_path,names=attribute_list2)

df1.to_sql(table_name2, conn, if_exists = 'replace', index =False)
print('Table is ready')

new_department = {
    'DEPT_ID': [9],
    'DEP_NAME': ['Quality Assurance'],
    'MANAGER_ID': [30010],
    'LOC_ID': ['L0010']
}
data_appended = pd.DataFrame(new_department)
data_appended.to_sql(table_name2, conn, if_exists = 'append', index =False)
print('Data appended successfully')

query2 = f"SELECT * FROM {table_name2}"
query2_output = pd.read_sql(query2, conn)
print(query2)
print(query2_output)

conn.close()