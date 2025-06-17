#Task 6.1
#Create 3 Dataframes for csv files 'sales.csv', 'houses.csv', 'employees.csv'
from itertools import count
from unittest.mock import inplace

import pandas as pd
import numpy as np
import pandavro as pdx
from numpy.ma.extras import average

SALES=r"C:\Users\acer\Desktop\Data Analytics Engineering Training\Python-Course\Session_6\source_files\sales.csv"
HOUSES=r"C:\Users\acer\Desktop\Data Analytics Engineering Training\Python-Course\Session_6\source_files\houses.csv"
EMPLOYEES=r"C:\Users\acer\Desktop\Data Analytics Engineering Training\Python-Course\Session_6\source_files\employees.csv"
"""with open(SALES) as s, open(HOUSES) as h, open(EMPLOYEES) as e:
    df_SALES = pd.read_csv(s)
    df_HOUSES = pd.read_csv(h)
    df_EMPLOYEES = pd.read_csv(e)"""

df_SALES=pd.read_csv(SALES)
df_HOUSES=pd.read_csv(HOUSES)
df_EMPLOYEES=pd.read_csv(EMPLOYEES)

#Task 6.2
#Extract employee first name / last name from 3 to 10 rows (including) from Employees

# Show all rows and columns (adjust max_rows and max_columns if needed)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)         # Prevent line wrapping
pd.set_option('display.max_colwidth', None)  # Show full column contents
print(df_EMPLOYEES.head())
employees=df_EMPLOYEES.loc[3:10,'EMP_FIRST_NAME':'EMP_LAST_NAME']
print( employees)

#Task 6.3
#Get amount of men / women among all employees (Please use function value_counts())

df_new=df_EMPLOYEES[df_EMPLOYEES['EMP_GENDER']=='F']
print(df_new.head())

df_new2=df_EMPLOYEES[df_EMPLOYEES.loc[:, 'EMP_GENDER'] == 'F']
print(df_new2.head())


counts=df_EMPLOYEES.value_counts(df_EMPLOYEES['EMP_GENDER'])
print(counts)

#Task 6.4
#Fill empty cells by 0 in column "square" in houses dataframe

print(df_HOUSES[df_HOUSES["SQUARE"].isna()].head())
df1=df_HOUSES[df_HOUSES["SQUARE"].isna()].head()
indices=df1.index
print(indices)

df_HOUSES.replace('', 0, inplace=True)  # Replace empty strings with 0
df_HOUSES.fillna(0, inplace=True)       # Replace NaN with 0


print(df_HOUSES.loc[indices,:])

#Task 6.5
#Create new column "unit_price" for price^1m2 (Please use the following formula: price / square) + round the result to 2 digits after point.
df_HOUSES["UNIT_PRICE"]=round(df_HOUSES['PRICE']/df_HOUSES['SQUARE'],2)

print(df_HOUSES.head())
print(df_HOUSES.loc[indices,:])

#Task 6.6
#Sort houses dataframe by price descending and put the result to json file.
df_HOUSES.sort_values(by='PRICE', ascending=False, inplace=True)
print(df_HOUSES.head())
df_HOUSES.to_json('task_6.6.json')

#Task 6.7
#Please filter employees dataframe and find how much women with name 'Vera' we have

df=df_EMPLOYEES[(df_EMPLOYEES['EMP_FIRST_NAME']=='Vera') & (df_EMPLOYEES['EMP_GENDER']=='F') ]
print(df)
print(len(df))#1

#Task 6.8
#Please count how many houses do we have with square >= 100 m2, group by category and subcategory.

df_filtered=df_HOUSES[df_HOUSES['SQUARE']>=100]
print(len( df_filtered))
print( df_filtered.groupby('HOUSE_CATEGORY'))

# Group by HOUSE_CATEGORY and HOUSE_SUBCATEGORY and count houses
house_counts = df_filtered.groupby(['HOUSE_CATEGORY', 'HOUSE_SUBCATEGORY']).size()

print(house_counts)

#Task 6.9
#Put the result from task 7 to file '.avro'

vera_count = df_EMPLOYEES[(df_EMPLOYEES['EMP_FIRST_NAME'] == 'Vera') & (df_EMPLOYEES['EMP_GENDER'] == 'F')]['EMP_FIRST_NAME'].count()
# Create a DataFrame with that count as a column
df_result = pd.DataFrame({'vera_count': [vera_count]})

print(df_result)
pdx.to_avro('task_6.9.avro',df_result)


#Task 6.10
#Please update "sales_amount"" column according to the following rule "sale_amount" = "sale_amount" + avg(sale_amount) * 0.02
# (Please try to use apply and lambda for it)
print(df_SALES.head())

# Compute average
avg = df_SALES['SALEAMOUNT'].mean()

# Define the function
func = lambda i: i + avg * 0.02

# Apply and assign back
df_SALES['SALEAMOUNT'] = df_SALES['SALEAMOUNT'].apply(func)

# Print result
print(df_SALES.head())
print(df_HOUSES.head())

#Task 6.11
#Please find all houses that are unsold yet (exists in houses but does not exist in sales, join by house_id).
#Put house ids to .json ('output_files/task_11.json'). Put result list of unique house names into house_ids_available list.

join_houses = df_HOUSES.set_index('HOUSE_ID').join(
    df_SALES.set_index('HOUSE_ID'),
    how='left',
    lsuffix='_house',  # suffix for df_HOUSES overlapping columns
    rsuffix='_sale'    # suffix for df_SALES overlapping columns
)


print(join_houses[join_houses['LOCATION_ID_sale'].isna()].shape[0])
indices=join_houses[join_houses['LOCATION_ID_sale'].isna()].index
print(join_houses.loc[indices,:].head())
unsold_houses=join_houses.loc[indices,:]
house_ids = unsold_houses.index.to_frame(index=False)
print(house_ids.head())

house_ids.to_json('task_6.11')

house_ids_available = list(unsold_houses.index)


#Task 6.12 (OPTIONAL)
#Please find sum of sales_amount by each employee, put the result to excel (emp_id, emp_first_name, emp_last_name, sum(sales_amount))
# and mark employees who have less than average sales_amount with red.

print(df_EMPLOYEES.head())
print(df_SALES.head())

joined_employees=df_EMPLOYEES.set_index('EMP_ID').join(df_SALES.set_index('EMP_ID'), how='inner',lsuffix='emp',rsuffix='sales')
columns_to_show=["EMP_FIRST_NAME","EMP_LAST_NAME"]



print(joined_employees.groupby(['EMP_ID','EMP_FIRST_NAME','EMP_LAST_NAME'])['SALEAMOUNT'].sum().reset_index(name='sum(sales_amount)').head())
grouped_employees=joined_employees.groupby(['EMP_ID','EMP_FIRST_NAME','EMP_LAST_NAME'])['SALEAMOUNT'].sum().reset_index(name='sum(sales_amount)')

average_sales_per_employee=grouped_employees['sum(sales_amount)'].mean()

# Define Excel writer
with pd.ExcelWriter('sales_summary.xlsx', engine='xlsxwriter') as writer:
    grouped_employees.to_excel(writer, index=False, sheet_name='Summary')

    workbook = writer.book
    worksheet = writer.sheets['Summary']

    # Define red format
    red_format = workbook.add_format({'bg_color': '#FFC7CE'})

    # Get column index of 'sum(sales_amount)'
    col_idx = grouped_employees.columns.get_loc('sum(sales_amount)')

    # Apply conditional formatting to that column
    worksheet.conditional_format(
        1, col_idx, len(grouped_employees), col_idx,
        {
            'type': 'cell',
            'criteria': '<',
            'value': average_sales_per_employee,
            'format': red_format
        }
    )
