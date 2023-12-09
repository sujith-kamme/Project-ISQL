import pandas as pd
import os
from tabulate import tabulate
import csv

def parse_input(input_string):
    tokens = input_string.split()
    #print(tokens)
    table_name = None
    column_names = None
    condition = None
    sort_column=None
    sort_op = 'asc'
    group_column = None
    group_condition=None
    left_table=None
    right_table=None
    join_type=None
    join_condition=None

    for i, token in enumerate(tokens):
        if token == "in" and i < len(tokens) - 1:
            table_name = tokens[i + 1]
        elif "join" in token and i < len(tokens) - 1:
            left_table = tokens[i-1]
            right_table = tokens[i+1]
            join_type="natural"
        elif token == "condition":
            join_condition = " ".join(tokens[i+1:])
        elif token == "print":
            column_names = tokens[i + 1:tokens.index("in")]
        elif token == "if" and i < len(tokens) - 1:
            condition = " ".join(tokens[i + 1:])
        elif token == "group" and i < len(tokens) - 1:
            group_column = tokens[i + 1]
        elif token == "with" and i < len(tokens) - 1:
            group_condition = " ".join(tokens[i + 1:])
        elif token == "sort" and i < len(tokens) - 1:
            sort_column = tokens[i + 1]
        elif token == "desc":
            sort_op='desc'

    if condition is not None:
        if "sort" in condition:
            t = condition.split()
            condition = " ".join(t[:t.index("sort")])
        if "group" in condition:
            t = condition.split()
            condition = " ".join(t[:t.index("group")])
    
    if group_condition is not None and "sort" in group_condition:
        t = group_condition.split()
        group_condition = " ".join(t[:t.index("sort")])

    if join_condition is not None:
        if "sort" in join_condition:
            t = join_condition.split()
            join_condition = " ".join(t[:t.index("sort")])
        if "group" in join_condition:
            t = join_condition.split()
            join_condition = " ".join(t[:t.index("group")])
        if "if" in join_condition:
            t = join_condition.split()
            join_condition = " ".join(t[:t.index("if")])

    return {
        'table': table_name,
        'columns': column_names,
        'condition': condition,
        'sort_column':sort_column,
        'sort_op': sort_op,
        'group_column': group_column,
        'group_condition':group_condition,
        'left_table':left_table,
        'right_table':right_table,
        'join_type':join_type,
        'join_condition': join_condition
    }


def find(file_paths, database,table, columns, condition=None, sort_column=None, sort_op='asc', group_column=None):
    output = []
    to_sort=[]
    for file_path in file_paths:
        full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "databases", database, table, file_path)
        temp_op = pd.read_csv(full_path)
        for column in columns:
            if column not in list(temp_op.columns) and column != "all":
                print(f"{column} column not found in the table")
                return None
        if condition:
            temp_op=isql_multi_filter(temp_op, condition)

        if "all" in columns:
            output.append(temp_op)
        else:
            temp_op=temp_op[columns]
            output.append(temp_op)
        
        if sort_column:
            sort_df = custom_sort(temp_op, sort_column)
            to_sort.append(sort_df)
        
    if sort_column:
        output_sort = merge_sorted_dataframes(to_sort,sort_column,'ascending')
        #else:
        #    output_sort = merge_sorted_dataframes(to_sort,sort_column,'descending')
        
        if sort_op == 'desc':
            columns = output_sort.columns.tolist()
            reversed_data = output_sort.values.tolist()[::-1]
            output_sort = pd.DataFrame(reversed_data, columns=columns)

        return output_sort
    
    return output

def aggregation(file_paths, database,table, column,condition=None):
    output=[]
    results=[]
    col=None
    op=None
    for file_path in file_paths:
        full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "databases", database, table, file_path)
        temp_op = pd.read_csv(full_path)
        #print(column)
        if condition:
            temp_op = isql_multi_filter(temp_op, condition)
        
        #print(column[0].split("."))
        col,op=column[0].split(".")
        if col not in temp_op.columns and col != "all":
            print(f"{col} column not found in the result.")
            return None
        
        if "all" in col:
            output.append(temp_op)
        else:
            temp_op=temp_op[col]
            output.append(temp_op)

        result = isql_aggregate(temp_op, col, op)
        #print(result)
        results.append(result)
    
    if len(results)==1:
        print("Result:")
        print(results[0])
    elif len(results)>1:
        print("Result:")
        if op=="count":
            output=sum(results)
        elif op=="sum":
            output=sum(results)
        elif op=="min":
            output=min(results)
        elif op=="max":
            output=max(results)
        elif op=="avg":
            output=sum(results)/len(results)
        print(output)

def isql_aggregate(records, column, function):
    if records is None or len(records) == 0:
        print("Empty records list")
        return None

    if isinstance(records, pd.DataFrame):
        records = records.to_dict(orient='records')
    if isinstance(records, pd.Series):
        records = records.to_frame().to_dict(orient='records')

    if not records:
        print("Empty records list after conversion")
        return None

    if function == 'count':
        return len(records)
    elif function == 'avg':
        total = sum(row[column] for row in records)
        return total / len(records)
    elif function == 'sum':
        return sum(row[column] for row in records)
    elif function == 'min':
        return min(row[column] for row in records)
    elif function == 'max':
        return max(row[column] for row in records)
    else:
        print(f"Unknown aggregation function: {function}")
        return None
    
def cnt(data):
    count=0
    for d in data:
        count+=1
    return count

def group_aggregation(file_paths, database, table, columns, condition=None, group_condition=None, group_column=None, sort_column=None,sort_op='asc'):
    grouped_data = {}

    for file_path in file_paths:
        full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "databases", database, table, file_path)
        temp_op = pd.read_csv(full_path)

        if temp_op is None:
            print(f"Error reading file: {full_path}")
            return

        if condition:
            temp_op = isql_multi_filter(temp_op, condition)

        if group_column:
            for index, row in temp_op.iterrows():
                group_key = row[group_column]
                if group_key not in grouped_data:
                    grouped_data[group_key] = []
                grouped_data[group_key].append(row.to_dict())
        else:
            
            grouped_data[None] = temp_op.to_dict(orient='records')
        
        result_df = pd.DataFrame()
        result={}
        for column in columns:
            if "." in column:
                col, op = column.split(".")
                result = isql_grp_aggregate(grouped_data, col, op)
                if group_column not in result_df:
                    result_df[group_column]=pd.Series(result.keys())
                result_df[column] = pd.Series(result.values())
        #print(result_df)
    
    if group_condition:
       result_df = isql_filter(result_df,group_condition)
        #having_col, having_op, having_value = condition.split()
        #having_col = having_col.strip()
        #having_op = having_op.strip()
        #having_value = float(having_value.strip())
        #addcode for >, <, >=,<=

    if sort_column:
        result_df=custom_sort(result_df,sort_column,(sort_op=='asc'))
    
    print("Result:")
    formatted_table = tabulate(result_df, headers='keys', tablefmt='psql')
    print(formatted_table)

def isql_grp_aggregate(grouped_data, column, function, chunk_size=3000):
    global_aggregate = {}

    for group_key, group_rows in grouped_data.items():
        if not group_rows:
            continue
        if function in ['avg','sum','min','max']:
            values = [float(row[column]) for row in group_rows]
        else:
            values=[row[column] for row in group_rows]
        num_chunks = len(values) // chunk_size + (len(values) % chunk_size > 0)

        local_aggregates = []

        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            chunk = values[start_idx:end_idx]

            if function == 'count':
                local_aggregate = len(chunk)
            elif function == 'avg':
                total = sum(chunk)
                local_aggregate = total / len(chunk) if len(chunk) > 0 else 0
            elif function == 'sum':
                local_aggregate = sum(chunk)
            elif function == 'min':
                local_aggregate = min(chunk) if len(chunk) > 0 else 0
            elif function == 'max':
                local_aggregate = max(chunk) if len(chunk) > 0 else 0
            else:
                print(f"Unknown aggregation function: {function}")
                return None

            local_aggregates.append(local_aggregate)

        if function == 'count':
            global_aggregate[group_key] = sum(local_aggregates)
        elif function == 'avg':
            total = sum(local_aggregates)
            global_aggregate[group_key] = total / len(local_aggregates) if len(local_aggregates) > 0 else 0
        elif function == 'sum':
            global_aggregate[group_key] = sum(local_aggregates)
        elif function == 'min':
            global_aggregate[group_key] = min(local_aggregates) if len(local_aggregates) > 0 else 0
        elif function == 'max':
            global_aggregate[group_key] = max(local_aggregates) if len(local_aggregates) > 0 else 0
        else:
            print(f"Unknown aggregation function: {function}")
            return None

    return global_aggregate

def join(left_file_paths, right_file_paths, database, columns, left_table, right_table,join_type,join_condition,condition=None, group_condition=None, group_column=None, sort_column=None,sort_op='asc'):
    left_df=read_csv_chunk(database,left_table,left_file_paths)
    right_df=read_csv_chunk(database,right_table,right_file_paths)
    #print(join_condition)
    left_join_column=join_condition.split()[0]
    right_join_column=join_condition.split()[-1]
    
    column=[]
    joined_df=pd.DataFrame()
    column.append(left_join_column)
    column.append(right_join_column)
    #print(column)
    if join_type == "natural":
        joined_df=natural_join(left_df,right_df,column)
    
    if condition:
        joined_df = isql_multi_filter(joined_df, condition)

    if "all" in columns:
        joined_df=joined_df
    else:
        joined_df=joined_df[columns]
    

    if sort_column:
        joined_df=custom_sort(joined_df,sort_column,(sort_op=='asc'))
    
    print("Result:")
    formatted_table = tabulate(joined_df, headers='keys', tablefmt='psql')
    print(formatted_table)



def read_csv_chunk(database_name, table_name, file_paths, chunk_size=3000):
    chunks = []
    for file_path in file_paths:
        full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "databases", database_name, table_name, file_path)
        with open(full_path, 'r', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            chunk = []
            for row in reader:
                chunk.append(row)
                if len(chunk) == chunk_size:
                    chunks.append(chunk)
                    chunk = []
            if chunk:
                chunks.append(chunk)
    return chunks

def natural_join(left_file, right_file, join_column):
    result = []
    left_join_col = join_column[0]
    right_join_col = join_column[-1]
    left_partitions = {}
    right_partitions = {}

    for left_chunk in left_file:
        for left_row in left_chunk:
            left_key = left_row[left_join_col]
            left_partitions.setdefault(left_key, []).append(left_row)

    for right_chunk in right_file:
        for right_row in right_chunk:
            right_key = right_row[right_join_col]
            right_partitions.setdefault(right_key, []).append(right_row)

    common_partitions = set(left_partitions.keys()).intersection(right_partitions.keys())
    for key in common_partitions:
        left_data = left_partitions[key]
        right_data = right_partitions[key]

        for left_row in left_data:
            for right_row in right_data:
                if left_row[left_join_col] == right_row[right_join_col]:
                    result.append({**left_row, **right_row})

    if result:
        result_df = pd.DataFrame(result)
        return result_df
    else:
        return pd.DataFrame()


def custom_sort(df, sort_column, ascending=True):
    records = df.to_dict(orient='records')

    n = len(records)
    for i in range(n - 1):
        for j in range(0, n - i - 1):
            current_value = records[j][sort_column]
            next_value = records[j + 1][sort_column]
            if (current_value > next_value and ascending) or (current_value < next_value and not ascending):
                records[j], records[j + 1] = records[j + 1], records[j]
    sorted_df = pd.DataFrame(records)

    return sorted_df

def merge_sorted_dataframes(sorted_dfs, sort_column, order='ascending'):
    result = []

    def compare(val1, val2):
        return val1 <= val2 if order == 'ascending' else val1 >= val2

    while any(not df.empty for df in sorted_dfs):
        min_df_idx = None
        min_value = None

        for idx, df in enumerate(sorted_dfs):
            if not df.empty:
                current_value = df.iloc[0][sort_column]
                if min_value is None or compare(current_value, min_value):
                    min_df_idx = idx
                    min_value = current_value

        result.append(sorted_dfs[min_df_idx].iloc[0].to_dict())
        sorted_dfs[min_df_idx] = sorted_dfs[min_df_idx].iloc[1:]
        sorted_dfs = [df for df in sorted_dfs if not df.empty]
    result_df = pd.DataFrame(result)

    return result_df

def print_isql(dataframes):
    if dataframes is None or any(df is None for df in dataframes):
        print("Error: Unable to generate output.")
        return
    if isinstance(dataframes, pd.DataFrame):
        dataframes = [dataframes]
    if len(dataframes)==1:
        print("Result:")
        result_df=dataframes[0]
        formatted_table = tabulate(result_df, headers='keys', tablefmt='psql')
        print(formatted_table)

    else:
        result_df = pd.concat(dataframes, ignore_index=True)
        formatted_table = tabulate(result_df, headers='keys', tablefmt='psql')
        if result_df.empty:
            print("No records found.")
        elif result_df is None:
            print("Some error encountered with column name")
        else:
            print("Result:")
            print(formatted_table)
            #print(result_df)

def isql_multi_filter(df, condition):
    if "and" in condition:
        first, second = condition.split("and")
        temp_df=isql_filter(df,first)
        result_df= isql_filter(temp_df,second)
        return result_df
    
    elif "or" in condition:
        first, second = condition.split("or")
        df1=isql_filter(df, first)
        df2=isql_filter(df, second)
        result_df = pd.concat([df1, df2], ignore_index=True)
        return result_df
    else:
        return isql_filter(df, condition)

def isql_filter(df, condition, chunk_size=3000):
    parts = condition.split()
    col = parts[0]
    op = parts[1]
    value = ' '.join(parts[2:])

    if value.startswith(("'", '"')) and value.endswith(("'", '"')):
        value = value[1:-1]

    try:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            value = pd.to_datetime(value)
        else:
            value = df[col].iloc[0].__class__(value)
    except ValueError:
        print(f"Value conversion failed for column '{col}' and value '{value}'")

    if col not in list(df.columns):
        print(f"{col} not found in the table")
        return None

    result_dfs = []
    for i in range(0, len(df), chunk_size):
        chunk = df[i:i + chunk_size]
        
        rows = []
        for index, row in chunk.iterrows():
            if op == '=' and row[col] == value:
                rows.append(row.tolist())
            elif op == '>' and row[col] > value:
                rows.append(row.tolist())
            elif op == '>=' and row[col] >= value:
                rows.append(row.tolist())
            elif op == '<' and row[col] < value:
                rows.append(row.tolist())
            elif op == '<=' and row[col] <= value:
                rows.append(row.tolist())
            elif op == '!=' and row[col] != value:
                rows.append(row.tolist())
            elif op == 'asin' and value in row[col]:
                rows.append(row.tolist())
        
        if rows:
            result_df = pd.DataFrame(rows, columns=chunk.columns)
            result_dfs.append(result_df)

    if result_dfs:
        result_df = pd.concat(result_dfs, ignore_index=True)
        return result_df
    else:
        return pd.DataFrame(columns=df.columns)
 