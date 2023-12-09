import os
import json

DATABASE_INFO_FILE = os.path.join(os.path.dirname(__file__), "..", "databases","db_schema.json")

def load_database_info():
    if os.path.exists(DATABASE_INFO_FILE):
        with open(DATABASE_INFO_FILE, 'r') as info_file:
            return json.load(info_file)
    else:
        return {}

def save_database_info(database_info):
    with open(DATABASE_INFO_FILE, 'w') as info_file:
        json.dump(database_info, info_file, indent=4)

def get_databases():
    with open(DATABASE_INFO_FILE, 'r') as info_file:
        d=json.load(info_file)
        return d

def create_database(database_name):
    scripts_path = os.path.join(os.path.dirname(__file__), "..", "databases")
    database_path = os.path.join(scripts_path, database_name)

    if os.path.exists(database_path):
        raise FileExistsError(f"Database '{database_name}' already exists.")
    os.makedirs(database_path)
    print(f"Database '{database_name}' created successfully.")
    database_info = load_database_info()
    database_info[database_name] = {}
    save_database_info(database_info)

def create_table(database_name, table_name, columns):
    if database_name not in load_database_info():
        raise ValueError(f"Database '{database_name}' does not exist.")

    table = os.path.join(os.path.dirname(__file__), "..", "databases",database_name, table_name)
    os.makedirs(table)
    table_path = os.path.join(os.path.dirname(__file__), "..", "databases",database_name, table_name, f"{table_name}.csv")
    if os.path.exists(table_path):
        raise FileExistsError(f"Table '{table_name}' already exists in database '{database_name}'.")

    with open(table_path, 'w') as table_file:
        table_file.write(columns + '\n')
    print(f"Table '{table_name}' created successfully in database '{database_name}'.")
    database_info = load_database_info()
    database_info[database_name][table_name]= [f"{table_name}.csv"]
    save_database_info(database_info)


def parse_user_input(user_input):
    parts = user_input.split()
    action = parts[0].lower()
    pk=None

    if action == 'add' and len(parts)>=3:
        table_name=parts[2]
        values = ' '.join(parts[3:]).replace("'", "").replace("\"", "")
        return {
            'operator':action,
            "table" : table_name,
            "values" : values,
        }
    elif action == 'set' and len(parts)>=10:
        set_column = parts[1]
        set_value = ' '.join(parts[3:parts.index("in")]).replace("'", "").replace("\"", "")
        table_name = parts[parts.index("in")+1]
        condition_column = parts[parts.index("if")+1]
        condition_op= parts[parts.index("if")+2]
        condition_value = ' '.join(parts[parts.index("if")+3:]).replace("'", "").replace("\"", "")
        return {
            'operator':action,
            'table': table_name,
            'set_column': set_column,
            'set_value': set_value,
            'condition_column':condition_column,
            'condition_op':condition_op,
            'condition_value':condition_value
        }
    elif action == 'del' and len(parts)>=7:
        table_name=parts[2]
        condition_column=parts[4]
        condition_op=parts[5]
        condition_value=' '.join(parts[6:]).replace("'", "").replace("\"", "")
        return {
            'operator':action,
            'table': table_name,
            'condition_column':condition_column,
            'condition_op':condition_op,
            'condition_value':condition_value
        }

def get_columns_from_file(table_path):
    with open(table_path, 'r') as table_file:
        header_line = table_file.readline().strip()
        return header_line.split(',')


def add_record(database_name, table_name, values):
    table_path = os.path.join(os.path.dirname(__file__), "..", "databases", database_name, table_name, f"{table_name}.csv")
    values_list = values.split(',')

    with open(table_path, 'a') as table_file:
        table_file.write(','.join(values_list) + '\n')

    print(f"Record added successfully to table '{table_name}'.")


def update_record(database_name, table_name, set_column, set_value, condition_column, condition_operator, condition_value):
    table_path = os.path.join(os.path.dirname(__file__), "..", "databases", database_name, table_name, f"{table_name}.csv")
    
    columns = get_columns_from_file(table_path)

    with open(table_path, 'r') as table_file:
        lines = table_file.readlines()

    for i in range(1, len(lines)):  
        record = lines[i].strip().split(',')
        if condition_operator == '=' and record[columns.index(condition_column)] == condition_value:
            record[columns.index(set_column)] = set_value
            lines[i] = ','.join(record) + '\n'
        elif condition_operator == '>' and record[columns.index(condition_column)] > condition_value:
            record[columns.index(set_column)] = set_value
            lines[i] = ','.join(record) + '\n'
        elif condition_operator == '<' and record[columns.index(condition_column)] < condition_value:
            record[columns.index(set_column)] = set_value
            lines[i] = ','.join(record) + '\n'
        elif condition_operator == '>=' and record[columns.index(condition_column)] >= condition_value:
            record[columns.index(set_column)] = set_value
            lines[i] = ','.join(record) + '\n'
        elif condition_operator == '<=' and record[columns.index(condition_column)] <= condition_value:
            record[columns.index(set_column)] = set_value
            lines[i] = ','.join(record) + '\n'
        elif condition_operator == '!=' and record[columns.index(condition_column)] != condition_value:
            record[columns.index(set_column)] = set_value
            lines[i] = ','.join(record) + '\n'

    with open(table_path, 'w') as table_file:
        table_file.writelines(lines)

    print(f"Records updated successfully in table '{table_name}'.")

def delete_record(database_name, table_name, condition_column, condition_operator, condition_value):
    table_path = os.path.join(os.path.dirname(__file__), "..", "databases", database_name, table_name, f"{table_name}.csv")
    columns = get_columns_from_file(table_path)

    with open(table_path, 'r') as table_file:
        lines = table_file.readlines()

    lines = [lines[0]]+[line for line in lines[1:] if not evaluate_condition(line.strip().split(','), condition_column, condition_operator, condition_value,columns)]

    with open(table_path, 'w') as table_file:
        table_file.writelines(lines)

    print(f"Records deleted successfully from table '{table_name}'.")

def evaluate_condition(record, condition_column, condition_operator, condition_value,columns):

    column_value = record[columns.index(condition_column)]

    if condition_operator == '=' and column_value == condition_value:
        return True
    elif condition_operator == '>' and column_value > condition_value:
        return True
    elif condition_operator == '<' and column_value < condition_value:
        return True
    elif condition_operator == '>=' and column_value >= condition_value:
        return True
    elif condition_operator == '<=' and column_value <= condition_value:
        return True
    elif condition_operator == '!=' and column_value != condition_value:
        return True
    else:
        return False