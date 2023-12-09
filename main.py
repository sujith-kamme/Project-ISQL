from functions import functions
from functions import create

flag = False
print('''Welcome to
 ___  ____    ___   _     
 |_ _|/ ___|  / _ \ | |    
  | | \___ \ | | | || |    
  | |  ___) || |_| || |___ 
 |___||____/  \__\_\|_____| version 1.1
      
To exit the application, please type "exit" or "quit".
      ''')

while True:
    user_input = str(input("ISQL> ").strip())
    databases = create.get_databases()
    if user_input == 'exit' or user_input== 'quit':
        print("Exiting ISQL..")
        break

    if user_input == "display databases":
        print("Available databases are:")
        for x in databases.keys():
            print(x)
        continue    
    elif "goto" in user_input:
        keyword, db_name = user_input.split()
        if db_name not in databases.keys():
            print("Database not available")
        else:
            database_name= db_name
            db=databases[db_name]
            print(f"Database changed to {db_name}")
        flag = True
        break

    elif "query" in user_input:
        database_name="movies" # taking default database as movie
        db= databases["movies"] 
        print("Database movies selected by default.")
        flag = True
        break
    
    elif "create-database" in user_input:
        keyword, db_name = user_input.split()
        create.create_database(db_name)
        continue

if flag:
    while True:
        user_input = str(input(f"{database_name}> ").strip())
        if user_input == 'exit'or user_input== 'quit':
            print("Exiting ISQL..")
            break

        if "create-table" in user_input:
            command, table_name, columns= user_input.split()
            create.create_table(database_name, table_name, columns)
            databases = create.get_databases()
            db=databases[database_name]
            continue
        if "add" in user_input or "set" in user_input or "del" in user_input:
            inp=create.parse_user_input(user_input)
            #print(inp)
            if inp['operator'] == 'add':
                create.add_record(database_name, inp['table'],inp['values'])
            elif inp['operator'] == 'set':
                create.update_record(database_name,inp['table'], inp['set_column'], inp['set_value'], inp['condition_column'], inp['condition_op'], inp['condition_value'])
            elif inp['operator'] == 'del':
                create.delete_record(database_name,inp['table'], inp['condition_column'], inp['condition_op'], inp['condition_value'])
            else:
                print("Check your input and try again.")
            continue
        #taking default db as movies
        d=functions.parse_input(user_input)
        #print(d)
        if d["table"] not in db.keys():
            print("Table Not found. Please enter accurate table name.")
        else:
            if d["join_type"] is not None:
                functions.join(db[d["left_table"]],db[d["right_table"]],database_name,d['columns'],d['left_table'],d['right_table'],d['join_type'],d['join_condition'],d['condition'],d["group_condition"],d["group_column"],d["sort_column"],d["sort_op"])
            elif "." in d["columns"][0]:
                if d["group_column"] is None:
                    functions.aggregation(db[d["table"]], database_name,d["table"],d['columns'],d["condition"])
            elif d["group_column"] is not None:
                functions.group_aggregation(db[d["table"]], database_name,d["table"],d['columns'],d['condition'],d["group_condition"],d["group_column"],d["sort_column"],d["sort_op"])
            else:
                temp=functions.find(db[d["table"]], database_name,d["table"], d["columns"], d["condition"], d["sort_column"], d["sort_op"])
                functions.print_isql(temp)
    