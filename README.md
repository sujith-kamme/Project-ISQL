# Project-ISQL
Custom Query Language for local databases

Project ISQL is aimed to develop a custom query language for querying a local database of files. This query language draws inspiration from SQL but incorporates refreshed new-age syntax and incorporates querying capability features to file based databases stored locally. ISQL uses relational data model i.e., Data will be stored in the form of tables. It supports projection (selecting a subset of rows), filtering (selecting rows), join (e.g., combining multiple data tables), grouping, aggregation, and ordering. In addition, it also supports creating databases and tables, support CRUD operation on the data. One of the key aspects of project ISQL is its efficient handling of large datasets by implementing a chunk-based data processing approach. Unlike traditional database systems that load the entire dataset into memory, ISQL processes data in manageable chunks. This design choice is to ensure scalability of the database system. Project ISQL is made accessible to users using the CLI application.

<img width="327" alt="image" src="https://github.com/sujith-kamme/Project-ISQL/assets/142932988/68c9a365-5486-4687-9b59-8147d0696e03">

**Architecture design**

<img width="350" alt="image" src="https://github.com/sujith-kamme/Project-ISQL/assets/142932988/ca297286-95de-4687-81b3-3ed4cefa8060">


**Process Flow Digram**

<img width="274" alt="image" src="https://github.com/sujith-kamme/Project-ISQL/assets/142932988/7e24ff0e-146a-4277-9988-a4e704f75469">


### **To run**
To run the project, please go through the following instructions.
1. Install dependencies
  •	pandas
  •	OS
  •	tabulate: (pip3 install tabulate)
  •	JSON
  •	CSV
2. Caution: Make no changes in the directory structure as it might affect the code.
3. Run main.py file. After successful run, application starts and you'll be greeted with "Welcome to ISQL".
 

Here are some sample queries:
#command to display all databases
display databases

#command to create db
create-database dsci

#command to switch database(can only happen once. If database is already selected then, you have to re-run the application again to switch db)
goto dsci

#command to create table
create-table sample id,name,gender

#command to add records
add in sample 1,Sujith,M
add in sample 2,"John Smith",M

#command to update records
set name = "Sujith Kamme" in sample if id = 1

#command to delete records
del in sample if name = "John Smith"

#commands for projection, filtering, joining, grouping, agregation and ordering
print all in movie
print movie_id movie_title in movie
print movie_id in movie if popularity > 5
print movie_id in movie if genres asin Crime and popularity > 1
print movie_id genres in movie if genres asin Action or genres asin Adventure
print all.count in movie
print popularity.min in movie
print popularity.max in movie
print popularity.avg in movie if runtime > 100
print movie_title original_language in movie sort movie_title
print movie_title in movie if popularity > 5 sort movie_title
print username num_reviews in users if num_reviews > 4000 sort num_reviews desc
print movie_id rating_val.count in ratings group movie_id
print original_language movie_id.count in movie group original_language with movie_id.count > 10
print original_language movie_id.count in movie group original_language with movie_id.count > 10 sort movie_id.count
print original_language movie_id.count in movie if genres asin Action group original_language with movie_id.count > 10 sort movie_id.count
print movie_id rating_val in movie join ratings condition movie_id = movie_id
print movie_id rating_val in movie join ratings condition movie_id = movie_id sort movie_id


**Important instructions** 
1. Space is mandatory between keywords, operators, and conditions or else the code would fail.
2. ISQL tolerates not using quotes for a single word string.  For string that contains more than one word, quotes are mandatory.
3. Each time you exit and re-run the application, you would have to use “goto <database name>” or else application halts.
