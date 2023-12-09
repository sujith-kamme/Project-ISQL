# Project-ISQL
Custom Query Language for local databases

Project ISQL is aimed to develop a custom query language for querying a local database of files. This query language draws inspiration from SQL but incorporates refreshed new-age syntax and incorporates querying capability features to file based databases stored locally. ISQL uses relational data model i.e., Data will be stored in the form of tables. It supports projection (selecting a subset of rows), filtering (selecting rows), join (e.g., combining multiple data tables), grouping, aggregation, and ordering. In addition, it also supports creating databases and tables, support CRUD operation on the data. One of the key aspects of project ISQL is its efficient handling of large datasets by implementing a chunk-based data processing approach. Unlike traditional database systems that load the entire dataset into memory, ISQL processes data in manageable chunks. This design choice is to ensure scalability of the database system. Project ISQL is made accessible to users using the CLI application.

<img width="400" alt="image" src="https://github.com/sujith-kamme/Project-ISQL/assets/142932988/68c9a365-5486-4687-9b59-8147d0696e03">

**Architecture design**


<img width="350" alt="image" src="https://github.com/sujith-kamme/Project-ISQL/assets/142932988/ca297286-95de-4687-81b3-3ed4cefa8060">


**Process Flow Digram**


<img width="274" alt="image" src="https://github.com/sujith-kamme/Project-ISQL/assets/142932988/7e24ff0e-146a-4277-9988-a4e704f75469">



To run the project, please go through the following instructions.
1. Install dependencies
  •	pandas
  •	OS
  •	tabulate: (pip3 install tabulate)
  •	JSON
  •	CSV
2. Caution: Make no changes in the directory structure as it might affect the code.
3. Run main.py file. After successful run, application starts and you'll be greeted with "Welcome to ISQL".
 


**Important instructions** 
1. Space is mandatory between keywords, operators, and conditions or else the code would fail.
2. ISQL tolerates not using quotes for a single word string.  For string that contains more than one word, quotes are mandatory.
3. Each time you exit and re-run the application, you would have to use “goto <database name>” or else application halts.
