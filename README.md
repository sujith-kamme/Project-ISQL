# Project-ISQL

**Custom Query Language for local databases**

Project ISQL is an innovative approach to developing a custom query language for efficiently querying local file-based databases. Drawing inspiration from SQL, ISQL incorporates a modern syntax and introduces enhanced querying capabilities tailored for file-based databases stored locally. The system utilizes a relational data model where data is organized in tables, supporting various operations such as projection, filtering, join, grouping, aggregation, and ordering. Additionally, ISQL allows users to create databases and tables, and it supports CRUD operations on the data. One of the notable features of Project ISQL is its efficient handling of large datasets, employing a chunk-based data processing approach to ensure scalability.

![Project-ISQL](https://github.com/sujith-kamme/Project-ISQL/assets/142932988/68c9a365-5486-4687-9b59-8147d0696e03)

## Architecture Design

![Architecture Design](https://github.com/sujith-kamme/Project-ISQL/assets/142932988/ca297286-95de-4687-81b3-3ed4cefa8060)

## Process Flow Diagram

![Process Flow Diagram](https://github.com/sujith-kamme/Project-ISQL/assets/142932988/7e24ff0e-146a-4277-9988-a4e704f75469)

## Getting Started

To run the project, follow these instructions:

1. **Install Dependencies:**
   - pandas
   - OS
   - tabulate (install with `pip3 install tabulate`)
   - JSON
   - CSV

2. **Caution:**
   Make no changes in the directory structure, as it might affect the code.

3. **Run:**
   Execute the `main.py` file. After a successful run, the application will start, and you'll be greeted with "Welcome to ISQL."

## Important Instructions

1. **Spacing:**
   Space is mandatory between keywords, operators, and conditions, or else the code would fail.

2. **String Handling:**
   ISQL tolerates not using quotes for a single-word string. For strings that contain more than one word, quotes are mandatory.

3. **Restarting Application:**
   Each time you exit and re-run the application, use “goto <database name>” to avoid halting the application.

Feel free to explore the power of Project ISQL, and if you encounter any issues, please check the documentation or reach out to me for assistance.

Happy querying! 🚀
