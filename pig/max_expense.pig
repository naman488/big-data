Employee = LOAD 'gs://project-18-pig/employee.txt' USING PigStorage(',') AS (id:int, name:chararray, salary:int, ratings:int);
Expenses = LOAD 'gs://project-18-pig/expenses.txt' USING PigStorage('\t') AS (id:int, expenses:int);
sort_by_expense_name = ORDER both_files by expenses DESC, name ASC;
result = LIMIT sort_by_expense_name 1;
DUMP result;
