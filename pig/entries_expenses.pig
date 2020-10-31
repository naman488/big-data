Employee = LOAD 'gs://project-18-pig/employee.txt' USING PigStorage(',') AS (id:int, name:chararray, salary:int, ratings:int);
Expenses = LOAD 'gs://project-18-pig/expenses.txt' USING PigStorage('\t') AS (id:int, expenses:int);
all_employee_rows = JOIN Employee BY id LEFT OUTER, Expenses BY id;
expense_with_id_present = FILTER all_employee_rows BY expenses is not null;
remove_duplicate_rows = DISTINCT expense_with_id_present;
DUMP remove_duplicate_rows
