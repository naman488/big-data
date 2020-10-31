Employee = LOAD 'gs://project-18-pig/employee.txt' USING PigStorage(',') AS (id:int, name:chararray, salary:int, ratings:int);
filter_by_odd_id = FILTER Employee BY id%2 == 1;
sort_by_salary_name = ORDER filter_by_odd_id by salary DESC, name ASC;
result = LIMIT sort_by_salary_name 3;
DUMP result;