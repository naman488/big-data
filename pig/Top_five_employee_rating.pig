Employee = LOAD 'gs://project-18-pig/employee.txt' USING PigStorage(',') AS (id:int, name:chararray, salary:int, ratings:int);
sort_by_rating_name = ORDER Employee by ratings DESC, name ASC;
result = LIMIT sort_by_rating_name 5;
DUMP result;