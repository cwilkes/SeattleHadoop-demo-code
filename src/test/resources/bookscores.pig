REGISTER target/demo-pig-udf-1.0-SNAPSHOT.jar;

A = LOAD '$dir/bookscores' as (name : chararray, reviewer : chararray, score : int);

dump A;
describe A;

B = group A by name;
describe B;
-- B: {group: chararray,A: {name: chararray,reviewer: chararray,score: int}}
C = FOREACH B GENERATE group, BestBook(A.reviewer, A.score) as reviewandscore;

dump B;
dump C;
describe C;
