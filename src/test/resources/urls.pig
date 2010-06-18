REGISTER target/demo-pig-udf-1.0-SNAPSHOT.jar;

A = LOAD 'src/test/resources/urls' USING QuerystringLoader('query', 'userid') AS (query: chararray, userid : int);

describe A;
dump A;
