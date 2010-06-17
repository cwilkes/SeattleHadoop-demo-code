REGISTER target/pig-demo-1.0-SNAPSHOT.jar;

A = LOAD 'src/test/resources/urls' USING org.seattlehadoop.pig.demo.storevisits.QuerystringLoader('query', 'userid') AS (query: chararray, userid : int);

dump A;