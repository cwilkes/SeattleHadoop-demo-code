REGISTER target/pig-demo-1.0-SNAPSHOT.jar;

A = LOAD 'src/test/resources/enterandexit' AS  (datetime: chararray, userid : int);

C = GROUP A by org.seattlehadoop.pig.demo.storevisits.DateBin(datetime, 3600000);

SPLIT C into D IF 1==1, E IF 1==1;

F = JOIN D by group, E by org.seattlehadoop.pig.demo.storevisits.AdderFunc(group,1);

describe F; 
dump F;