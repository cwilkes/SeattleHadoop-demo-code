REGISTER target/demo-pig-udf-1.0-SNAPSHOT.jar;

purchasetimes = LOAD '$dir/purchasetimes' AS (userid: int, datein: chararray, dateout: chararray);

-- quickybuyers = FILTER purchasetimes BY DateWithinFilter(datein, dateout, 600000);
quickybuyers = FILTER purchasetimes BY DateWithinFilter(datein, dateout);

DUMP quickybuyers;
