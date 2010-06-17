REGISTER target/pig-demo-1.0-SNAPSHOT.jar;

planets = load '$dir/planets' as (name : chararray, l:tuple(x : int, y : int, z : int));
cosmo = load '$dir/cosmo' as (planet1 : chararray, planet2 : chararray);

A = JOIN cosmo BY planet1, planets BY name;
B = JOIN A by planet2, planets BY name;

locations =  FOREACH B GENERATE $1 AS p1name:chararray, $2 AS p2name : chararray, AstroDist($3,$5) as distance;

describe B;
dump B;

dump locations;
