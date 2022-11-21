REGISTER /usr/lib/pig/piggybank.jar;

movies = LOAD '$inputMovies' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','NOCHANGE') as (actor_id:chararray, played_in:int, director_in:int);

actor = load'$inputActors' USING PigStorage('\t') as (nconst:chararray, primaryName:chararray,birthYear:int,deathYear:int, primaryProfession:chararray, knownForTitles:chararray);



actor2 = FOREACH actor generate nconst,primaryName,primaryProfession;
together = JOIN movies BY actor_id, actor2 BY nconst;
together_simple = foreach together generate actor_id as actor_id, primaryName as primaryName ,played_in as played_in,director_in as director_in,primaryProfession as primaryProfession;

D2 = filter together_simple by (primaryProfession matches '.*(actor)|(actress).*');
D3 = order D2 by played_in desc;
D4 = limit D3 3;
D5 = foreach D4 generate actor_id as id, primaryName as name, played_in as played, director_in as directed;

A2 = filter together_simple by (primaryProfession matches '.*director.*');
A3 = order A2 by director_in desc;
A4 = limit A3 3;
A5 = foreach A4 generate actor_id as id, primaryName as name, played_in as played, director_in as directed;



result= union D5 , A5;




STORE result INTO '$out' USING JsonStorage();

