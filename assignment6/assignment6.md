'''
A = LOAD '/user/shared/tweets2011/tweets2011.txt' AS (id:chararray, date:chararray, name:chararray, comment:chararray);
B = FOREACH A GENERATE SUBSTRING(date,4,13) AS time0, comment AS comment;
C = FOREACH B GENERATE REPLACE(time0,'Jan ','1/') AS time1;
D = FOREACH C GENERATE REPLACE(time1,'Feb ','2/') AS time2;
E = group D by time2;
F = FOREACH E GENERATE group as term, COUNT(D) as count;
T = FILTER F BY ((SUBSTRING(term,0,2) == '1/') AND (SUBSTRING(term,2,4) >= '23')) OR ((SUBSTRING(term,0,2) == '2/') AND (SUBSTRING(term,2,4) <= '08'));
dump T;



A = LOAD '/user/shared/tweets2011/tweets2011.txt' AS (id:chararray, date:chararray, name:chararray, comment:chararray);
X = FOREACH A GENERATE SUBSTRING(date,4,13) AS time0, REGEX_EXTRACT_ALL(comment,'.*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*') as Ecomment;
Y = FILTER X by Ecomment is not null; 
C = FOREACH Y GENERATE REPLACE(time0,'Jan ','1/') AS time1;
D = FOREACH C GENERATE REPLACE(time1,'Feb ','2/') AS time2;
E = group D by time2;
F = FOREACH E GENERATE group as term, COUNT(D) as count;
T = FILTER F BY ((SUBSTRING(term,0,2) == '1/') AND (SUBSTRING(term,2,4) >= '23')) OR ((SUBSTRING(term,0,2) == '2/') AND (SUBSTRING(term,2,4) <= '08'));
dump T;

'''


