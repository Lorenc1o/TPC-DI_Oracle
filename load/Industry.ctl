LOAD DATA 
INFILE
TRUNCATE
INTO TABLE Industry
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' TRAILING NULLCOLS
(
    IN_ID,
    IN_NAME,
    IN_SC_ID
)