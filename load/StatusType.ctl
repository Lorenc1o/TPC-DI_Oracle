LOAD DATA 
INFILE
TRUNCATE
INTO TABLE StatusType
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' TRAILING NULLCOLS
(
    ST_ID,
	ST_NAME
)