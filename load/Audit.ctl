OPTIONS (SKIP=1)
LOAD DATA 
APPEND
INTO TABLE Audit_ 
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' TRAILING NULLCOLS
(
    DataSet,
    BatchID,
    Date_ DATE "YYYY-MM-DD",
    Attribute,
    Value,
    DValue
)