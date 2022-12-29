INSERT INTO DImessages 
    (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
    SELECT CURRENT_TIMESTAMP, C.BatchID, 'DimCompany', 'Invalid SPRating', 'Alert', 'CO_ID = ' || C.SK_CompanyID || ', CO_SP_RATE = ' || C.SPRating
    select SPRating
    FROM DimCompany C
    WHERE SPRating not in (
    'AAA', 'AA+', 'AA-', 'AA', 'A+', 'A-', 'A', 
    'BBB+', 'BBB-', 'BBB', 'BB+', 'BB-', 'BB', 
    'B+', 'B-', 'B', 'CCC+', 'CCC-', 'CCC', 
    'CC', 'C', 'D');

UPDATE DimCompany
SET
    SPRating = 'NULL',
    isLowGrade = 'NULL'
where SPRating not in (
    'AAA', 'AA+', 'AA-', 'AA', 'A+', 'A-', 'A', 
    'BBB+', 'BBB-', 'BBB', 'BB+', 'BB-', 'BB', 
    'B+', 'B-', 'B', 'CCC+', 'CCC-', 'CCC', 
    'CC', 'C', 'D');

exit