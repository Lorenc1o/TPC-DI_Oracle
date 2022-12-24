LOAD DATA 
INTO TABLE DimTime
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' TRAILING NULLCOLS
(
    SK_TimeID,
    TimeValue DATE "HH24:MI:SS",
    HourID,
    HourDesc,
    MinuteID,
    MinuteDesc,
    SecondID,
    SecondDesc,
    MarketHoursFlag,
    OfficeHoursFlag
)