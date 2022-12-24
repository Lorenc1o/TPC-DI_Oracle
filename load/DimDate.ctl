LOAD DATA 
INTO TABLE DimDate 
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' TRAILING NULLCOLS
(
    SK_DateID,
    DateValue DATE "YYYY-MM-DD",
    DateDesc,
    CalendarYearID,
    CalendarYearDesc,
    CalendarQtrID,
    CalendarQtrDesc,
    CalendarMonthID,
    CalendarMonthDesc,
    CalendarWeekID,
    CalendarWeekDesc,
    DayOfWeeknumeric,
    DayOfWeekDesc,
    FiscalYearID,
    FiscalYearDesc,
    FiscalQtrID,
    FiscalQtrDesc,
    HolidayFlag
)