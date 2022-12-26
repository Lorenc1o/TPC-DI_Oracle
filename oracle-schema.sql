--CREATE TYPE taxStatus AS Enumeric ('0', '1', '2');

CREATE TABLE BatchDate( BatchNumber NUMBER(3,0), 
						BatchDate DATE NOT NULL
);

CREATE TABLE DimBroker  ( SK_BrokerID  NUMBER(11,0) PRIMARY KEY,
							BrokerID  NUMBER(11,0) NOT NULL,
							ManagerID  NUMBER(11,0),
							FirstName       CHAR(50) NOT NULL,
							LastName       CHAR(50) NOT NULL,
							MiddleInitial       CHAR(1),
							Branch       CHAR(50),
							Office       CHAR(50),
							Phone       CHAR(14),
							IsCurrent CHAR(5) NOT NULL CHECK (IsCurrent = 'true' OR IsCurrent = 'false'),
							BatchID NUMBER(5,0) NOT NULL,
							EffectiveDate date NOT NULL,
							EndDate date NOT NULL							
);

CREATE TABLE DimCustomer  ( SK_CustomerID  NUMBER(11,0) PRIMARY KEY,
							CustomerID NUMBER(11,0) NOT NULL,
							TaxID CHAR(20) NOT NULL,
							Status CHAR(10) NOT NULL,
							LastName CHAR(30) NOT NULL,
							FirstName CHAR(30) NOT NULL,
							MiddleInitial CHAR(1),
							Gender CHAR(1),
							Tier NUMBER(1,0),
							DOB date NOT NULL,
							AddressLine1	varchar(80) NOT NULL,
							AddressLine2	varchar(80),
							PostalCode	char(12) NOT NULL,
							City	char(25) NOT NULL,
							StateProv	char(20) NOT NULL,
							Country	char(24),
							Phone1	char(30),
							Phone2	char(30),
							Phone3	char(30),
							Email1	char(50),
							Email2	char(50),
							NationalTaxRateDesc	varchar(50),
							NationalTaxRate	NUMBER(6,5),
							LocalTaxRateDesc	varchar(50),
							LocalTaxRate	NUMBER(6,5),
							AgencyID	char(30),
							CreditRating NUMBER(5,0),
							NetWorth	NUMBER(10),
							MarketingNameplate varchar(100),
							IsCurrent CHAR(5) NOT NULL CHECK (IsCurrent = 'true' OR IsCurrent = 'false'),
							BatchID NUMBER(5,0) NOT NULL,
							EffectiveDate date NOT NULL,
							EndDate date NOT NULL
);

CREATE TABLE DimAccount  ( SK_AccountID  NUMBER(11,0) PRIMARY KEY,
                            AccountID  NUMBER(11,0) NOT NULL,
                            SK_BrokerID  NUMBER(11,0) NOT NULL REFERENCES DimBroker (SK_BrokerID),
                            SK_CustomerID  NUMBER(11,0) NOT NULL REFERENCES DimCustomer (SK_CustomerID),
                            Status       CHAR(10) NOT NULL,
                            AccountDesc       varchar(50),
                            TaxStatus NUMBER(1,0) NOT NULL CHECK (TaxStatus = 0 OR TaxStatus = 1 OR TaxStatus = 2),
                            IsCurrent CHAR(5) NOT NULL CHECK (IsCurrent = 'true' OR IsCurrent = 'false'),
                            BatchID NUMBER(5,0) NOT NULL,
                            EffectiveDate date NOT NULL,
                            EndDate date NOT NULL
);


CREATE TABLE DimCompany (   SK_CompanyID NUMBER(11,0) PRIMARY KEY, 
							CompanyID NUMBER(11,0) NOT NULL,
							Status CHAR(10) Not NULL, 
							Name CHAR(60) Not NULL,
							Industry CHAR(50) Not NULL,
							SPrating CHAR(4),
							isLowGrade CHAR(5) NOT NULL CHECK (isLowGrade = 'true' OR isLowGrade = 'false'),
							CEO CHAR(100) Not NULL,
							AddressLine1 CHAR(80),
							AddressLine2 CHAR(80),
							PostalCode CHAR(12) Not NULL,
							City CHAR(25) Not NULL,
							StateProv CHAR(20) Not NULL,
							Country CHAR(24),
							Description CHAR(150) Not NULL,
							FoundingDate DATE,
							IsCurrent CHAR(5) NOT NULL CHECK (IsCurrent = 'true' OR IsCurrent = 'false'),
							BatchID NUMBER(5,0) Not NULL,
							EffectiveDate DATE Not NULL,
							EndDate DATE Not NULL
);

CREATE TABLE DimDate (  SK_DateID NUMBER(11,0) PRIMARY KEY,
						DateValue DATE Not NULL,
						DateDesc CHAR(20) Not NULL,
						CalendarYearID NUMBER(4) Not NULL,
						CalendarYearDesc CHAR(20) Not NULL,
						CalendarQtrID NUMBER(5) Not NULL,
						CalendarQtrDesc CHAR(20) Not NULL,
						CalendarMonthID NUMBER(6) Not NULL,
						CalendarMonthDesc CHAR(20) Not NULL,
						CalendarWeekID NUMBER(6) Not NULL,
						CalendarWeekDesc CHAR(20) Not NULL,
						DayOfWeeknumeric NUMBER(1) Not NULL,
						DayOfWeekDesc CHAR(10) Not NULL,
						FiscalYearID NUMBER(4) Not NULL,
						FiscalYearDesc CHAR(20) Not NULL,
						FiscalQtrID NUMBER(5) Not NULL,
						FiscalQtrDesc CHAR(20) Not NULL,
						HolidayFlag CHAR(5) NOT NULL CHECK (HolidayFlag = 'true' OR HolidayFlag = 'false')
);

CREATE TABLE DimSecurity( SK_SecurityID NUMBER(11,0) PRIMARY KEY,
							Symbol CHAR(15) Not NULL,
							Issue CHAR(6) Not NULL,
							Status CHAR(10) Not NULL,
							Name CHAR(70) Not NULL,
							ExchangeID CHAR(6) Not NULL,
							SK_CompanyID NUMBER(11,0) Not NULL REFERENCES DimCompany (SK_CompanyID),
							SharesOutstanding NUMBER(12,0) Not NULL,
							FirstTrade DATE Not NULL,
							FirstTradeOnExchange DATE Not NULL,
							Dividend NUMBER(10,2) Not NULL,
							IsCurrent CHAR(5) NOT NULL check (IsCurrent = 'false' or IsCurrent = 'true'),
							BatchID NUMBER(5) Not NULL,
							EffectiveDate DATE Not NULL,
							EndDate DATE Not NULL
);

CREATE TABLE DimTime ( SK_TimeID NUMBER(11,0) PRIMARY KEY,
						TimeValue DATE Not NULL,
						HourID NUMBER(2) Not NULL,
						HourDesc CHAR(20) Not NULL,
						MinuteID NUMBER(2) Not NULL,
						MinuteDesc CHAR(20) Not NULL,
						SecondID NUMBER(2) Not NULL,
						SecondDesc CHAR(20) Not NULL,
						MarketHoursFlag CHAR(5) NOT NULL check (MarketHoursFlag = 'false' or MarketHoursFlag = 'true'),
						OfficeHoursFlag CHAR(5) NOT NULL check (OfficeHoursFlag = 'false' or OfficeHoursFlag = 'true')
);

CREATE TABLE DimTrade (	TradeID NUMBER(11,0) Not NULL,
						SK_BrokerID NUMBER(11,0) REFERENCES DimBroker (SK_BrokerID),
						SK_CreateDateID NUMBER(11,0) Not NULL REFERENCES DimDate (SK_DateID),
						SK_CreateTimeID NUMBER(11,0) Not NULL REFERENCES DimTime (SK_TimeID),
						SK_CloseDateID NUMBER(11,0) REFERENCES DimDate (SK_DateID),
						SK_CloseTimeID NUMBER(11,0) REFERENCES DimTime (SK_TimeID),
						Status CHAR(10) Not NULL,
						DT_Type CHAR(12) Not NULL,
						CashFlag CHAR(5) NOT NULL check (CashFlag = 'false' or CashFlag = 'true'),
						SK_SecurityID NUMBER(11,0) Not NULL REFERENCES DimSecurity (SK_SecurityID),
						SK_CompanyID NUMBER(11,0) Not NULL REFERENCES DimCompany (SK_CompanyID),
						Quantity NUMBER(6,0) Not NULL,
						BidPrice NUMBER(8,2) Not NULL,
						SK_CustomerID NUMBER(11,0) Not NULL REFERENCES DimCustomer (SK_CustomerID),
						SK_AccountID NUMBER(11,0) Not NULL REFERENCES DimAccount (SK_AccountID),
						ExecutedBy CHAR(64) Not NULL,
						TradePrice NUMBER(8,2),
						Fee NUMBER(10,2),
						Commission NUMBER(10,2),
						Tax NUMBER(10,2),
						BatchID NUMBER(5) Not Null
);

CREATE TABLE DImessages ( MessageDateAndTime TIMESTAMP Not NULL,
							BatchID NUMBER(5) Not NULL,
							MessageSource CHAR(30),
							MessageText CHAR(50) Not NULL,
							MessageType CHAR(12) Not NULL,
							MessageData CHAR(100)
);

CREATE TABLE FactCashBalances ( SK_CustomerID NUMBER(11,0) Not Null REFERENCES DimCustomer (SK_CustomerID),
								SK_AccountID NUMBER(11,0) Not Null REFERENCES DimAccount (SK_AccountID),
								SK_DateID NUMBER(11,0) Not Null REFERENCES DimDate (SK_DateID),
								SK_TimeID NUMBER(11,0) Not Null REFERENCES DimTime (SK_TimeID),
								Cash NUMBER(15,2) Not Null,
								BatchID NUMBER(5)
);

CREATE TABLE FactHoldings (	TradeID NUMBER(11,0) Not Null,
							CurrentTradeID NUMBER(11,0) Not Null,
							SK_CustomerID NUMBER(11,0) Not NULL REFERENCES DimCustomer (SK_CustomerID),
							SK_AccountID NUMBER(11,0) Not NULL REFERENCES DimAccount (SK_AccountID),
							SK_SecurityID NUMBER(11,0) Not NULL REFERENCES DimSecurity (SK_SecurityID),
							SK_CompanyID NUMBER(11,0) Not NULL REFERENCES DimCompany (SK_CompanyID),
							SK_DateID NUMBER(11,0) Not NULL REFERENCES DimDate (SK_DateID),
							SK_TimeID NUMBER(11,0) Not NULL REFERENCES DimTime (SK_TimeID),
							CurrentPrice NUMBER(8,2) CHECK (CurrentPrice > 0) ,
							CurrentHolding NUMBER(6) Not NULL,
							BatchID NUMBER(5)
);

CREATE TABLE FactMarketHistory (    SK_SecurityID NUMBER(11,0) Not Null REFERENCES DimSecurity (SK_SecurityID),
									SK_CompanyID NUMBER(11,0) Not Null REFERENCES DimCompany (SK_CompanyID),
									SK_DateID NUMBER(11,0) Not Null REFERENCES DimDate (SK_DateID),
									PERatio NUMBER(10,2),
									Yield NUMBER(5,2) Not Null,
									FiftyTwoWeekHigh NUMBER(8,2) Not Null,
									SK_FiftyTwoWeek NUMBER(11,0) Not Null,
									FiftyTwoWeekLow NUMBER(8,2) Not Null,
									SK_FiftyTwoWeekL NUMBER(11,0) Not Null,
									ClosePrice NUMBER(8,2) Not Null,
									DayHigh NUMBER(8,2) Not Null,
									DayLow NUMBER(8,2) Not Null,
									Volume NUMBER(12) Not Null,
									BatchID NUMBER(5)
);

CREATE TABLE FactWatches ( SK_CustomerID NUMBER(11,0) Not NULL REFERENCES DimCustomer (SK_CustomerID),
							SK_SecurityID NUMBER(11,0) Not NULL REFERENCES DimSecurity (SK_SecurityID),
							SK_DateID_DatePlaced NUMBER(11,0) Not NULL REFERENCES DimDate (SK_DateID),
							SK_DateID_DateRemoved NUMBER(11,0) REFERENCES DimDate (SK_DateID),
							BatchID NUMBER(5) Not Null 
);

CREATE TABLE Industry ( IN_ID CHAR(2) Not NULL,
						IN_NAME CHAR(50) Not NULL,
						IN_SC_ID CHAR(4) Not NULL
);

CREATE TABLE Financial ( SK_CompanyID NUMBER(11,0) Not NULL REFERENCES DimCompany (SK_CompanyID),
						FI_YEAR NUMBER(4) Not NULL,
						FI_QTR NUMBER(1) Not NULL,
						FI_QTR_START_DATE DATE Not NULL,
						FI_REVENUE NUMBER(15,2) Not NULL,
						FI_NET_EARN NUMBER(15,2) Not NULL,
						FI_BASIC_EPS NUMBER(10,2) Not NULL,
						FI_DILUT_EPS NUMBER(10,2) Not NULL,
						FI_MARGIN NUMBER(10,2) Not NULL,
						FI_INVENTORY NUMBER(15,2) Not NULL,
						FI_ASSETS NUMBER(15,2) Not NULL,
						FI_LIABILITY NUMBER(15,2) Not NULL,
						FI_OUT_BASIC NUMBER(12) Not NULL,
						FI_OUT_DILUT NUMBER(12) Not NULL
);

CREATE TABLE Prospect ( AgencyID CHAR(30) NOT NULL UNIQUE,  
						SK_RecordDateID NUMBER(11,0) NOT NULL, 
						SK_UpdateDateID NUMBER(11,0) NOT NULL REFERENCES DimDate (SK_DateID),
						BatchID NUMBER(5) NOT NULL,
						IsCustomer CHAR(5) NOT NULL check (IsCustomer = 'false' or IsCustomer = 'true'),
						LastName CHAR(30) NOT NULL,
						FirstName CHAR(30) NOT NULL,
						MiddleInitial CHAR(1),
						Gender CHAR(1),
						AddressLine1 CHAR(80),
						AddressLine2 CHAR(80),
						PostalCode CHAR(12),
						City CHAR(25) NOT NULL,
						State CHAR(20) NOT NULL,
						Country CHAR(24),
						Phone CHAR(30), 
						Income NUMBER(9),
						numericberCars NUMBER(2), 
						numericberChildren NUMBER(2), 
						MaritalStatus CHAR(1), 
						Age NUMBER(3),
						CreditRating NUMBER(4),
						OwnOrRentFlag CHAR(1), 
						Employer CHAR(30),
						numericberCreditCards NUMBER(2), 
						NetWorth NUMBER(12),
						MarketingNameplate CHAR(100)
);

CREATE TABLE StatusType ( ST_ID CHAR(4) Not NULL,
							ST_NAME CHAR(10) Not NULL
);

CREATE TABLE TaxRate ( TX_ID CHAR(4) Not NULL,
						TX_NAME CHAR(50) Not NULL,
						TX_RATE NUMBER(6,5) Not NULL
);

CREATE TABLE TradeType ( TT_ID CHAR(3) Not NULL,
							TT_NAME CHAR(12) Not NULL,
							TT_IS_SELL NUMBER(1) Not NULL,
							TT_IS_MRKT NUMBER(1) Not NULL
);


CREATE TABLE Audit_ ( DataSet CHAR(20) Not Null,
							BatchID NUMBER(5),
							Date_ DATE,
							Attribute CHAR(50) not null,
							Value NUMBER(15),
							DValue NUMBER(15,5)
-- constraint UNIQUE(DataSet, BatchID, Attribute)							
);

-- staging tables

CREATE TABLE S_Company (
	PTS CHAR(15) NOT NULL,
	REC_TYPE CHAR(3) NOT NULL,
	COMPANY_NAME CHAR(60) NOT NULL,
	CIK CHAR(10) NOT NULL,
	STATUS CHAR(4) NOT NULL,
	INDUSTRY_ID CHAR(2) NOT NULL,
	SP_RATING CHAR(4) NOT NULL,
	FOUNDING_DATE CHAR(8) NOT NULL,
	ADDR_LINE_1 CHAR(80) NOT NULL,
	ADDR_LINE_2 CHAR(80) NOT NULL,
	POSTAL_CODE CHAR(12) NOT NULL,
	CITY CHAR(25) NOT NULL,
	STATE_PROVINCE CHAR(20) NOT NULL,
	COUNTRY CHAR(24) NOT NULL,
	CEO_NAME CHAR(46) NOT NULL,
	DESCRIPTION CHAR(150) NOT NULL
);

CREATE TABLE S_Security (
	PTS CHAR(15) NOT NULL,
	REC_TYPE CHAR(3) NOT NULL,
	SYMBOL CHAR(15) NOT NULL,
	ISSUE_TYPE CHAR(6) NOT NULL,
	STATUS CHAR(4) NOT NULL,
	NAME CHAR(70) NOT NULL,
	EX_ID CHAR(6) NOT NULL,
	SH_OUT CHAR(13) NOT NULL,
	FIRST_TRADE_DATE CHAR(8) NOT NULL,
	FIRST_TRADE_EXCHANGE CHAR(8) NOT NULL,
	DIVIDEN CHAR(12) NOT NULL,
	COMPANY_NAME_OR_CIK CHAR(60) NOT NULL
);

CREATE TABLE S_Financial(
	PTS CHAR(15),
	REC_TYPE CHAR(3),
	YEAR CHAR(4),
	QUARTER CHAR(1),
	QTR_START_DATE CHAR(8),
	POSTING_DATE CHAR(8),
	REVENUE CHAR(17),
	EARNINGS CHAR(17),
	EPS CHAR(12),
	DILUTED_EPS CHAR(12),
	MARGIN CHAR(12),
	INVENTORY CHAR(17),
	ASSETS CHAR(17),
	LIABILITIES CHAR(17),
	SH_OUT CHAR(13),
	DILUTED_SH_OUT CHAR(13),
	CO_NAME_OR_CIK CHAR(60)
);

CREATE TABLE S_Prospect(
    AGENCY_ID CHAR(30) NOT NULL,
    LAST_NAME CHAR(30) NOT NULL,
    FIRST_NAME CHAR(30) NOT NULL,
    MIDDLE_INITIAL CHAR(1),
    GENDER CHAR(1),
    ADDRESS_LINE_1 CHAR(80),
    ADDRESS_LINE_2 CHAR(80),
    POSTAL_CODE CHAR(12),
    CITY CHAR(25) NOT NULL,
    STATE CHAR(20) NOT NULL,
    COUNTRY CHAR(24),
    PHONE CHAR(30),
    INCOME NUMBER(9),
    NUMBER_CARS NUMBER(2),
    NUMBER_CHILDREM NUMBER(2),
    MARITAL_STATUS CHAR(1),
    AGE NUMBER(3),
    CREDIT_RATING NUMBER(4),
    OWN_OR_RENT_FLAG CHAR(1),
    EMPLOYER CHAR(30),
    NUMBER_CREDIT_CARDS NUMBER(2),
    NET_WORTH NUMBER(12)
);

CREATE TABLE S_Watches(
    W_C_ID INTEGER NOT NULL,
    W_S_SYMB CHAR(15) NOT NULL,
    W_DTS DATE NOT NULL,
    W_ACTION CHAR(4) NOT NULL
);

CREATE TABLE S_Cash_Balances(
    CT_CA_ID INTEGER NOT NULL,
    CT_DTS DATE NOT NULL,
    CT_AMT CHAR(20) NOT NULL,
    CT_NAME CHAR(100) NOT NULL
);
-- CREATE INDEX PIndex ON dimtrade (tradeid);
-- CREATE TABLE dimtradeforexperiment
-- (
--   tradeid NUMBER(,0) NOT NULL,
--   sk_brokerid NUMBER(,0),
--   date_int NUMBER(,0),
--   time_int NUMBER(,0),
--   status character(10) NOT NULL,
--   dt_type character(12) NOT NULL,
--   cashflag NUMBER(1,0) NOT NULL CHECK ( = 0 OR = 1),
--   sk_securityid NUMBER(,0) NOT NULL,
--   sk_companyid NUMBER(,0) NOT NULL,
--   quantity NUMBER(6,0) NOT NULL,
--   bidprice NUMBER(8,2) NOT NULL,
--   sk_customerid NUMBER(,0) NOT NULL,
--   sk_accountid NUMBER(,0) NOT NULL,
--   executedby character(64) NOT NULL,
--   tradeprice NUMBER(8,2),
--   fee NUMBER(10,2),
--   commission NUMBER(10,2),
--   tax NUMBER(10,2),
--   batchid NUMBER(5,0) NOT NULL,
--   th_st_id character(4)
-- );

exit