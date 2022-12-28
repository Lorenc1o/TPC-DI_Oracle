INSERT INTO DimCompany (SK_CompanyID,CompanyID, Status,Name,Industry,SPrating,isLowGrade,CEO,AddressLine1,AddressLine2,PostalCode,City,StateProv,Country,Description,FoundingDate,IsCurrent,BatchID,EffectiveDate,EndDate)
SELECT ORA_HASH(ROWNUM), C.CIK, S.ST_NAME, C.COMPANY_NAME, I.IN_NAME,C.SP_RATING, 
    CASE 
        WHEN LPAD(C.SP_RATING,1)='A' OR LPAD(C.SP_RATING,3)='BBB' THEN
            'false'
        ELSE
            'true'
        END,
    C.CEO_NAME, C.ADDR_LINE_1,C.ADDR_LINE_2, C.POSTAL_CODE, C.CITY, C.STATE_PROVINCE, C.COUNTRY, C.DESCRIPTION,
    TO_DATE(FOUNDING_DATE,'YYYYMMDD'),'true', 1, TO_DATE(LPAD(C.PTS,8),'YYYYMMDD'), TO_DATE('99991231','YYYYMMDD')
FROM S_Company C
JOIN Industry I ON C.INDUSTRY_ID = I.IN_ID
JOIN StatusType S ON C.STATUS = S.ST_ID
WHERE FOUNDING_DATE <> '        ' and LPAD(C.PTS,8) <> '        ';

CREATE TABLE sdc_dimcompany 
   (	SK_COMPANYID NUMBER(11,0), 
	COMPANYID NUMBER(11,0) NOT NULL, 
	STATUS CHAR(10) NOT NULL, 
	NAME CHAR(60) NOT NULL, 
	INDUSTRY CHAR(50) NOT NULL, 
	SPRATING CHAR(4), 
	ISLOWGRADE CHAR(5) NOT NULL, 
	CEO CHAR(100) NOT NULL, 
	ADDRESSLINE1 CHAR(80), 
	ADDRESSLINE2 CHAR(80), 
	POSTALCODE CHAR(12) NOT NULL, 
	CITY CHAR(25) NOT NULL, 
	STATEPROV CHAR(20) NOT NULL, 
	COUNTRY CHAR(24), 
	DESCRIPTION CHAR(150) NOT NULL, 
	FOUNDINGDATE DATE, 
	ISCURRENT CHAR(5) NOT NULL, 
	BATCHID NUMBER(5,0) NOT NULL, 
	EFFECTIVEDATE DATE NOT NULL, 
	ENDDATE DATE NOT NULL, 
	 CHECK (isLowGrade = 'true' OR isLowGrade = 'false') ENABLE, 
	 CHECK (IsCurrent = 'true' OR IsCurrent = 'false') ENABLE, 
	 PRIMARY KEY ("SK_COMPANYID"));
ALTER TABLE sdc_dimcompany
    ADD RN DECIMAL;
INSERT INTO sdc_dimcompany
SELECT DC.*, ROW_NUMBER() OVER(ORDER BY CompanyID, EffectiveDate) RN
FROM DimCompany DC;

UPDATE DimCompany 
SET DimCompany.EndDate = 
    (SELECT EndDate FROM ( 
        SELECT s1.SK_CompanyID,
                s2.EffectiveDate EndDate
        FROM sdc_dimcompany s1
        JOIN sdc_dimcompany s2 ON (s1.RN = (s2.RN - 1) AND s1.CompanyID = s2.CompanyID)
        WHERE s1.SK_CompanyID = DimCompany.SK_CompanyID)),
    DimCompany.IsCurrent = 
    (SELECT 'false' FROM ( 
        SELECT *
        FROM sdc_dimcompany s1
        JOIN sdc_dimcompany s2 ON (s1.RN = (s2.RN - 1) AND s1.CompanyID = s2.CompanyID)
        WHERE s1.SK_CompanyID = DimCompany.SK_CompanyID))
WHERE EXISTS ( 
        SELECT *
        FROM sdc_dimcompany s1
        JOIN sdc_dimcompany s2 ON (s1.RN = (s2.RN - 1) AND s1.CompanyID = s2.CompanyID)
        WHERE s1.SK_CompanyID = DimCompany.SK_CompanyID);

DROP TABLE sdc_dimcompany;
exit;
