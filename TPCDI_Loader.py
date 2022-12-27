import os
import glob
import xmltodict
import oracledb

from utils import prepare_char_insertion, prepare_numeric_insertion

class TPCDI_Loader():
  BASE_SQL_CMD = ""

  def __init__(self, sf, config, batch_number, drop_sql, create_sql, load_path, overwrite=False):
    """
    Initialize staging database.

    Attributes:
        sf (str): Scale factor to be used in benchmark.
        db_name (str): Name of database schema to which the data will be loaded.    
        config (config list): Config object retrieved from calling ConfigParser().read().
        batch_number (int): Batch number that going to be processed
        drop_sql (str): Path to the sql file for dropping all the tables
        create_sql (str): Path to the sql file for creating all the tables
        load_path (str): Path to the directory load, where all sql files are located
    """

    self.sf = sf
    self.batch_number = batch_number
    self.batch_dir = "../staging/"+self.sf+"/Batch"+str(self.batch_number)+"/"
    self.load_path = load_path
    self.oracle_user = config['ORACLESQL_SERVER']['oracle_user']
    self.oracle_pwd = config['ORACLESQL_SERVER']['oracle_pwd']
    self.oracle_host = config['ORACLESQL_SERVER']['oracle_host']
    self.oracle_db = config['ORACLESQL_SERVER']['oracle_db']

    # Construct base oraclesql command (set host, port, and user)
    TPCDI_Loader.BASE_SQL_CMD = 'sqlplus %s/%s@%s/%s' % (
        self.oracle_user, self.oracle_pwd, self.oracle_host, self.oracle_db)
    TPCDI_Loader.BASE_SQLLDR_CMD = 'sqlldr userid=%s/%s@%s/%s' % (
      self.oracle_user, self.oracle_pwd, self.oracle_host, self.oracle_db
    )
    print(TPCDI_Loader.BASE_SQLLDR_CMD)
    # Drop database if it is exists and overwrite param is set to True
    if overwrite:
      print('YYYYYYYYYYYYes 0')
      # dropping the tables
      cmd = TPCDI_Loader.BASE_SQL_CMD+' @%s' % (drop_sql)
      os.system(cmd)

      # Create the tables
      cmd = TPCDI_Loader.BASE_SQL_CMD+' @%s' % (create_sql)
      os.system(cmd)
    

  def load_current_batch_date(self):
    with open(self.batch_dir+"BatchDate.txt", "r") as batch_date_file:
      cmd = TPCDI_Loader.BASE_SQL_CMD+' @%s' % (self.load_path+'/BatchDate.sql')
      cmd += ' %s %s' % (self.batch_number,batch_date_file.read().strip())
      os.system(cmd)
  
  def load_dim_date(self):
    """
    Create DimDate table in the staging database and then load rows in Date.txt into it.
    """

    # Create query to load text data into dimDate table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/DimDate.ctl', self.batch_dir + 'Date.txt')
    os.system(cmd)

  def load_dim_time(self):
    """
    Create DimTime table in the staging database and then load rows in Time.txt into it.
    """
    # Create query to load text data into dimTime table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/DimTime.ctl', self.batch_dir + 'Time.txt')
    os.system(cmd)
    
  def load_industry(self):
    """
    Create Industry table in the staging database and then load rows in Industry.txt into it.
    """
    # Create query to load text data into industry table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/Industry.ctl', self.batch_dir + 'Industry.txt')
    os.system(cmd)

  def load_status_type(self):
    """
    Create StatusType table in the staging database and then load rows in StatusType.txt into it.
    """
    # Create query to load text data into statusType table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/StatusType.ctl', self.batch_dir + 'StatusType.txt')
    os.system(cmd)

  def load_tax_rate(self):
    """
    Create TaxRate table in the staging database and then load rows in TaxRate.txt into it.
    """
    # Create query to load text data into taxRate table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/TaxRate.ctl', self.batch_dir + 'TaxRate.txt')
    os.system(cmd)
    
  def load_trade_type(self):
    """
    Create TradeType table in the staging database and then load rows in TradeType.txt into it.
    """
    # Create query to load text data into tradeType table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/TradeType.ctl', self.batch_dir + 'TradeType.txt')
    os.system(cmd)

  def load_staging_customer(self):
    """
    Create S_Customer table in the staging database and then load rows in CustomerMgmt.xml into it.
    """

    # Create ddl to store customer
    customer_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE S_Customer(
      ActionType CHAR(9) NOT NULL,
      ActionTS CHAR(20) NOT NULL,
      C_ID INTEGER NOT NULL,
      C_TAX_ID CHAR(20),
      C_GNDR CHAR(1) NOT NULL,
      C_TIER NUMERIC(1),
      C_DOB DATE,
      C_L_NAME CHAR(25),
      C_F_NAME CHAR(20),
      C_M_NAME CHAR(1),
      C_ADLINE1 CHAR(80),
      C_ADLINE2 CHAR(80),
      C_ZIPCODE CHAR(12),
      C_CITY CHAR(25),
      C_STATE_PROV CHAR(20),
      C_CTRY CHAR(24),
      C_PRIM_EMAIL CHAR(50),
      C_ALT_EMAIL CHAR(50),
      C_PHONE_1_C_CTRY_CODE CHAR(30),
      C_PHONE_1_C_AREA_CODE CHAR(30),
      C_PHONE_1_C_LOCAL CHAR(30),
      C_PHONE_1_C_EXT CHAR(30),
      C_PHONE_2_C_CTRY_CODE CHAR(30),
      C_PHONE_2_C_AREA_CODE CHAR(30),
      C_PHONE_2_C_LOCAL CHAR(30),
      C_PHONE_2_C_EXT CHAR(30),
      C_PHONE_3_C_CTRY_CODE CHAR(30),
      C_PHONE_3_C_AREA_CODE CHAR(30),
      C_PHONE_3_C_LOCAL CHAR(30),
      C_PHONE_3_C_EXT CHAR(30),
      C_LCL_TX_ID CHAR(4),
      C_NAT_TX_ID CHAR(4),
      CA_ID INTEGER NOT NULL,
      CA_TAX_ST NUMERIC(1),
      CA_B_ID INTEGER,
      CA_NAME CHAR(50)
    );
    """
    
    # Construct mysql client bash command to execute ddl and data loading query
    # customer_ddl_cmd = TPCDI_Loader.BASE_SQL_CMD+" -D "+self.db_name+" -e \""+customer_ddl+"\""
    
    # Execute the command
    os.system(customer_ddl_cmd)

    s_customer_base_query = "INSERT INTO S_Customer VALUES "
    s_customer_values = []
    max_packet = 150

    with open("staging/"+self.sf+"/Batch1/CustomerMgmt.xml") as fd:
      doc = xmltodict.parse(fd.read())
      actions = doc['TPCDI:Actions']['TPCDI:Action']
      for action in actions:
        ActionType = prepare_char_insertion(action['@ActionType'])
        ActionTS = prepare_char_insertion(action['@ActionTS'])
        C_ID = prepare_numeric_insertion(action['Customer']['@C_ID'])
        C_TAX_ID = C_GNDR = C_TIER = C_DOB = C_L_NAME = C_F_NAME = C_M_NAME = C_ADLINE1 = C_ADLINE2 = C_ZIPCODE = C_CITY = C_STATE_PROV = C_CTRY = "''"
        try:
          C_TAX_ID = prepare_char_insertion(action['Customer']['@C_TAX_ID'])
        except:
          pass
        try:
          C_GNDR = prepare_char_insertion(action['Customer']['@C_GNDR'])
        except:
          pass
        try:
          C_TIER = prepare_numeric_insertion(action['Customer']['@C_TIER'])
        except:
          pass
        try:
          C_DOB = prepare_char_insertion(action['Customer']['@C_DOB'])
        except:
          pass
        try:
          C_L_NAME = prepare_char_insertion(action['Customer']['Name']['C_L_NAME'])
        except:
          pass
        try:
          C_F_NAME = prepare_char_insertion(action['Customer']['Name']['C_F_NAME'])
        except:
          pass
        try:
          C_M_NAME = prepare_char_insertion(action['Customer']['Name']['C_M_NAME'])
        except:
          pass
        try:
          C_ADLINE1 = prepare_char_insertion(action['Customer']['Address']['C_ADLINE1'])
        except:
          pass
        try:
          C_ADLINE2 = prepare_char_insertion(action['Customer']['Address']['C_ADLINE2'])
        except:
          pass
        try:
          C_ZIPCODE = prepare_char_insertion(action['Customer']['Address']['C_ADLINE2'])
        except:
          pass
        try:
          C_CITY = prepare_char_insertion(action['Customer']['Address']['C_CITY'])
        except:
          pass
        try:
          C_STATE_PROV = prepare_char_insertion(action['Customer']['Address']['C_STATE_PROV'])
        except:
          pass
        try:
          C_CTRY = prepare_char_insertion(action['Customer']['Address']['C_CTRY'])
        except:
          pass
        C_PRIM_EMAIL = C_ALT_EMAIL = C_PHONE_1_C_CTRY_CODE = C_PHONE_1_C_AREA_CODE = C_PHONE_1_C_LOCAL = C_PHONE_2_C_CTRY_CODE = C_PHONE_2_C_AREA_CODE = C_PHONE_2_C_LOCAL = C_PHONE_3_C_CTRY_CODE = C_PHONE_3_C_AREA_CODE = C_PHONE_3_C_LOCAL = C_PHONE_1_C_EXT = C_PHONE_2_C_EXT = C_PHONE_3_C_EXT = "''"
        try:
          C_PRIM_EMAIL = prepare_char_insertion(action['Customer']['ContactInfo']['C_PRIM_EMAIL'])
        except:
          pass
        try:
          C_ALT_EMAIL = prepare_char_insertion(action['Customer']['ContactInfo']['C_ALT_EMAIL'])
        except:
          pass
        try:
          C_PHONE_1_C_EXT = prepare_char_insertion(action['Customer']['ContactInfo']['C_PHONE_1']['C_EXT'])
        except:
          pass
        try:
          C_PHONE_1_C_LOCAL = prepare_char_insertion(action['Customer']['ContactInfo']['C_PHONE_1']['C_LOCAL'])
        except:
          pass
        try:
          C_PHONE_1_C_AREA_CODE = prepare_char_insertion(action['Customer']['ContactInfo']['C_PHONE_1']['C_AREA_CODE'])
        except:
          pass
        try:
          C_PHONE_1_C_CTRY_CODE = prepare_char_insertion(action['Customer']['ContactInfo']['C_PHONE_1']['C_CTRY_CODE'])
        except:
          pass
        try:
          C_PHONE_2_C_EXT = prepare_char_insertion(action['Customer']['ContactInfo']['C_PHONE_2']['C_EXT'])
        except:
          pass
        try:
          C_PHONE_2_C_LOCAL = prepare_char_insertion(action['Customer']['ContactInfo']['C_PHONE_2']['C_LOCAL'])
        except:
          pass
        try:
          C_PHONE_2_C_AREA_CODE = prepare_char_insertion(action['Customer']['ContactInfo']['C_PHONE_2']['C_AREA_CODE'])
        except:
          pass
        try:
          C_PHONE_2_C_CTRY_CODE = prepare_char_insertion(action['Customer']['ContactInfo']['C_PHONE_2']['C_CTRY_CODE'])
        except:
          pass
        try:
          C_PHONE_3_C_EXT = prepare_char_insertion(action['Customer']['ContactInfo']['C_PHONE_3']['C_EXT'])
        except:
          pass
        try:
          C_PHONE_3_C_LOCAL = prepare_char_insertion(action['Customer']['ContactInfo']['C_PHONE_3']['C_LOCAL'])
        except:
          pass
        try:
          C_PHONE_3_C_AREA_CODE = prepare_char_insertion(action['Customer']['ContactInfo']['C_PHONE_3']['C_AREA_CODE'])
        except:
          pass
        try:
          C_PHONE_3_C_CTRY_CODE = prepare_char_insertion(action['Customer']['ContactInfo']['C_PHONE_3']['C_CTRY_CODE'])
        except:
          pass
        C_LCL_TX_ID = C_NAT_TX_ID = "''"
        try:
          C_LCL_TX_ID = prepare_char_insertion(action['Customer']['TaxInfo']['C_LCL_TX_ID'])
          C_NAT_TX_ID = prepare_char_insertion(action['Customer']['TaxInfo']['C_NAT_TX_ID'])
        except:
          pass
        CA_ID = "''"
        try:
          CA_ID = prepare_numeric_insertion(action['Customer']['Account']['@CA_ID'])
        except:
          pass
        CA_TAX_ST = "''"
        try:
          CA_TAX_ST = prepare_numeric_insertion(action['Customer']['Account']['@CA_TAX_ST'])
        except:
          pass
        CA_B_ID = "''"
        try:
          CA_B_ID = prepare_numeric_insertion(action['Customer']['Account']['CA_B_ID'])
        except:
          pass
        CA_NAME = "''"
        try:
          CA_NAME = prepare_char_insertion(action['Customer']['Account']['CA_NAME'])
        except:
          pass

        s_customer_values.append(f"({ActionType}, {ActionTS}, {C_ID}, {C_TAX_ID}, {C_GNDR}, {C_TIER}, {C_DOB}, {C_L_NAME}, {C_F_NAME}, {C_M_NAME}, {C_ADLINE1}, {C_ADLINE2}, {C_ZIPCODE}, {C_CITY}, {C_STATE_PROV}, {C_CTRY}, {C_PRIM_EMAIL}, {C_ALT_EMAIL}, {C_PHONE_1_C_CTRY_CODE}, {C_PHONE_1_C_AREA_CODE}, {C_PHONE_1_C_LOCAL}, {C_PHONE_1_C_EXT}, {C_PHONE_2_C_CTRY_CODE}, {C_PHONE_2_C_AREA_CODE}, {C_PHONE_2_C_LOCAL}, {C_PHONE_2_C_EXT}, {C_PHONE_3_C_CTRY_CODE}, {C_PHONE_3_C_AREA_CODE}, {C_PHONE_3_C_LOCAL}, {C_PHONE_3_C_EXT}, {C_LCL_TX_ID}, {C_NAT_TX_ID}, {CA_ID}, {CA_TAX_ST}, {CA_B_ID}, {CA_NAME})")
        if len(s_customer_values)>=max_packet:
          # Create query to load text data into tradeType table
          s_customer_load_query=s_customer_base_query+','.join(s_customer_values)
          s_customer_values = []
          # Construct mysql client bash command to execute ddl and data loading query
          # s_customer_load_cmd = TPCDI_Loader.BASE_SQL_CMD+" -D "+self.db_name+" -e \""+s_customer_load_query+"\""
      
          # Execute the command
          os.system(s_customer_load_cmd)
        
  
  def load_staging_broker(self):
    """
    Load rows in HR.csv into S_Broker table in staging database.
    """

    # Create query to load txt data into S_Watches table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/Broker.ctl', self.batch_dir + 'HR.csv')
    os.system(cmd)

  def load_broker(self):
    """
    Create DimBroker table in the target database and then load rows in HR.csv into it.
    """
    load_dim_broker_query = """
      INSERT INTO DimBroker (BrokerID,ManagerID,FirstName,LastName,MiddleInitial,Branch,Office,Phone,IsCurrent,BatchID,EffectiveDate,EndDate)
      SELECT SB.EmployeeID, SB.ManagerID, SB.EmployeeFirstName, SB.EmployeeLastName, SB.EmployeeMI, SB.EmployeeBranch, SB.EmployeeOffice, SB.EmployeePhone, 'true', %d, (SELECT MIN(DateValue) FROM DimDate), TO_DATE('9999/12/31', 'yyyy/mm/dd')
      FROM S_Broker SB
      WHERE SB.EmployeeJobCode = 314
    """ % (self.batch_number)
    print(load_dim_broker_query)

    with oracledb.connect(
      user=self.oracle_user, password=self.oracle_pwd, 
      dsn=self.oracle_host+'/'+self.oracle_db) as connection:
      with connection.cursor() as cursor:
        cursor.execute(load_dim_broker_query)
      connection.commit()


  def load_staging_cash_balances(self):
    """
    Load rows in CashTransaction.txt into S_Cash_Balances table in staging database.
    """

    # Create query to load txt data into S_Watches table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/CashBalances.ctl', self.batch_dir + 'CashTransaction.txt')
    os.system(cmd)

  def load_staging_watches(self):
    """
    Load rows in WatchHistory.txt into S_Watches table in staging database.
    """

    # Create query to load txt data into S_Watches table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/Watches.ctl', self.batch_dir + 'WatchHistory.txt')
    os.system(cmd)

  def load_staging_prospect(self):
    """
    Load rows in Prospect.csv into S_Prospect table in staging database.
    """

    # Create query to load csv data into S_Prospect table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/Prospect.ctl', self.batch_dir + 'Prospect.csv')
    os.system(cmd)

  def load_prospect(self):
    marketing_nameplate_func = """
    CREATE OR REPLACE FUNCTION get_marketing_template(net_worth NUMBER, income NUMBER,
    number_credit_cards NUMBER, number_children NUMBER, age NUMBER,
    credit_rating NUMBER, number_cars NUMBER)
    RETURN VARCHAR AS
        marketing_template VARCHAR(100);
    BEGIN
        IF (net_worth>1000000 OR income>200000) THEN
            marketing_template :=  CONCAT(marketing_template, 'HighValue+');
        END IF;
        IF (number_credit_cards>5 OR number_children>3) THEN
          marketing_template :=  CONCAT(marketing_template, 'Expenses+');
        END IF;
        IF (age>45) THEN
          marketing_template :=  CONCAT(marketing_template, 'Boomer+');
        END IF;
        IF (credit_rating<600 or income <5000 or net_worth < 100000) THEN
          marketing_template :=  CONCAT(marketing_template, 'MoneyAlert+');
        END IF;
        IF (number_cars>3 or number_credit_cards>7) THEN
          marketing_template :=  CONCAT(marketing_template, 'Spender+');
        END IF;
        IF (age>25 or net_worth>1000000) THEN
          marketing_template :=  CONCAT(marketing_template, 'Inherited+');
        END IF;
        RETURN SUBSTR(marketing_template, 1, LENGTH(marketing_template) - 1);
    END;
    """
    #TODO: Change IsCustomer when staging customer is implemented
    load_prospect_query = """   
    INSERT INTO Prospect
    SELECT SP.AGENCY_ID, (SELECT SK_DateID FROM DimDate WHERE DateValue IN (SELECT BatchDate FROM BatchDate WHERE BatchNumber = {0})) SK_RecordDateID,
          (SELECT SK_DateID FROM DimDate WHERE DateValue IN (SELECT BatchDate FROM BatchDate WHERE BatchNumber = {0})) SK_UpdateDateID, {0} BatchID, 
          'false' IsCustomer, SP.LAST_NAME, SP.FIRST_NAME, SP.MIDDLE_INITIAL, SP.GENDER, SP.ADDRESS_LINE_1, SP.ADDRESS_LINE_2, SP.POSTAL_CODE, SP.CITY,
          SP.STATE, SP.COUNTRY, SP.PHONE, SP.INCOME, SP.NUMBER_CARS,SP.NUMBER_CHILDREM, SP.MARITAL_STATUS, SP.AGE,
          SP.CREDIT_RATING, SP.OWN_OR_RENT_FLAG, SP.EMPLOYER,SP.NUMBER_CREDIT_CARDS, SP.NET_WORTH, 
          get_marketing_template(SP.NET_WORTH, SP.INCOME, SP.NUMBER_CREDIT_CARDS, SP.NUMBER_CHILDREM, SP.AGE, SP.CREDIT_RATING, SP.NUMBER_CARS) MarketingNameplate
    FROM S_Prospect SP
    """.format(self.batch_number)    
    print(load_prospect_query)

    with oracledb.connect(
      user=self.oracle_user, password=self.oracle_pwd, 
      dsn=self.oracle_host+'/'+self.oracle_db) as connection:
      with connection.cursor() as cursor:
        cursor.execute(marketing_nameplate_func)
        cursor.execute(load_prospect_query)
      connection.commit()
  
  def load_audit(self):
    """
    Create Audit table in the staging database and then load rows in files with "_audit.csv" ending into it.
    """
    for filepath in glob.iglob(self.batch_dir+"*_audit.csv"):# Create query to load text data into tradeType table
      cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/Audit.ctl', filepath)
      os.system(cmd)

  def load_staging_finwire(self):
    """
    Create S_Company and S_Security table in the staging database and then load rows in FINWIRE files with the type of CMP
    """
    base_path = "../staging/"+self.sf+"/Batch1/"
    s_company_base_query = "INSERT INTO S_Company"
    s_security_base_query = "INSERT INTO S_Security"
    s_financial_base_query = "INSERT INTO S_Financial"
    s_company_values = []
    s_security_values = []
    s_financial_values = []
    max_packet = 150
    for fname in os.listdir(base_path):
      print('fname', fname)
      if("FINWIRE" in fname and "audit" not in fname):
        print('YYYYYYYYYYYYes 1')
        with open(base_path+fname, 'r') as finwire_file:
          for idx, line in enumerate(finwire_file):
            pts = line[:15] #0
            rec_type=line[15:18] #1

            if rec_type=="CMP":
              company_name = line[18:78] #2
              cik = line[78:88] #3
              status = line[88:92] #4
              industry_id = line[92:94] #5
              sp_rating = line[94:98] # 6
              founding_date = line[98:106] #7
              addr_line_1 = line[106:186] #8
              addr_line_2 = line[186:266] #9
              postal_code = line[266:278] #10
              city = line[278:303] #10
              state_province = line[303:323] #11
              country = line[323:347] #12
              ceo_name = line[347:393] #13
              description = line[393:][:-1] #14

              s_company_values.append(
                "%s (PTS,REC_TYPE,COMPANY_NAME,CIK,STATUS,INDUSTRY_ID,SP_RATING,FOUNDING_DATE,ADDR_LINE_1,"
                "ADDR_LINE_2,POSTAL_CODE,CITY,STATE_PROVINCE,COUNTRY,CEO_NAME,DESCRIPTION) "
                "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
                %(
                  s_company_base_query,
                  pts,rec_type,company_name,cik,status,industry_id,sp_rating,founding_date,addr_line_1,
                  addr_line_2,postal_code,city,state_province,country,ceo_name,description
                ))

              if len(s_company_values)>=max_packet:
                print("yes 1")
                # Create query to load text data into tradeType table
                with oracledb.connect(
                  user=self.oracle_user, password=self.oracle_pwd, 
                  dsn=self.oracle_host+'/'+self.oracle_db) as connection:
                  with connection.cursor() as cursor:
                    for query in s_company_values:
                      cursor.execute(query)
                  connection.commit()
                s_company_values = []
                      
            elif rec_type == "SEC":
              symbol = line[18:33]
              issue_type = line[33:39]
              status = line[39:43]
              name = line[43:113]
              ex_id = line[113:119]
              sh_out = line[119:132]
              first_trade_date = line[132:140]
              first_trade_exchange = line[140:148]
              dividen = line[148:160]
              company_name = line[160:][:-1]
              
              s_security_values.append(
                "%s (PTS,REC_TYPE,SYMBOL,ISSUE_TYPE,STATUS,NAME,EX_ID,SH_OUT,FIRST_TRADE_DATE,FIRST_TRADE_EXCHANGE,"
                "DIVIDEN,COMPANY_NAME_OR_CIK)"
                "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
                %(
                  s_security_base_query,
                  pts,rec_type,symbol,issue_type,status,name,ex_id,sh_out,first_trade_date,
                  first_trade_exchange,dividen,company_name))

              if len(s_security_values)>=max_packet:
                print('yes 2')
                # Create query to load text data into tradeType table
                with oracledb.connect(
                  user=self.oracle_user, password=self.oracle_pwd, 
                  dsn=self.oracle_host+'/'+self.oracle_db) as connection:
                  with connection.cursor() as cursor:
                    for query in s_security_values:
                      cursor.execute(query)
                  connection.commit()
                s_security_values = []

            elif rec_type == "FIN":
              year = line[18:22]
              quarter = line[22:23]
              qtr_start_date = line[23:31]
              posting_date = line[31:39]
              revenue = line[39:56]
              earnings = line[56:73]
              eps = line[73:85]
              diluted_eps = line[85:97]
              margin = line[97:109]
              inventory = line[109:126]
              assets = line[126:143]
              liabilities = line[143:160]
              sh_out = line[160:173]
              diluted_sh_out = line[173:186]
              co_name_or_cik = line[186:][:-1]

              s_financial_values.append(
                "%s (PTS, REC_TYPE, YEAR,QUARTER,QTR_START_DATE,POSTING_DATE,REVENUE,EARNINGS,EPS,DILUTED_EPS,"
                "MARGIN,INVENTORY,ASSETS,LIABILITIES,SH_OUT,DILUTED_SH_OUT,CO_NAME_OR_CIK) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
                %(s_financial_base_query,
                  pts, rec_type, year,quarter,qtr_start_date,posting_date, revenue, earnings, eps, diluted_eps, 
                  margin, inventory, assets, liabilities, sh_out,diluted_sh_out,co_name_or_cik
                  ))

              if len(s_financial_values)>=max_packet:
                print('yes 3')
                # Create query to load text data into tradeType table
                with oracledb.connect(
                  user=self.oracle_user, password=self.oracle_pwd, 
                  dsn=self.oracle_host+'/'+self.oracle_db) as connection:
                  with connection.cursor() as cursor:
                    for query in s_financial_values:
                      cursor.execute(query)
                  connection.commit()
                s_financial_values = []

  def load_target_dim_company(self):
    """
    Create Dim Company table in the staging database and then load rows by joining staging_company, staging_industry, and staging StatusType
    """
    # Create query to load text data into dim_company table
    dim_company_load_query="""
      INSERT INTO DimCompany (CompanyID,Status,Name,Industry,SPrating,isLowGrade,CEO,AddressLine1,AddressLine2,PostalCode,City,StateProv,Country,Description,FoundingDate,IsCurrent,BatchID,EffectiveDate,EndDate)
      SELECT C.CIK,S.ST_NAME, C.COMPANY_NAME, I.IN_NAME,C.SP_RATING, IF(LEFT(C.SP_RATING,1)='A' OR LEFT (C.SP_RATING,3)='BBB','FALSE','TRUE'),
            C.CEO_NAME, C.ADDR_LINE_1,C.ADDR_LINE_2, C.POSTAL_CODE, C.CITY, C.STATE_PROVINCE, C.COUNTRY, C.DESCRIPTION,
            STR_TO_DATE(FOUNDING_DATE,'%Y%m%d'),TRUE, 1, STR_TO_DATE(LEFT(C.PTS,8),'%Y%m%d'), STR_TO_DATE('99991231','%Y%m%d')
      FROM S_Company C
      JOIN Industry I ON C.INDUSTRY_ID = I.IN_ID
      JOIN StatusType S ON C.STATUS = S.ST_ID;
    """
    
    # Handle type 2 slowly changing dimension on company
    dim_company_sdc_query = """
    CREATE TABLE sdc_dimcompany
      LIKE DimCompany;
    ALTER TABLE sdc_dimcompany
      ADD COLUMN RN NUMERIC;
    INSERT INTO sdc_dimcompany
    SELECT *, ROW_NUMBER() OVER(ORDER BY CompanyID, EffectiveDate) RN
    FROM DimCompany;

    WITH candidate AS (
    SELECT s1.SK_CompanyID,
          s2.EffectiveDate EndDate
    FROM sdc_dimcompany s1
          JOIN sdc_dimcompany s2 ON (s1.RN = (s2.RN - 1) AND s1.CompanyID = s2.CompanyID))
    UPDATE DimCompany,candidate SET DimCompany.EndDate = candidate.EndDate, DimCompany.IsCurrent=FALSE WHERE DimCompany.SK_CompanyID = candidate.SK_CompanyID;
    DROP TABLE sdc_dimcompany;
    """

    # Construct mysql client bash command to execute ddl and data loading query
    # dim_company_ddl_cmd = TPCDI_Loader.BASE_SQL_CMD+" -D "+self.db_name+" -e \""+dim_company_ddl+"\""
    # dim_company_load_cmd = TPCDI_Loader.BASE_SQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+dim_company_load_query+"\""
    # dim_company_sdc_cmd = TPCDI_Loader.BASE_SQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+dim_company_sdc_query+"\""

    # Execute the command
    os.system(dim_company_ddl_cmd)
    os.system(dim_company_load_cmd)    
    os.system(dim_company_sdc_cmd)
  
  def load_target_dim_security(self):
    """
    Create Security table in the staging database and then load rows by ..
    """
    # Create query to load text data into security table
    security_load_query="""
    INSERT INTO DimSecurity (Symbol,Issue,Status,Name,ExchangeID,SK_CompanyID,SharesOutstanding,FirstTrade,FirstTradeOnExchange,Dividend,IsCurrent,BatchID,EffectiveDate,EndDate)
    SELECT SS.SYMBOL,SS.ISSUE_TYPE, ST.ST_NAME, SS.NAME, SS.EX_ID, DC.SK_CompanyID, SS.SH_OUT, STR_TO_DATE(SS.FIRST_TRADE_DATE,'%Y%m%d'),
          STR_TO_DATE(FIRST_TRADE_EXCHANGE, '%Y%m%d'), SS.DIVIDEN, TRUE, 1, STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d'), STR_TO_DATE('99991231','%Y%m%d')
    FROM S_Security SS
    JOIN StatusType ST ON SS.STATUS = ST.ST_ID
    JOIN DimCompany DC ON DC.SK_CompanyID = convert(SS.COMPANY_NAME_OR_CIK, SIGNED)
                        AND DC.EffectiveDate <= STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d')
                        AND STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d') < DC.EndDate
                        AND LEFT(SS.COMPANY_NAME_OR_CIK,1)='0';

    INSERT INTO DimSecurity (Symbol,Issue,Status,Name,ExchangeID,SK_CompanyID,SharesOutstanding,FirstTrade,FirstTradeOnExchange,Dividend,IsCurrent,BatchID,EffectiveDate,EndDate)
    SELECT SS.SYMBOL,SS.ISSUE_TYPE, ST.ST_NAME, SS.NAME, SS.EX_ID, DC.SK_CompanyID, SS.SH_OUT, STR_TO_DATE(SS.FIRST_TRADE_DATE,'%Y%m%d'),
          STR_TO_DATE(FIRST_TRADE_EXCHANGE, '%Y%m%d'), SS.DIVIDEN, TRUE, 1, STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d'), STR_TO_DATE('99991231','%Y%m%d')
    FROM S_Security SS
    JOIN StatusType ST ON SS.STATUS = ST.ST_ID
    JOIN DimCompany DC ON RTRIM(SS.COMPANY_NAME_OR_CIK) = DC.Name
                        AND DC.EffectiveDate <= STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d')
                        AND STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d') < DC.EndDate
                        AND LEFT(SS.COMPANY_NAME_OR_CIK,1) <> '0';
    """

    dim_security_scd = """
    CREATE TABLE sdc_dimsecurity
      LIKE DimSecurity;
    ALTER TABLE sdc_dimsecurity
      ADD COLUMN RN NUMERIC;
    INSERT INTO sdc_dimsecurity
    SELECT *, ROW_NUMBER() OVER(ORDER BY Symbol, EffectiveDate) RN
    FROM DimSecurity;

    WITH candidate AS (SELECT s1.SK_SecurityID, s2.EffectiveDate EndDate
                      FROM sdc_dimsecurity s1
                              JOIN sdc_dimsecurity s2 ON (s1.RN = (s2.RN - 1) AND s1.Symbol = s2.Symbol))
    UPDATE DimSecurity, candidate
    SET DimSecurity.EndDate   = candidate.EndDate,
        DimSecurity.IsCurrent = FALSE
    WHERE DimSecurity.SK_SecurityID = candidate.SK_SecurityID;
    DROP TABLE sdc_dimsecurity;
    """
    
    # Construct mysql client bash command to execute ddl and data loading query
    # dim_security_ddl_cmd = TPCDI_Loader.BASE_SQL_CMD+" -D "+self.db_name+" -e \""+security_ddl+"\""
    # dim_security_load_cmd = TPCDI_Loader.BASE_SQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+security_load_query+"\""
    # dim_security_scd_cmd = TPCDI_Loader.BASE_SQL_CMD+" -D "+self.db_name+" -e \""+dim_security_scd+"\""

    # Execute the command
    os.system(dim_security_ddl_cmd)
    os.system(dim_security_load_cmd)
    os.system(dim_security_scd_cmd)

  def load_target_financial(self):
    """
    Create Financial table in the staging database and then load rows by ..
    """
    # Create query to load text data into financial table
    financial_load_query="""
    INSERT INTO Financial
      SELECT SK_CompanyID, SF.YEAR, SF.QUARTER, SF.QTR_START_DATE, SF.REVENUE,  SF.EARNINGS, SF.EPS, SF.DILUTED_EPS,SF.MARGIN, SF.INVENTORY, SF.ASSETS, SF.LIABILITIES, SF.SH_OUT, SF.DILUTED_SH_OUT
      FROM S_Financial SF
      JOIN DimCompany DC ON DC.SK_CompanyID = convert(SF.CO_NAME_OR_CIK, SIGNED)
                          AND DC.EffectiveDate <= STR_TO_DATE(LEFT(SF.PTS,8),'%Y%m%d')
                          AND STR_TO_DATE(LEFT(SF.PTS,8),'%Y%m%d') < DC.EndDate
                          AND LEFT(CO_NAME_OR_CIK,1)='0';
    INSERT INTO Financial
      SELECT SK_CompanyID, SF.YEAR, SF.QUARTER, SF.QTR_START_DATE, SF.REVENUE,  SF.EARNINGS, SF.EPS, SF.DILUTED_EPS,SF.MARGIN, SF.INVENTORY, SF.ASSETS, SF.LIABILITIES, SF.SH_OUT, SF.DILUTED_SH_OUT
      FROM S_Financial SF
      JOIN DimCompany DC ON RTRIM(SF.CO_NAME_OR_CIK) = DC.Name
                          AND DC.EffectiveDate <= STR_TO_DATE(LEFT(SF.PTS,8),'%Y%m%d')
                          AND STR_TO_DATE(LEFT(SF.PTS,8),'%Y%m%d') < DC.EndDate
                          AND LEFT(CO_NAME_OR_CIK,1) <> '0'
    """
    
    # Construct mysql client bash command to execute ddl and data loading query
    # dim_financial_ddl_cmd = TPCDI_Loader.BASE_SQL_CMD+" -D "+self.db_name+" -e \""+financial_ddl+"\""
    # dim_financial_load_cmd = TPCDI_Loader.BASE_SQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+financial_load_query+"\""
    
    # Execute the command
    os.system(dim_financial_ddl_cmd)
    os.system(dim_financial_load_cmd)    

  def load_staging_trade_history(self):
    """
    Create s_trade_history table in to the database and then load rows in TradeHistory.txt into it.
    """

    # Create ddl to store tade_history
    tade_history_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE s_trade_history(
      th_t_id NUMERIC(15),
      th_dts DATETIME,
      th_st_id CHAR(4)
    );

    """

    # Create query to load text data into tade_history table
    tade_history_load_query="LOAD DATA LOCAL INFILE '"+self.batch_dir+"TradeHistory.txt' INTO TABLE s_trade_history COLUMNS TERMINATED BY '|';"
    
    # Construct mysql client bash command to execute ddl and data loading query
    # tade_history_ddl_cmd = TPCDI_Loader.BASE_SQL_CMD+" -D "+self.db_name+" -e \""+tade_history_ddl+"\""
    # tade_history_load_cmd = TPCDI_Loader.BASE_SQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+tade_history_load_query+"\""
    
    # Execute the command
    os.system(tade_history_ddl_cmd)
    os.system(tade_history_load_cmd)

  def load_staging_trade(self):
    """
    Create s_trade table in to the database and then load rows in Trade.txt into it.
    """

    # Create ddl to store tade
    tade_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE s_trade(
      cdc_flag CHAR(1),
      cdc_dsn NUMERIC(12),
      t_id NUMERIC(15),
      t_dts DATETIME,
      t_st_id CHAR(4),
      t_tt_id CHAR(3),
      t_is_cash CHAR(3),
      t_s_symb CHAR(15) NOT NULL,
      t_qty NUMERIC(6) NOT NULL,
      t_bid_price NUMERIC(8),
      t_ca_id NUMERIC(11),
      t_exec_name CHAR(49),
      t_trade_price NUMERIC(8),
      t_chrg NUMERIC(10),
      t_comm NUMERIC(10),
      t_tax NUMERIC(10)
    );


    """

    if self.batch_number == 1:
      # Create query to load text data into tade_ table
      tade_load_query="LOAD DATA LOCAL INFILE '"+self.batch_dir+"Trade.txt' INTO TABLE s_trade COLUMNS TERMINATED BY '|' \
      (t_id,t_dts,t_st_id,t_tt_id,t_is_cash,t_s_symb,t_qty,t_bid_price,t_ca_id,t_exec_name,t_trade_price,t_chrg,t_comm,t_tax);"
    else:
      # Create query to load text data into tade_ table
      tade_load_query="LOAD DATA LOCAL INFILE '"+self.batch_dir+"Trade.txt' INTO TABLE s_trade COLUMNS TERMINATED BY '|'"
      

    # Construct mysql client bash command to execute ddl and data loading query
    # tade_ddl_cmd = TPCDI_Loader.BASE_SQL_CMD+" -D "+self.db_name+" -e \""+tade_ddl+"\""
    # tade_load_cmd = TPCDI_Loader.BASE_SQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+tade_load_query+"\""
    
    # Execute the command
    os.system(tade_ddl_cmd)
    os.system(tade_load_cmd)