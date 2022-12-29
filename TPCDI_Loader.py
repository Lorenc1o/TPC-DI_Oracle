import os
import glob
import xmltodict
import oracledb

from utils import char_insert

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
    # Drop database if it is exists and overwrite param is set to True
    if overwrite:
      # dropping the tables
      cmd = TPCDI_Loader.BASE_SQL_CMD+' @%s' % (drop_sql)
      os.system(cmd)

      # Create the tables
      cmd = TPCDI_Loader.BASE_SQL_CMD+' @%s' % (create_sql)
      os.system(cmd)
    

  def load_current_batch_date(self):
    print("Loading batch date...")
    with open(self.batch_dir+"BatchDate.txt", "r") as batch_date_file:
      cmd = TPCDI_Loader.BASE_SQL_CMD+' @%s' % (self.load_path+'/BatchDate.sql')
      cmd += ' %s %s' % (self.batch_number,batch_date_file.read().strip())
      os.system(cmd)
    print("Done.")
  
  def load_dim_date(self):
    """
    Create DimDate table in the staging database and then load rows in Date.txt into it.
    """
    print("Loading dim date...")
    # Create query to load text data into dimDate table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/DimDate.ctl', self.batch_dir + 'Date.txt')
    os.system(cmd)
    print("Done.")

  def load_dim_time(self):
    """
    Create DimTime table in the staging database and then load rows in Time.txt into it.
    """
    print("Loading dim time...")
    # Create query to load text data into dimTime table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/DimTime.ctl', self.batch_dir + 'Time.txt')
    os.system(cmd)
    print("Done.")
    
  def load_industry(self):
    """
    Create Industry table in the staging database and then load rows in Industry.txt into it.
    """
    print("Loading industry...")
    # Create query to load text data into industry table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/Industry.ctl', self.batch_dir + 'Industry.txt')
    os.system(cmd)
    print("Done.")

  def load_status_type(self):
    """
    Create StatusType table in the staging database and then load rows in StatusType.txt into it.
    """
    print("Loading status type...")
    # Create query to load text data into statusType table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/StatusType.ctl', self.batch_dir + 'StatusType.txt')
    os.system(cmd)
    print("Done.")

  def load_tax_rate(self):
    """
    Create TaxRate table in the staging database and then load rows in TaxRate.txt into it.
    """
    print("Loading tax rate...")
    # Create query to load text data into taxRate table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/TaxRate.ctl', self.batch_dir + 'TaxRate.txt')
    os.system(cmd)
    print("Done.")
    
  def load_trade_type(self):
    """
    Create TradeType table in the staging database and then load rows in TradeType.txt into it.
    """
    print("Loading trade type...")
    # Create query to load text data into tradeType table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/TradeType.ctl', self.batch_dir + 'TradeType.txt')
    os.system(cmd)
    print("Done.")
        
  def load_staging_customer_account(self):
    print("Loading staging customer and account...")
    customer_inserts = []
    account_inserts = []
    max_packet = 150
    with open(self.batch_dir + '/CustomerMgmt.xml') as fd:
      doc = xmltodict.parse(fd.read())
      actions = doc['TPCDI:Actions']['TPCDI:Action']
      for action in actions:
        action_type = action['@ActionType']
        # Customer fields
        try:
          c_id = action['Customer']['@C_ID']
        except:
          c_id = None
        try:
          c_tax_id = action['Customer']['@C_TAX_ID']
        except:
          c_tax_id = None
        try:
          c_l_name = action['Customer']['Name']['C_L_NAME']
        except:
          c_l_name = None
        try:
          c_f_name = action['Customer']['Name']['C_F_NAME']
        except:
          c_f_name = None
        try:
          c_m_name = action['Customer']['Name']['C_M_NAME']
        except:
          c_m_name = None
        try:
          c_tier = action['Customer']['@C_TIER']
          if c_tier == '':
            c_tier = 0
        except:
          c_tier = 0
        try:
          c_dob = action['Customer']['@C_DOB']
        except:
          c_dob = None
        try:
          c_prim_email = action['Customer']['ContactInfo']['C_PRIM_EMAIL']
        except:
          c_prim_email = None
        try:
          c_alt_email = action['Customer']['ContactInfo']['C_ALT_EMAIL']
        except:
          c_alt_email = None
        try:
          c_gndr = action['Customer']['@C_GNDR'].upper()
        except:
          c_gndr = 'U'
        if c_gndr != 'M' and c_gndr != 'F':
          c_gndr = 'U'
        try:
          c_adline1 = action['Customer']['Address']['C_ADLINE1']
        except:
          c_adline1 = None
        try:
          c_adline2 = action['Customer']['Address']['C_ADLINE2']
        except:
          c_adline2 = None
        try:
          c_zipcode = action['Customer']['Address']['C_ZIPCODE']
        except:
          c_zipcode = None
        try:
          c_city = action['Customer']['Address']['C_CITY']
        except:
          c_city = None
        try:
          c_state_prov = action['Customer']['Address']['C_STATE_PROV']
        except:
          c_state_prov = None
        try:
          c_ctry = action['Customer']['Address']['C_CTRY']
        except:
          c_ctry = None
        c_status = 'ACTIVE'
        phones = []
        try:
          phones.append(action['Customer']['ContactInfo']['C_PHONE_1'])
        except:
          phones.append(None)
        try:
          phones.append(action['Customer']['ContactInfo']['C_PHONE_2'])
        except:
          phones.append(None)
        try:
          phones.append(action['Customer']['ContactInfo']['C_PHONE_3'])
        except:
          phones.append(None)
        phone_numbers = []
        for phone in phones:
          try:
            c_ctry_code = phone['C_CTRY_CODE']
          except:
            c_ctry_code = None
          try:
            c_area_code = phone['C_AREA_CODE']
          except:
            c_area_code = None
          try:
            c_local = phone['C_LOCAL']
          except:
            c_local = None
          phone_number = None
          if c_ctry_code is not None and c_area_code is not None and c_local is not None:
            phone_number = '+' + c_ctry_code + '(' + c_area_code + ')' + c_local
          elif c_ctry_code is None and c_area_code is not None and c_local is not None:
            phone_number = '(' + c_area_code + ')' + c_local
          elif c_ctry_code is None and c_area_code is None and c_local is not None:
            phone_number = c_local
          if phone_number is not None and phone['C_EXT'] is not None:
            phone_number = phone_number + phone['C_EXT']
          phone_numbers.append(phone_number)
          try:
            c_nat_tx_id = action['Customer']['TaxInfo']['C_NAT_TX_ID']
          except:
            c_nat_tx_id = None
          try:
            c_lcl_tx_id = action['Customer']['TaxInfo']['C_LCL_TX_ID']
          except:
            c_lcl_tx_id = None
          try:
            action_ts_date = action['@ActionTS'][0:10]
          except:
            action_ts_date = None
        if c_id is not None:
          insert_customer = f"""
          INSERT INTO S_Customer (ActionType, CustomerID, TaxID, Status, LastName, FirstName, MiddleInitial, Gender, Tier, DOB, AddressLine1, AddressLine2, PostalCode,
            City, StateProv, Country, Phone1, Phone2, Phone3, Email1, Email2, NationalTaxRateDesc, NationalTaxRate, LocalTaxRateDesc, LocalTaxRate, EffectiveDate, EndDate, BatchId)
          VALUES ('{char_insert(action_type)}', {c_id}, '{char_insert(c_tax_id)}', '{char_insert(c_status)}', '{char_insert(c_l_name)}', '{char_insert(c_f_name)}', '{char_insert(c_m_name)}', 
            '{char_insert(c_gndr)}', {c_tier}, TO_DATE('{char_insert(c_dob)}', 'yyyy-mm-dd'), '{char_insert(c_adline1)}', '{char_insert(c_adline2)}', '{char_insert(c_zipcode)}', 
            '{char_insert(c_city)}', '{char_insert(c_state_prov)}', '{char_insert(c_ctry)}', '{char_insert(phone_numbers[0])}', '{char_insert(phone_numbers[1])}', 
            '{char_insert(phone_numbers[2])}', '{char_insert(c_prim_email)}', '{char_insert(c_alt_email)}', 
            (SELECT TX_NAME FROM TaxRate WHERE TX_ID = '{char_insert(c_nat_tx_id)}'), (SELECT TX_RATE FROM TaxRate WHERE TX_ID = '{char_insert(c_nat_tx_id)}'),
            (SELECT TX_NAME FROM TaxRate WHERE TX_ID = '{char_insert(c_lcl_tx_id)}'), (SELECT TX_RATE FROM TaxRate WHERE TX_ID = '{char_insert(c_lcl_tx_id)}'),
            TO_DATE('{char_insert(action_ts_date)}', 'yyyy-mm-dd'), TO_DATE('9999-12-31', 'yyyy-mm-dd'), {self.batch_number})
          """
          customer_inserts.append(insert_customer)
        # Account fields
        try:
          a_id = action['Customer']['Account']['@CA_ID']
        except:
          a_id = None
        try:
          a_Desc = action['Customer']['Account']['CA_NAME']
        except:
          a_Desc = None
        try:
          a_taxStatus = action['Customer']['Account']['@CA_TAX_ST']
        except:
          a_taxStatus = None
        try:
          a_brokerID = action['Customer']['Account']['CA_B_ID']
        except:
          a_brokerID = None
        # action_type we have it already
        try:
          action_ts_date = action['@ActionTS'][0:10]
        except:
          action_ts_date = None
        if a_id is not None:
          insert_account = f"""
          INSERT INTO S_Account (ActionType, AccountID, Status, BrokerID, CustomerID, AccountDesc, TaxStatus, EffectiveDate, EndDate, BatchId)
          VALUES ('{char_insert(action_type)}', {a_id}, 'Active', '{char_insert(a_brokerID)}', '{char_insert(c_id)}', '{char_insert(a_Desc)}', '{char_insert(a_taxStatus)}',
            TO_DATE('{char_insert(action_ts_date)}', 'yyyy-mm-dd'), TO_DATE('9999-12-31', 'yyyy-mm-dd'), {self.batch_number})
          """
          account_inserts.append(insert_account)

        if len(customer_inserts) + len(account_inserts) >= max_packet:
                # Create query to load text data into tradeType table
                with oracledb.connect(
                  user=self.oracle_user, password=self.oracle_pwd, 
                  dsn=self.oracle_host+'/'+self.oracle_db) as connection:
                  with connection.cursor() as cursor:
                    for ins in customer_inserts:
                      cursor.execute(ins)
                    connection.commit()
                    customer_inserts = []
                    for ins in account_inserts:
                      cursor.execute(ins)
                    connection.commit()
                    account_inserts = []
    print('Done.')
  
  def load_new_customer(self):
    """
    Load NEW customers into DimCustomer table
    """
    print('Loading new customers into DimCustomer table...')
    # First, we insert all NEW customers into the DimCustomer table
    load_query = """
      INSERT INTO DimCustomer(CustomerID, TaxID, Status, LastName, FirstName, MiddleInitial, Gender, Tier, DOB, AddressLine1, AddressLine2, PostalCode,
                City, StateProv, Country, Phone1, Phone2, Phone3, Email1, Email2, NationalTaxRateDesc, NationalTaxRate, LocalTaxRateDesc, LocalTaxRate, EffectiveDate, 
                EndDate, BatchId, AgencyID, CreditRating, NetWorth, MarketingNameplate, IsCurrent)
      WITH Copied AS (
          SELECT C.CustomerID, C.TaxID, C.Status, C.LastName, C.FirstName, C.MiddleInitial, C.Gender, C.Tier, C.DOB, C.AddressLine1, C.AddressLine2, C.PostalCode,
              C.City, C.StateProv, C.Country, C.Phone1, C.Phone2, C.Phone3, C.Email1, C.Email2, C.NationalTaxRateDesc, C.NationalTaxRate, C.LocalTaxRateDesc, C.LocalTaxRate, C.EffectiveDate, 
              C.EndDate, C.BatchId, P.AgencyID, P.CreditRating, P.NetWorth, P.MarketingNameplate
          FROM Prospect P, S_Customer C
          WHERE C.ActionType = 'NEW' AND
              P.FirstName = C.FirstName AND 
              UPPER(P.LastName) = UPPER(C.LastName) AND
              TRIM(UPPER(P.AddressLine1)) = TRIM(UPPER(C.AddressLine1)) AND
              TRIM(UPPER(P.AddressLine2)) = TRIM(UPPER(C.AddressLine2)) AND
              TRIM(UPPER(P.PostalCode)) = TRIM(UPPER(C.PostalCode)) AND
              NOT EXISTS (SELECT * 
                  FROM S_Customer C1 
                  WHERE C.CustomerID = C1.CustomerID AND
                      (C1.ActionType = 'UPDCUST' OR C1.ActionType = 'INACT') AND
                      C1.EffectiveDate > C.EffectiveDate
              )
      )
      SELECT C.CustomerID, C.TaxID, C.Status, C.LastName, C.FirstName, C.MiddleInitial, C.Gender, C.Tier, C.DOB, C.AddressLine1, C.AddressLine2, C.PostalCode,
              C.City, C.StateProv, C.Country, C.Phone1, C.Phone2, C.Phone3, C.Email1, C.Email2, C.NationalTaxRateDesc, C.NationalTaxRate, C.LocalTaxRateDesc, C.LocalTaxRate, C.EffectiveDate, 
              C.EndDate, C.BatchId, CP.AgencyID, CP.CreditRating, CP.NetWorth, CP.MarketingNameplate, 'true'
      FROM S_Customer C LEFT OUTER JOIN Copied CP ON (C.CustomerID = CP.CustomerID)
      WHERE C.ActionType = 'NEW'
    """
    with oracledb.connect(
      user=self.oracle_user, password=self.oracle_pwd, 
      dsn=self.oracle_host+'/'+self.oracle_db) as connection:
      with connection.cursor() as cursor:
        cursor.execute(load_query)
      connection.commit()
    print('Done.')

  def load_new_account(self):
    """
    Load NEW accounts in S_Account into DimAccount table in the target database.
    """
    print('Loading new accounts...')
    # First, we insert all NEW rows from S_Account into DimAccount
    load_query = """
      INSERT INTO DimAccount (AccountID, SK_BrokerID, SK_CustomerID, Status, AccountDesc, TaxStatus, IsCurrent, BatchID, EffectiveDate, EndDate)
      WITH Copied AS (
        SELECT A.AccountID, B.SK_BrokerID, C.SK_CustomerID, A.Status, A.AccountDesc, A.TaxStatus, A.BatchID, A.EffectiveDate, A.EndDate
        FROM S_Account A
        JOIN DimBroker B ON A.BrokerID = B.BrokerID
        JOIN DimCustomer C ON A.CustomerID = C.CustomerID
        WHERE A.ActionType IN ('NEW', 'ADDACCT') AND
          NOT EXISTS (
            SELECT * FROM S_Account A1
            WHERE A.AccountID = A1.AccountID AND
              (A1.ActionType = 'UPDACC' OR A1.ActionType = 'INACT') AND
              A1.EffectiveDate > A.EffectiveDate
          )
      )
      SELECT A.AccountID, CP.SK_BrokerID, CP.SK_CustomerID, A.Status, A.AccountDesc, A.TaxStatus, 'true', A.BatchID, A.EffectiveDate, A.EndDate
      FROM S_Account A
      LEFT OUTER JOIN Copied CP ON (A.AccountID = CP.AccountID)
      WHERE A.ActionType IN ('NEW', 'ADDACCT')
    """
    with oracledb.connect(
      user=self.oracle_user, password=self.oracle_pwd, 
      dsn=self.oracle_host+'/'+self.oracle_db) as connection:
      with connection.cursor() as cursor:
        cursor.execute(load_query)
      connection.commit()
    print('Done.')

  def create_trigger_UPD_customer_account(self):
    """
    Create a trigger that will update the DimAccount table when a customer is updated.
    """
    print('Creating trigger UPD_CUSTOMER_ACCOUNT...')
    create_trigger_query = """
      CREATE OR REPLACE TRIGGER UPD_CUSTOMER_ACCOUNT
      AFTER UPDATE OF SK_CustomerID ON DimAccount
      FOR EACH ROW
      BEGIN
        UPDATE DimAccount
        SET SK_CustomerID = :new.SK_CustomerID
        WHERE AccountID = :old.AccountID;
      END;
    """
    with oracledb.connect(
      user=self.oracle_user, password=self.oracle_pwd, 
      dsn=self.oracle_host+'/'+self.oracle_db) as connection:
      with connection.cursor() as cursor:
        cursor.execute(create_trigger_query)
      connection.commit()
    print('Done.')

  def load_update_customer(self):
    # Now we update all fields in the DimCustomer table with the latest values from S_Customer
    print('Updating DimCustomer table...')
    base_update_query = """
      UPDATE DimCustomer C
      SET %s = (
        SELECT MAX(%s)
        FROM S_Customer C1
        WHERE C1.CustomerID = C.CustomerID AND
          C1.%s IS NOT NULL AND
          C1.ActionType = 'UPDCUST' AND
          NOT EXISTS (
            SELECT * FROM S_Customer C2 
            WHERE C2.CustomerID = C1.CustomerID 
            AND C2.ActionType = 'UPDCUST' AND C2.EffectiveDate > C1.EffectiveDate)
      )
      WHERE EXISTS (
        SELECT * FROM S_Customer C1
        WHERE C1.CustomerID = C.CustomerID AND
          C1.%s IS NOT NULL AND
          C1.ActionType = 'UPDCUST' AND
          NOT EXISTS (
            SELECT * FROM S_Customer C2
            WHERE C2.CustomerID = C1.CustomerID AND
              C2.ActionType = 'UPDCUST' AND C2.EffectiveDate > C1.EffectiveDate)
      )
    """
    update_query_status = base_update_query % ('C.Status', 'C1.Status', 'Status', 'Status')
    update_query_last_name = base_update_query % ('C.LastName', 'C1.LastName', 'LastName', 'LastName')
    update_query_first_name = base_update_query % ('C.FirstName', 'C1.FirstName', 'FirstName', 'FirstName')
    update_query_middle_initial = base_update_query % ('C.MiddleInitial', 'C1.MiddleInitial', 'MiddleInitial', 'MiddleInitial')
    update_query_gender = base_update_query % ('C.Gender', 'C1.Gender', 'Gender', 'Gender')
    update_query_tier = base_update_query % ('C.Tier', 'C1.Tier', 'Tier', 'Tier')
    update_query_dob = base_update_query % ('C.DOB', 'C1.DOB', 'DOB', 'DOB')
    update_query_address_line1 = base_update_query % ('C.AddressLine1', 'C1.AddressLine1', 'AddressLine1', 'AddressLine1')
    update_query_address_line2 = base_update_query % ('C.AddressLine2', 'C1.AddressLine2', 'AddressLine2', 'AddressLine2')
    update_query_postal_code = base_update_query % ('C.PostalCode', 'C1.PostalCode', 'PostalCode', 'PostalCode')
    update_query_city = base_update_query % ('C.City', 'C1.City', 'City', 'City')
    update_query_state_prov = base_update_query % ('C.StateProv', 'C1.StateProv', 'StateProv', 'StateProv')
    update_query_country = base_update_query % ('C.Country', 'C1.Country', 'Country', 'Country')
    update_query_phone1 = base_update_query % ('C.Phone1', 'C1.Phone1', 'Phone1', 'Phone1')
    update_query_phone2 = base_update_query % ('C.Phone2', 'C1.Phone2', 'Phone2', 'Phone2')
    update_query_phone3 = base_update_query % ('C.Phone3', 'C1.Phone3', 'Phone3', 'Phone3')
    update_query_email1 = base_update_query % ('C.Email1', 'C1.Email1', 'Email1', 'Email1')
    update_query_email2 = base_update_query % ('C.Email2', 'C1.Email2', 'Email2', 'Email2')
    update_query_national_tax_rate_desc = base_update_query % ('C.NationalTaxRateDesc', 'C1.NationalTaxRateDesc', 'NationalTaxRateDesc', 'NationalTaxRateDesc')
    update_query_national_tax_rate = base_update_query % ('C.NationalTaxRate', 'C1.NationalTaxRate', 'NationalTaxRate', 'NationalTaxRate')
    update_query_local_tax_rate_desc = base_update_query % ('C.LocalTaxRateDesc', 'C1.LocalTaxRateDesc', 'LocalTaxRateDesc', 'LocalTaxRateDesc')
    update_query_local_tax_rate = base_update_query % ('C.LocalTaxRate', 'C1.LocalTaxRate', 'LocalTaxRate', 'LocalTaxRate')

    # To finalize the update, we need to update the values from Prospect
    base_update_prospect_query = """
    UPDATE DimCustomer C
      SET C.AgencyID = (
        SELECT MAX(CP.AgencyID)
        FROM (SELECT P.AgencyID
              FROM Prospect P
              WHERE P.FirstName = C.FirstName AND 
                  UPPER(P.LastName) = UPPER(C.LastName) AND
                  TRIM(UPPER(P.AddressLine1)) = TRIM(UPPER(C.AddressLine1)) AND
                  TRIM(UPPER(P.AddressLine2)) = TRIM(UPPER(C.AddressLine2)) AND
                  TRIM(UPPER(P.PostalCode)) = TRIM(UPPER(C.PostalCode))
              ) CP),
          C.CreditRating = (
          SELECT MAX(CP.CreditRating)
          FROM (SELECT P.CreditRating
                FROM Prospect P
                WHERE P.FirstName = C.FirstName AND 
                    UPPER(P.LastName) = UPPER(C.LastName) AND
                    TRIM(UPPER(P.AddressLine1)) = TRIM(UPPER(C.AddressLine1)) AND
                    TRIM(UPPER(P.AddressLine2)) = TRIM(UPPER(C.AddressLine2)) AND
                    TRIM(UPPER(P.PostalCode)) = TRIM(UPPER(C.PostalCode))
                ) CP),
          C.NetWorth = (
          SELECT MAX(CP.NetWorth)
          FROM (SELECT P.NetWorth
                FROM Prospect P
                WHERE P.FirstName = C.FirstName AND 
                    UPPER(P.LastName) = UPPER(C.LastName) AND
                    TRIM(UPPER(P.AddressLine1)) = TRIM(UPPER(C.AddressLine1)) AND
                    TRIM(UPPER(P.AddressLine2)) = TRIM(UPPER(C.AddressLine2)) AND
                    TRIM(UPPER(P.PostalCode)) = TRIM(UPPER(C.PostalCode))
                ) CP),
          C.MarketingNameplate = (
          SELECT MAX(CP.MarketingNameplate)
          FROM (SELECT P.MarketingNameplate
                FROM Prospect P
                WHERE P.FirstName = C.FirstName AND 
                    UPPER(P.LastName) = UPPER(C.LastName) AND
                    TRIM(UPPER(P.AddressLine1)) = TRIM(UPPER(C.AddressLine1)) AND
                    TRIM(UPPER(P.AddressLine2)) = TRIM(UPPER(C.AddressLine2)) AND
                    TRIM(UPPER(P.PostalCode)) = TRIM(UPPER(C.PostalCode))
                ) CP)
      WHERE EXISTS (
        SELECT * FROM S_Customer C1
        WHERE C1.CustomerID = C.CustomerID AND
          C1.ActionType = 'UPDCUST' AND
          NOT EXISTS (
            SELECT * FROM S_Customer C2
            WHERE C2.CustomerID = C1.CustomerID AND
              C2.ActionType = 'UPDCUST' AND C2.EffectiveDate > C1.EffectiveDate)
      )
    """
    with oracledb.connect(
      user=self.oracle_user, password=self.oracle_pwd, 
      dsn=self.oracle_host+'/'+self.oracle_db) as connection:
      with connection.cursor() as cursor:
        print('...')
        cursor.execute(update_query_status)
        cursor.execute(update_query_last_name)
        cursor.execute(update_query_first_name)
        print('...')
        cursor.execute(update_query_middle_initial)
        cursor.execute(update_query_gender)
        cursor.execute(update_query_tier)
        print('...')
        cursor.execute(update_query_dob)
        cursor.execute(update_query_address_line1)
        cursor.execute(update_query_address_line2)
        print('...')
        cursor.execute(update_query_postal_code)
        cursor.execute(update_query_city)
        cursor.execute(update_query_state_prov)
        print('...')
        cursor.execute(update_query_country)
        cursor.execute(update_query_phone1)
        cursor.execute(update_query_phone2)
        print('...')
        cursor.execute(update_query_phone3)
        cursor.execute(update_query_email1)
        cursor.execute(update_query_email2)
        print('...')
        cursor.execute(update_query_national_tax_rate_desc)
        cursor.execute(update_query_national_tax_rate)
        cursor.execute(update_query_local_tax_rate_desc)
        cursor.execute(update_query_local_tax_rate)
        print('...')
        cursor.execute(base_update_prospect_query)
      connection.commit()
    print('Done.')

  def load_update_account(self):
    # Now we update the DimAccount table
    print('Updating accounts...')
    base_update_query = """
    UPDATE DimAccount A
      SET A.%s = (
        SELECT MAX(A1.%s)
        FROM S_Account A1
        WHERE A1.AccountID = A.AccountID AND
          A1.ActionType = 'UPDACCT' AND
          NOT EXISTS (
            SELECT * FROM S_Account A2
            WHERE A2.AccountID = A1.AccountID AND
              A2.ActionType = 'UPDACCT' AND A2.EffectiveDate > A1.EffectiveDate)
      )
      WHERE EXISTS (
        SELECT * FROM S_Account A1
        WHERE A1.AccountID = A.AccountID AND
          A1.ActionType = 'UPDACCT' AND
          NOT EXISTS (
            SELECT * FROM S_Account A2
            WHERE A2.AccountID = A1.AccountID AND
              A2.ActionType = 'UPDACCT' AND A2.EffectiveDate > A1.EffectiveDate)
      )
    """
    update_query_status = base_update_query % ('Status', 'Status')
    update_query_account_desc = base_update_query % ('AccountDesc', 'AccountDesc')
    update_query_account_taxstatus = base_update_query % ('TaxStatus', 'TaxStatus')
    with oracledb.connect(
      user=self.oracle_user, password=self.oracle_pwd, 
      dsn=self.oracle_host+'/'+self.oracle_db) as connection:
      with connection.cursor() as cursor:
        cursor.execute(update_query_status)
        cursor.execute(update_query_account_desc)
        cursor.execute(update_query_account_taxstatus)
      connection.commit()
    print('Done.')

  def load_close_account(self):
    # Now, we update the Status field for all rows in the DimAccount table
    # for which there is a row in S_Account with an ActionType of 'CLOSEACCT'
    print('Closing accounts...')
    update_query_status = """
      UPDATE DimAccount A
      SET A.Status = 'Inactive'
      WHERE EXISTS (
        SELECT * FROM S_Account A1
        WHERE A1.AccountID = A.AccountID AND
          A1.ActionType = 'CLOSEACCT' AND
          NOT EXISTS (
            SELECT * FROM S_Account A2
            WHERE A2.AccountID = A1.AccountID AND
              A2.ActionType = 'UPDACCT' AND A2.EffectiveDate > A1.EffectiveDate)
      )
    """
    with oracledb.connect(
      user=self.oracle_user, password=self.oracle_pwd, 
      dsn=self.oracle_host+'/'+self.oracle_db) as connection:
      with connection.cursor() as cursor:
        cursor.execute(update_query_status)
      connection.commit()
    print('Done.')

  def load_inact_customer(self):
    # Finally, we update the EndDate field and the isCurrent field for all rows in the DimCustomer table
    # for which there is a row in S_Customer with an ActionType of 'INACT'
    print('Updating inactive customers...')
    update_query_end_date = """
      UPDATE DimCustomer C
      SET C.EndDate = (
        SELECT MAX(C1.EffectiveDate)
        FROM S_Customer C1
        WHERE C1.CustomerID = C.CustomerID AND
          C1.ActionType = 'INACT' AND
          NOT EXISTS (
            SELECT * FROM S_Customer C2
            WHERE C2.CustomerID = C1.CustomerID AND
              C2.ActionType = 'INACT' AND C2.EffectiveDate > C1.EffectiveDate)
      ),
      C.isCurrent = 'false'
      WHERE EXISTS (
        SELECT * FROM S_Customer C1
        WHERE C1.CustomerID = C.CustomerID AND
          C1.ActionType = 'INACT' AND
          NOT EXISTS (
            SELECT * FROM S_Customer C2
            WHERE C2.CustomerID = C1.CustomerID AND
              C2.ActionType = 'INACT' AND C2.EffectiveDate > C1.EffectiveDate)
      )
    """
    with oracledb.connect(
      user=self.oracle_user, password=self.oracle_pwd, 
      dsn=self.oracle_host+'/'+self.oracle_db) as connection:
      with connection.cursor() as cursor:
        cursor.execute(update_query_end_date)
      connection.commit()
    print('Done.')
  
  def load_inact_account(self):
    # Finally, we update the EndDate field, the Status and the isCurrent field for all rows in the DimAccount table
    # for which there is a row in S_Account with an ActionType of 'INACT'
    print('Updating inactive accounts...')
    update_query_end_date = """
      UPDATE DimAccount A
      SET A.EndDate = (
        SELECT MAX(A1.EffectiveDate)
        FROM S_Account A1
        WHERE A1.AccountID = A.AccountID AND
          A1.ActionType = 'INACT' AND
          NOT EXISTS (
            SELECT * FROM S_Account A2
            WHERE A2.AccountID = A1.AccountID AND
              A2.ActionType = 'INACT' AND A2.EffectiveDate > A1.EffectiveDate)
      ),
      A.Status = 'Inactive',
      A.isCurrent = 'false'
      WHERE EXISTS (
        SELECT * FROM S_Account A1
        WHERE A1.AccountID = A.AccountID AND
          A1.ActionType = 'INACT' AND
          NOT EXISTS (
            SELECT * FROM S_Account A2
            WHERE A2.AccountID = A1.AccountID AND
              A2.ActionType = 'INACT' AND A2.EffectiveDate > A1.EffectiveDate)
      )
    """
    with oracledb.connect(
      user=self.oracle_user, password=self.oracle_pwd, 
      dsn=self.oracle_host+'/'+self.oracle_db) as connection:
      with connection.cursor() as cursor:
        cursor.execute(update_query_end_date)
      connection.commit()
    print('Done.')
  
  def update_prospect(self):
    upd_query = """
      UPDATE Prospect P
      SET P.IsCustomer  = 'true'
      WHERE EXISTS (
        SELECT *
        FROM DimCustomer C
        WHERE UPPER(P.LastName) = UPPER(C.LastName) AND
              TRIM(UPPER(P.AddressLine1)) = TRIM(UPPER(C.AddressLine1)) AND
              TRIM(UPPER(P.AddressLine2)) = TRIM(UPPER(C.AddressLine2)) AND
              TRIM(UPPER(P.PostalCode)) = TRIM(UPPER(C.PostalCode))
      )
    """
    print('Updating prospect customer info...')
    with oracledb.connect(
      user=self.oracle_user, password=self.oracle_pwd, 
      dsn=self.oracle_host+'/'+self.oracle_db) as connection:
      with connection.cursor() as cursor:
        cursor.execute(upd_query)
      connection.commit()
    print('Done.')

  def load_staging_broker(self):
    """
    Load rows in HR.csv into S_Broker table in staging database.
    """
    print('Loading staging broker...')
    # Create query to load txt data into S_Watches table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/Broker.ctl', self.batch_dir + 'HR.csv')
    os.system(cmd)
    print('Done.')

  def load_broker(self):
    """
    Create DimBroker table in the target database and then load rows in HR.csv into it.
    """
    print('Loading broker...')
    load_dim_broker_query = """
      INSERT INTO DimBroker (BrokerID,ManagerID,FirstName,LastName,MiddleInitial,Branch,Office,Phone,IsCurrent,BatchID,EffectiveDate,EndDate)
      SELECT SB.EmployeeID, SB.ManagerID, SB.EmployeeFirstName, SB.EmployeeLastName, SB.EmployeeMI, SB.EmployeeBranch, SB.EmployeeOffice, SB.EmployeePhone, 'true', %d, (SELECT MIN(DateValue) FROM DimDate), TO_DATE('9999/12/31', 'yyyy/mm/dd')
      FROM S_Broker SB
      WHERE SB.EmployeeJobCode = 314
    """ % (self.batch_number)

    with oracledb.connect(
      user=self.oracle_user, password=self.oracle_pwd, 
      dsn=self.oracle_host+'/'+self.oracle_db) as connection:
      with connection.cursor() as cursor:
        cursor.execute(load_dim_broker_query)
      connection.commit()
    print('Done.')


  def load_staging_cash_balances(self):
    """
    Load rows in CashTransaction.txt into S_Cash_Balances table in staging database.
    """
    print('Loading staging cash balances...')
    # Create query to load txt data into S_Watches table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/CashBalances.ctl', self.batch_dir + 'CashTransaction.txt')
    os.system(cmd)
    print('Done.')

  def load_staging_watches(self):
    """
    Load rows in WatchHistory.txt into S_Watches table in staging database.
    """
    print('Loading staging watches...')
    # Create query to load txt data into S_Watches table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/Watches.ctl', self.batch_dir + 'WatchHistory.txt')
    os.system(cmd)
    print('Done.')

  def load_staging_prospect(self):
    """
    Load rows in Prospect.csv into S_Prospect table in staging database.
    """
    print('Loading staging prospect...')
    # Create query to load csv data into S_Prospect table
    cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/Prospect.ctl', self.batch_dir + 'Prospect.csv')
    os.system(cmd)
    print('Done.')

  def load_prospect(self):
    print('Loading prospect...')
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

    with oracledb.connect(
      user=self.oracle_user, password=self.oracle_pwd, 
      dsn=self.oracle_host+'/'+self.oracle_db) as connection:
      with connection.cursor() as cursor:
        cursor.execute(marketing_nameplate_func)
        cursor.execute(load_prospect_query)
      connection.commit()
    print('Done.')
  
  def load_audit(self):
    """
    Create Audit table in the staging database and then load rows in files with "_audit.csv" ending into it.
    """
    print('Loading staging audit...')
    for filepath in glob.iglob(self.batch_dir+"*_audit.csv"):# Create query to load text data into tradeType table
      cmd = TPCDI_Loader.BASE_SQLLDR_CMD+' control=%s data=%s' % (self.load_path+'/Audit.ctl', filepath)
      os.system(cmd)
    print('Done.')

  def load_staging_finwire(self):
    """
    Create S_Company and S_Security table in the staging database and then load rows in FINWIRE files with the type of CMP
    """
    print('Loading staging company, security and financial...')
    base_path = "../staging/"+self.sf+"/Batch1/"
    s_company_base_query = "INSERT INTO S_Company"
    s_security_base_query = "INSERT INTO S_Security"
    s_financial_base_query = "INSERT INTO S_Financial"
    s_company_values = []
    s_security_values = []
    s_financial_values = []
    max_packet = 150
    for fname in os.listdir(base_path):
      if("FINWIRE" in fname and "audit" not in fname):
        with open(base_path+fname, 'r') as finwire_file:
          for line in finwire_file:
            pts = line[:15] #0
            rec_type=line[15:18] #1

            if rec_type=="CMP":
              company_name = line[18:78] #2
              # check if company name is made of blanks
              if company_name.strip() == "":
                company_name = "NULL"
              cik = line[78:88] #3
              status = line[88:92] #4
              industry_id = line[92:94] #5
              sp_rating = line[94:98] # 6
              # check if sp rating is made of blanks
              if sp_rating.strip() == "":
                sp_rating = "NULL"
              founding_date = line[98:106] #7
              # check if founding date is made of blanks
              if founding_date.strip() == "":
                founding_date = "NULL"
              addr_line_1 = line[106:186] #8
              # check if address line 1 is made of blanks
              if addr_line_1.strip() == "":
                addr_line_1 = "NULL"
              addr_line_2 = line[186:266] #9
              # check if address line 2 is made of blanks
              if addr_line_2.strip() == "":
                addr_line_2 = "NULL"
              postal_code = line[266:278] #10
              # check if postal code is made of blanks
              if postal_code.strip() == "":
                postal_code = "NULL"
              city = line[278:303] #10
              # check if city is made of blanks
              if city.strip() == "":
                city = "NULL"
              state_province = line[303:323] #11
              # check if state province is made of blanks
              if state_province.strip() == "":
                state_province = "NULL"
              country = line[323:347] #12
              # check if country is made of blanks
              if country.strip() == "":
                country = "NULL"
              ceo_name = line[347:393] #13
              # check if ceo name is made of blanks
              if ceo_name.strip() == "":
                ceo_name = "NULL"
              description = line[393:][:-1] #14
              # check if description is made of blanks
              if description.strip() == "":
                description = "NULL"

              query = "%s (PTS,REC_TYPE,COMPANY_NAME,CIK,STATUS,INDUSTRY_ID,SP_RATING,FOUNDING_DATE,ADDR_LINE_1," % s_company_base_query
              query += "ADDR_LINE_2,POSTAL_CODE,CITY,STATE_PROVINCE,COUNTRY,CEO_NAME,DESCRIPTION) "
              query += "VALUES ('%s','%s'," % (pts, rec_type)

              query = "%s (PTS,REC_TYPE,COMPANY_NAME,CIK,STATUS,INDUSTRY_ID,SP_RATING,FOUNDING_DATE,ADDR_LINE_1," % s_company_base_query
              query += "ADDR_LINE_2,POSTAL_CODE,CITY,STATE_PROVINCE,COUNTRY,CEO_NAME,DESCRIPTION) "
              query += "VALUES ('%s','%s'," % (pts, rec_type)
              
              # now we add all the values which are not "NULL"
              if company_name != "NULL":
                query += "'%s'," % company_name
              else:
                query += "NULL,"
              query += "'%s'," % cik
              query += "'%s'," % status
              query += "'%s'," % industry_id
              if sp_rating != "NULL":
                query += "'%s'," % sp_rating
              else:
                query += "NULL,"
              if founding_date != "NULL":
                query += "'%s'," % founding_date
              else:
                query += "NULL,"
              if addr_line_1 != "NULL":
                query += "'%s'," % addr_line_1
              else:
                query += "NULL,"
              if addr_line_2 != "NULL":
                query += "'%s'," % addr_line_2
              else:
                query += "NULL,"
              if postal_code != "NULL":
                query += "'%s'," % postal_code
              else:
                query += "NULL,"
              if city != "NULL":
                query += "'%s'," % city
              else:
                query += "NULL,"
              if state_province != "NULL":
                query += "'%s'," % state_province
              else:
                query += "NULL,"
              if country != "NULL":
                query += "'%s'," % country
              else:
                query += "NULL,"
              if ceo_name != "NULL":
                query += "'%s'," % ceo_name
              else:
                query += "NULL,"
              if description != "NULL":
                query += "'%s'" % description
              else:
                query += "NULL"
              query += ")"

              s_company_values.append(query)

              if len(s_company_values)>=max_packet:
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
                # Create query to load text data into tradeType table
                with oracledb.connect(
                  user=self.oracle_user, password=self.oracle_pwd, 
                  dsn=self.oracle_host+'/'+self.oracle_db) as connection:
                  with connection.cursor() as cursor:
                    for query in s_financial_values:
                      cursor.execute(query)
                  connection.commit()
                s_financial_values = []

        # after reading each line, save all the records that are left in the arrays (<150)

        with oracledb.connect(
          user=self.oracle_user, password=self.oracle_pwd, 
          dsn=self.oracle_host+'/'+self.oracle_db) as connection:
          with connection.cursor() as cursor:
            for query in s_company_values:
              cursor.execute(query)
          connection.commit()
        s_company_values = []

        with oracledb.connect(
          user=self.oracle_user, password=self.oracle_pwd, 
          dsn=self.oracle_host+'/'+self.oracle_db) as connection:
          with connection.cursor() as cursor:
            for query in s_security_values:
              cursor.execute(query)
          connection.commit()
        s_security_values = []

        with oracledb.connect(
          user=self.oracle_user, password=self.oracle_pwd, 
          dsn=self.oracle_host+'/'+self.oracle_db) as connection:
          with connection.cursor() as cursor:
            for query in s_financial_values:
              cursor.execute(query)
          connection.commit()
        s_financial_values = []
    print('Done.')
          
  def load_target_dim_company(self):
    """
    Create Dim Company table in the staging database and then load rows by joining staging_company, staging_industry, and staging StatusType
    """
    print('Loading DimCompany table...')

    load_dim_company_query = """
    INSERT INTO DimCompany (CompanyID, Status,Name,Industry,SPrating,isLowGrade,CEO,AddressLine1,AddressLine2,PostalCode,City,StateProv,Country,Description,FoundingDate,IsCurrent,BatchID,EffectiveDate,EndDate)
    SELECT C.CIK, S.ST_NAME, C.COMPANY_NAME, I.IN_NAME,C.SP_RATING, 
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
    WHERE FOUNDING_DATE IS NOT NULL
    """
    create_sdc_dimcompany_query = """
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
      PRIMARY KEY ("SK_COMPANYID"))
    """
    alter_sdc_dimcompany_query = """
    ALTER TABLE sdc_dimcompany
        ADD RN DECIMAL
    """
    fill_sdc_dimcompany_query = """
    INSERT INTO sdc_dimcompany
    SELECT DC.*, ROW_NUMBER() OVER(ORDER BY CompanyID, EffectiveDate) RN
    FROM DimCompany DC
    """
    update_sdc_dimcompany_query = """
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
            WHERE s1.SK_CompanyID = DimCompany.SK_CompanyID)
    """
    drop_sdc_dimcompany_query = """
    DROP TABLE sdc_dimcompany
    """
    with oracledb.connect(
      user=self.oracle_user, password=self.oracle_pwd, 
      dsn=self.oracle_host+'/'+self.oracle_db) as connection:
      with connection.cursor() as cursor:
        cursor.execute(load_dim_company_query)
        cursor.execute(create_sdc_dimcompany_query)
        cursor.execute(alter_sdc_dimcompany_query)
        cursor.execute(fill_sdc_dimcompany_query)
        cursor.execute(update_sdc_dimcompany_query)
        cursor.execute(drop_sdc_dimcompany_query)
      connection.commit()
    print('Done.')
  
  def load_target_dim_security(self):
    """
    Create Security table in the staging database and then load rows by joining staging_security, status_type and dim_company
    """
    print('Loading DimSecurity...')
    load_dim_security_query_1 = """
    INSERT INTO DimSecurity (Symbol,Issue,Status,Name,ExchangeID,SK_CompanyID,SharesOutstanding,FirstTrade,FirstTradeOnExchange,Dividend,IsCurrent,BatchID,EffectiveDate,EndDate)
    SELECT SS.SYMBOL,SS.ISSUE_TYPE, ST.ST_NAME, SS.NAME, SS.EX_ID, DC.SK_CompanyID, SS.SH_OUT, TO_DATE(SS.FIRST_TRADE_DATE,'YYYY-MM-DD'),
          TO_DATE(FIRST_TRADE_EXCHANGE, 'YYYY-MM-DD'), SS.DIVIDEN, 'true', 1, TO_DATE(LPAD(SS.PTS,8),'YYYY-MM-DD'), TO_DATE('99991231','YYYY-MM-DD')
    FROM S_Security SS
    JOIN StatusType ST ON SS.STATUS = ST.ST_ID
    JOIN DimCompany DC ON DC.SK_CompanyID = CAST(SS.COMPANY_NAME_OR_CIK AS INTEGER)
                        AND DC.EffectiveDate <= TO_DATE(LPAD(SS.PTS,8),'YYYY-MM-DD')
                        AND TO_DATE(LPAD(SS.PTS,8),'YYYY-MM-DD') < DC.EndDate
                        AND LPAD(SS.COMPANY_NAME_OR_CIK,1)='0'
    """

    load_dim_security_query_2 = """                    
    INSERT INTO DimSecurity (Symbol,Issue,Status,Name,ExchangeID,SK_CompanyID,SharesOutstanding,FirstTrade,FirstTradeOnExchange,Dividend,IsCurrent,BatchID,EffectiveDate,EndDate)
    SELECT SS.SYMBOL,SS.ISSUE_TYPE, ST.ST_NAME, SS.NAME, SS.EX_ID, DC.SK_CompanyID, SS.SH_OUT, TO_DATE(SS.FIRST_TRADE_DATE,'YYYY-MM-DD'),
          TO_DATE(FIRST_TRADE_EXCHANGE, 'YYYY-MM-DD'), SS.DIVIDEN, 'true', 1, TO_DATE(LPAD(SS.PTS,8),'YYYY-MM-DD'), TO_DATE('99991231','YYYY-MM-DD')
    FROM S_Security SS
    JOIN StatusType ST ON SS.STATUS = ST.ST_ID
    JOIN DimCompany DC ON RTRIM(SS.COMPANY_NAME_OR_CIK) = DC.Name
                        AND DC.EffectiveDate <= TO_DATE(LPAD(SS.PTS,8),'YYYY-MM-DD')
                        AND TO_DATE(LPAD(SS.PTS,8),'YYYY-MM-DD') < DC.EndDate
                        AND LPAD(SS.COMPANY_NAME_OR_CIK,1) <> '0'
    """
    create_sdc_dimsecurity_query = """                        
    CREATE TABLE sdc_dimsecurity
      (	SK_SECURITYID NUMBER(11,0), 
      SYMBOL CHAR(15) NOT NULL, 
      ISSUE CHAR(6) NOT NULL, 
      STATUS CHAR(10) NOT NULL, 
      NAME CHAR(70) NOT NULL, 
      EXCHANGEID CHAR(6) NOT NULL, 
      SK_COMPANYID NUMBER(11,0) NOT NULL, 
      SHARESOUTSTANDING NUMBER(12,0) NOT NULL, 
      FIRSTTRADE DATE NOT NULL, 
      FIRSTTRADEONEXCHANGE DATE NOT NULL, 
      DIVIDEND NUMBER(10,2) NOT NULL, 
      ISCURRENT CHAR(5) NOT NULL, 
      BATCHID NUMBER(5,0) NOT NULL, 
      EFFECTIVEDATE DATE NOT NULL, 
      ENDDATE DATE NOT NULL, 
      CHECK (IsCurrent = 'false' or IsCurrent = 'true') ENABLE, 
      PRIMARY KEY ("SK_SECURITYID"))
    """
    alter_sdc_dimsecurity_query = """        
    ALTER TABLE sdc_dimsecurity
      ADD RN DECIMAL
    """
    fill_sdc_dimsecurity_query = """
    INSERT INTO sdc_dimsecurity
    SELECT DS.*, ROW_NUMBER() OVER(ORDER BY Symbol, EffectiveDate) RN
    FROM DimSecurity DS
    """
    update_sdc_dimsecurity_query = """
    UPDATE DimSecurity
    SET DimSecurity.EndDate = 
        (SELECT EndDate FROM (
            SELECT s1.SK_SecurityID, s2.EffectiveDate EndDate
            FROM sdc_dimsecurity s1
            JOIN sdc_dimsecurity s2 ON (s1.RN = (s2.RN - 1) AND s1.Symbol = s2.Symbol)
            WHERE s1.SK_SecurityID = DimSecurity.SK_SecurityID)),
        DimSecurity.IsCurrent = 
        (SELECT 'false' FROM (
            SELECT s1.SK_SecurityID, s2.EffectiveDate EndDate
            FROM sdc_dimsecurity s1
            JOIN sdc_dimsecurity s2 ON (s1.RN = (s2.RN - 1) AND s1.Symbol = s2.Symbol)
            WHERE s1.SK_SecurityID = DimSecurity.SK_SecurityID))
        WHERE EXISTS (SELECT * FROM (
            SELECT s1.SK_SecurityID, s2.EffectiveDate EndDate
            FROM sdc_dimsecurity s1
            JOIN sdc_dimsecurity s2 ON (s1.RN = (s2.RN - 1) AND s1.Symbol = s2.Symbol)
            WHERE s1.SK_SecurityID = DimSecurity.SK_SecurityID))
    """
    drop_sdc_dimsecurity_query = """            
    DROP TABLE sdc_dimsecurity
    """
    with oracledb.connect(
      user=self.oracle_user, password=self.oracle_pwd, 
      dsn=self.oracle_host+'/'+self.oracle_db) as connection:
      with connection.cursor() as cursor:
        cursor.execute(load_dim_security_query_1)
        cursor.execute(load_dim_security_query_2)
        cursor.execute(create_sdc_dimsecurity_query)
        cursor.execute(alter_sdc_dimsecurity_query)
        cursor.execute(fill_sdc_dimsecurity_query)
        cursor.execute(update_sdc_dimsecurity_query)
        cursor.execute(drop_sdc_dimsecurity_query)
      connection.commit()
    print('Done.')

  def load_target_financial(self):
    """
    Create Financial table in the staging database and then load rows by ..
    """
    # Create query to load text data into financial table
    financial_load_query="""
    INSERT INTO Financial
      SELECT SK_CompanyID, SF.YEAR, SF.QUARTER, SF.QTR_START_DATE, SF.REVENUE,  SF.EARNINGS, SF.EPS, SF.DILUTED_EPS,SF.MARGIN, SF.INVENTORY, SF.ASSETS, SF.LIABILITIES, SF.SH_OUT, SF.DILUTED_SH_OUT
      FROM S_Financial SF
      JOIN DimCompany DC ON DC.SK_CompanyID = cast(SF.CO_NAME_OR_CIK as INT)
                          AND DC.EffectiveDate <= TO_DATE(SUBSTR(SF.PTS, 1,8),'YYYY-MM-DD')
                          AND TO_DATE(SUBSTR(SF.PTS, 1,8),'YYYY-MM-DD') < DC.EndDate
                          AND SUBSTR(CO_NAME_OR_CIK, 1,1)='0'
    """.format(self.batch_number)
    print(financial_load_query)
    financial_load_query2="""
    INSERT INTO Financial
      SELECT SK_CompanyID, SF.YEAR, SF.QUARTER, SF.QTR_START_DATE, SF.REVENUE,  SF.EARNINGS, SF.EPS, SF.DILUTED_EPS,SF.MARGIN, SF.INVENTORY, SF.ASSETS, SF.LIABILITIES, SF.SH_OUT, SF.DILUTED_SH_OUT
      FROM S_Financial SF
      JOIN DimCompany DC ON RTRIM(SF.CO_NAME_OR_CIK) = DC.Name
                          AND DC.EffectiveDate <= TO_DATE(SUBSTR(SF.PTS, 1,8),'YYYY-MM-DD')
                          AND TO_DATE(SUBSTR(SF.PTS, 1,8),'YYYY-MM-DD') < DC.EndDate
                          AND SUBSTR(CO_NAME_OR_CIK, 1,1) <> '0'
    """.format(self.batch_number)
    print(financial_load_query2)

    with oracledb.connect(
            user=self.oracle_user, password=self.oracle_pwd,
            dsn=self.oracle_host + '/' + self.oracle_db) as connection:
      with connection.cursor() as cursor:
        cursor.execute(financial_load_query)
        cursor.execute(financial_load_query2)
      connection.commit()

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