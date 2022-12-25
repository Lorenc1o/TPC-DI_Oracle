declare
   c int;
begin
    select count(*) into c from user_tables where table_name = upper('DimAccount');
    if c = 1 then
        execute immediate 'drop table DimAccount';
    end if;
    select count(*) into c from user_tables where table_name = upper('DimBroker');
    if c = 1 then
        execute immediate 'drop table DimBroker';
    end if;
    select count(*) into c from user_tables where table_name = upper('DimCompany');
    if c = 1 then
        execute immediate 'drop table DimCompany';
    end if;
    select count(*) into c from user_tables where table_name = upper('DimCustomer');
    if c = 1 then
        execute immediate 'drop table DimCustomer';
    end if;
    select count(*) into c from user_tables where table_name = upper('DimDate');
    if c = 1 then
        execute immediate 'drop table DimDate';
    end if;
    select count(*) into c from user_tables where table_name = upper('DimTime');
    if c = 1 then
        execute immediate 'drop table DimTime';
    end if;
    select count(*) into c from user_tables where table_name = upper('DimTrade');
    if c = 1 then
        execute immediate 'drop table DimTrade';
    end if;
    select count(*) into c from user_tables where table_name = upper('DImessages');
    if c = 1 then
        execute immediate 'drop table DImessages';
    end if;
    select count(*) into c from user_tables where table_name = upper('FactCashBalances');
    if c = 1 then
        execute immediate 'drop table FactCashBalances';
    end if;
    select count(*) into c from user_tables where table_name = upper('FactHoldings');
    if c = 1 then
        execute immediate 'drop table FactHoldings';
    end if;
    select count(*) into c from user_tables where table_name = upper('FactMarketHistory');
    if c = 1 then
        execute immediate 'drop table FactMarketHistory';
    end if;
    select count(*) into c from user_tables where table_name = upper('FactWatches');
    if c = 1 then
        execute immediate 'drop table FactWatches';
    end if;
    select count(*) into c from user_tables where table_name = upper('Industry');
    if c = 1 then
        execute immediate 'drop table Industry';
    end if;
    select count(*) into c from user_tables where table_name = upper('Financial');
    if c = 1 then
        execute immediate 'drop table Financial';
    end if;
    select count(*) into c from user_tables where table_name = upper('Prospect');
    if c = 1 then
        execute immediate 'drop table Prospect';
    end if;
    select count(*) into c from user_tables where table_name = upper('StatusType');
    if c = 1 then
        execute immediate 'drop table StatusType';
    end if;
    select count(*) into c from user_tables where table_name = upper('TaxRate');
    if c = 1 then
        execute immediate 'drop table TaxRate';
    end if;
    select count(*) into c from user_tables where table_name = upper('TradeType');
    if c = 1 then
        execute immediate 'drop table TradeType';
    end if;
    select count(*) into c from user_tables where table_name = upper('Audit_');
    if c = 1 then
        execute immediate 'drop table Audit_';
    end if;
end;
exit