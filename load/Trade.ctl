LOAD DATA 
INTO TABLE S_Trade 
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' TRAILING NULLCOLS
(
    t_id,
    t_dts DATE "YYYY-MM-DD HH24:MI:SS",
    t_st_id,
    t_tt_id,
    t_is_cash,
    t_s_symb,
    t_qty,
    t_bid_price,
    t_ca_id,
    t_exec_name,
    t_trade_price,
    t_chrg,
    t_comm,
    t_tax
)