#-*- coding: utf-8 -*-

INTERVAL = 2

## for Prgram
ALERT_TYPE_DICT = {
    'status': {'status', 'status_all', 'conn', 'link'},
    'range': {
        'load', 'util', 'rx_bps', 'rx_drop_pps', 'rx_err_pps', 
        'rx_pps', 'rx_util', 'rx_util_rate', 'tx_bps', 'tx_drop_pps', 
        'tx_err_pps', 'tx_pps', 'tx_util', 'tx_util_rate', 'degree'
    }
}

## DB
SELECT_FROM_PRX_ITEM = """
        select * from fivegmon.fgm_pxy_item where step4 = 'cms.svr.proc.status.ps-ef'
    """
SQL_UPD_PXY = """
update fivegmon.fgm_pxy_item
set mod_dttm=%s, mon_val=%s, mon_val_str=%s, c_result=%s, c_error=%s, item_chk=0, a_result=null, p_result=null
where region=%s and hostname=%s and step1=%s and step2=%s and step3=%s and step4=%s and obj=%s and mod_dttm < %s ;
"""
SQL_INS_PXY = """
insert into fivegmon.fgm_pxy_item(mod_dttm, region, hostname, step1, step2, step3, step4, obj, mon_val, mon_val_str, alert_type, c_result, c_error)
select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
from ( select count(mod_dttm) as mc from fivegmon.fgm_pxy_item
  where region=%s and hostname=%s and step1=%s and step2=%s and step3=%s and step4=%s and obj=%s) as chk
where chk.mc=0;
"""
SQL_UPD_PXYSW = """
update fivegmon.fgm_pxy_switch_item
set mod_dttm=%s, mon_val=%s, mon_val_str=%s, c_result=%s, c_error=%s, item_chk=0, a_result=null, p_result=null
where region=%s and switchname=%s and step1=%s and step2=%s and step3=%s and step4=%s and obj=%s and mod_dttm < %s ;
"""
SQL_INS_PXYSW = """
insert into fivegmon.fgm_pxy_switch_item(mod_dttm, region, switchname, step1, step2, step3, step4, obj, mon_val, mon_val_str, alert_type, c_result, c_error)
select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
from ( select count(mod_dttm) as mc from fivegmon.fgm_pxy_switch_item
  where region=%s and switchname=%s and step1=%s and step2=%s and step3=%s and step4=%s and obj=%s) as chk
where chk.mc=0;
"""



