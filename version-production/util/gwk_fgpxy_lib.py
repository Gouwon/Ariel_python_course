#-*- coding: utf-8 -*-

## default LIB
import threading, os, csv, platform, subprocess, json
from datetime import datetime
from time import sleep

## open LIB
import ruamel.yaml, pymysql

## custom LIB


## parse key
PKEY_SVR = "cmn.svr"
PKEY_AMF_CISCO = "cisco.amf"
PKEY_CISCO_PRE = 'cisco.'
PKEY_SAMSUNG_PRE = 'samsung.'
PKEY_ELUON_PRE = 'eluon.'
PKEY_ARIEL_PRE = 'ariel.'
PKEY_CMS_SVR = 'cms.svr'
PKEY_VM = 'cmn.vm'
PKEY_5SVR = '5gim.svr'
PKEY_5VM = '5gim.vm'

PKEY_PCF_SNET = "s-net.pcf"
PKEY_PCF_ORACLE = "oracle.pcf"
PKEY_SMF_SAMSUNG = "samsung.smf"
PKEY_UPF_SAMSUNG = "samsung.upf"
PKEY_AMF_SAMSUNG = "samsung.amf"
PKEY_SWITCH_CISCO = 'cisco.switch'
PKEY_STORAGE_EMC = 'emc.storage'

## alarm
# common
PKEY_CONN_SSH = "conn.status.ssh"
PKEY_PORT_LINK_ALL_CARR = "port.link_all.carrier"
PKEY_PORT_STATUS_ALL_OPER = "port.status_all.operstate"
PKEY_PROC_STATUS_PS = 'proc.status.ps-ef'
PKEY_PROC_OPENSTACK_STATUS = 'proc.openstack.show'
PKEY_PROC_LIBVIRTD_STATUS = 'proc.libvirtd.show'
PKEY_STORAGE_STATUS = 'storage.status.show'
PKEY_STORAGE_LINK = 'storage.link.show'
PKEY_AUTH_STATUS = 'auth.status.show'

# server
PKEY_FAN_IPMI = "fan.status.ipmi"
PKEY_FAN_ALL_IPMI = "fan.status_all.ipmi"
PKEY_PSU_STATUS_DMI = "psu.status.dmidecode"
PKEY_VM_STATUS_ALL_VIR = "vm.status_all.virsh"

# cisco amf
PKEY_VCS_VNF_STATUS = "vnf.status.show"
PKEY_VCS_PROC_STATUS = "proc.status.show"
PKEY_VCS_PORT_STATUS = "port.status.show"
PKEY_VCS_DIPORT_STATUS = "diport.status.show"

# s-net/oracle pcf
PKEY_VOR_PROC_STATUS = "proc.status.ps-ef"

# samsung vnf
PKEY_VSS_PROC_STATUS = "proc.status.blk"
PKEY_VSS_VNF_STATUS = "vnf.status.node"
PKEY_VSS_PORT_STATUS = "port.status.port"

## perf
# common
PKEY_CPU_USE_TOP = "cpu.use.top"
PKEY_MEM_USE_PROC = "mem.use.proc"
PKEY_FS_USE_DF = "fs.use.df"
PKEY_PORT_BYTE_STAT = "port.byte.stat"
PKEY_PORT_PACKET_STAT = "port.packet.stat"
PKEY_PORT_ERROR_STAT = "port.error.stat"
PKEY_PORT_DROP_STAT = "port.drop.stat"
PKEY_TEMP_DEGREE_HWMON = "temp.degree.hwmon"
PKEY_TEMP_DEGREE_IPMI = "temp.degree.ipmi"

# server


# cisco amf
PKEY_VCS_CPU_USE_SHOW = "cpu.use.show"
PKEY_VCS_PORT_BPS_SHOW = "port.bps.show"
PKEY_VCS_PORT_PPS_SHOW = "port.pps.show"
PKEY_VCS_DIPORT_BPS_SHOW = "diport.bps.show"
PKEY_VCS_DIPORT_PPS_SHOW = "diport.pps.show"



#cli: xxx.show
#...: ... xxx
# status.show -> status,state
# use.show -> value,level

#   CISCO MGMT SW
# <     PKEY_VCS_PORT_STATUS
PKEY_VCS_PORT_LINK_SHOW = 'port.link.show'
PKEY_VCS_FAN_STATUS = 'fan.status.show'
PKEY_VCS_POWER_STATUS = 'power.status.show'
# <     PKEY_VCS_CPU_USE_SHOW
PKEY_VCS_MEM_USE_SHOW = 'mem.use.show'
PKEY_VCS_TEMP_STATUS = 'temp.status.show'
# <     PKEY_VCS_PORT_BPS_SHOW
# <     PKEY_VCS_PORT_PPS_SHOW
PKEY_VCS_PORT_ERROR_SHOW = 'port.error.show'
PKEY_VCS_PORT_CRC_SHOW = 'port.crc.show'
#< PKEY_AUTH_STATUS
#   EMC ST
PKEY_VEMC_ST_STATUS = 'st.status.show'
PKEY_VEMC_PORT_STATUS = 'port.status.show'
PKEY_VEMC_PORT_LINK_SHOW = 'port.link.show'
PKEY_VEMC_FAN_STATUS = 'fan.status.show'
PKEY_VEMC_POWER_STATUS = 'power.status.show'
PKEY_VEMC_PROC_STATUS = 'proc.status.show'
# < PKEY_CPU_USE_TOP
PKEY_VEMC_MEM_STATUS = 'mem.status.show'
PKEY_VEMC_MEM_UTIL_SHOW = 'mem.util.show'
PKEY_VEMC_DISK_USE_SHOW = 'disk.use.show'
PKEY_VEMC_TEMP_DEGREE_SHOW = 'temp.degree.show'
# < PKEY_PORT_BYTE_STAT
# < PKEY_PORT_PACKET_STAT
# < PKEY_PORT_ERROR_STAT
# < PKEY_PORT_DROP_STAT
# < PKEY_AUTH_STATUS
#   ELUON UDM/VTAS
PKEY_VEL_VNF_STATUS = "vnf.status.show"
PKEY_VEL_PROC_STATUS = "proc.status.show"
PKEY_VEL_PORT_STATUS = "port.status.show"
PKEY_VEL_CPU_UTIL_SHOW = 'cpu.util.show'
PKEY_VEL_MEM_UTIL_SHOW = 'mem.util.show'
PKEY_VEL_FS_UTIL_SHOW = 'fs.util.show'
# < PKEY_AUTH_STATUS
#   SAMSUNG CU
PKEY_VSS_NE_LIST_SHOW = 'ne.list.show'
PKEY_VSS_NE_CONN_SHOW = 'ne.conn.show'
#PKEY_VSS_VNF_STATUS = 'vnf.status.show'
PKEY_VSS_CPU_LOAD_SHOW = 'cpu.load.show'
PKEY_VSS_MEM_UTIL_SHOW = 'mem.util.show'
PKEY_VSS_FS_UTIL_SHOW = 'fs.util.show'
# < PKEY_AUTH_STATUS
#   ARIEL PUBS
PKEY_VAR_VNF_STATUS = "vnf.status.show"
PKEY_VAR_PROC_STATUS = "proc.status.show"
PKEY_VAR_PORT_STATUS = "port.status.show"
PKEY_VAR_CPU_LOAD_SHOW = 'cpu.load.show'
PKEY_VAR_MEM_UTIL_SHOW = 'mem.util.show'
#PKEY_VAR_FS_UTIL_SHOW = 'fs.util.show'
PKEY_VAR_PORT_BPS_SHOW = 'port.bps.show'
PKEY_VAR_PORT_PPS_SHOW = 'port.pps.show'
# < PKEY_AUTH_STATUS
#   CMN VM
# < PKEY_VM_STATUS_ALL_VIR
# < PKEY_PORT_STATUS_ALL_OPER
# < PKEY_PSU_STATUS_DMI
# < PKEY_FAN_ALL_IPMI
# < PKEY_CPU_USE_TOP
# < PKEY_MEM_USE_PROC
# < PKEY_FS_USE_DF
# < PKEY_PORT_BYTE_STAT
# < PKEY_PORT_PACKET_STAT
# < PKEY_PORT_ERROR_STAT
# < PKEY_PORT_DROP_STAT
# < PKEY_TEMP_DEGREE_HWMON

# s-net/oracle pcf

# samsung vnf
PKEY_VSS_CPU_USE = "cpu.use.load"
PKEY_VSS_FS_USE = "fs.use.dku"
PKEY_VSS_EXT_PORT_INFO = "ext-port.info.pport"
PKEY_VSS_DPDK_INT_PORT_INFO = "dpdk-int-port.info.iport"
PKEY_VSS_IPC_INT_PORT_INFO = "ipc-int-port.info.ipc"

## step
ST1_SVR = "server"
ST1_SW = "switch"
ST1_VM = "vm"
ST1_ST = 'storage'

ST2_PORT = "port"
ST2_PSU = "psu"
ST2_FAN = "fan"
ST2_VM = "vm"
ST2_VDU = "vdu"
ST2_NET = "net"
ST2_CPU = "cpu"
ST2_MEM = "mem"
ST2_FS = "fs"
ST2_TEMP = "temp"
ST2_PROC = "proc"
ST2_SW = 'sw'
ST2_ST = 'storage'
ST2_NE = 'ne'
ST2_AUTH = 'auth'

ST3_STATUS = "status"
ST3_GRP_STATUS = "grp_status"
ST3_STATUS_ALL = "status_all"
ST3_LINK = "link"
ST3_CONN = "conn"
ST3_UTIL = "util"
ST3_LOAD = "load"
ST3_RX_BPS = "rx_bps"
ST3_TX_BPS = "tx_bps"
ST3_RX_PPS = "rx_pps"
ST3_TX_PPS = "tx_pps"
ST3_RX_UTIL = "rx_util"
ST3_TX_UTIL = "tx_util"
ST3_RX_UTIL_RATE = "rx_util_rate"
ST3_TX_UTIL_RATE = "tx_util_rate"
ST3_RX_ERR_GAP = "rx_err_gap"
ST3_TX_ERR_GAP = "tx_err_gap"
ST3_RX_DROP_GAP = "rx_drop_gap"
ST3_TX_DROP_GAP = "tx_drop_gap"
ST3_RX_ERR = "rx_err"
ST3_TX_ERR = "tx_err"
ST3_RX_ERR_PPS = "rx_err_pps"
ST3_TX_ERR_PPS = "tx_err_pps"
ST3_RX_DROP = "rx_drop"
ST3_TX_DROP = "tx_drop"
ST3_RX_DROP_PPS = "rx_drop_pps"
ST3_TX_DROP_PPS = "tx_drop_pps"
ST3_RX_DROP_PPS_NORM = "rx_drop_pps_norm"
ST3_TX_DROP_PPS_NORM = "tx_drop_pps_norm"
ST3_DEGREE = "degree"
ST3_CRC = 'crc'

## type
ATYPE_STATUS = "status"
ATYPE_RANGE = "range"
ATYPE_UNKNOWN = "unknown"

## RI_VAL
RI_VAL_PROC_RUNNING = "on"
RI_VAL_PROC_STARTING = "start"
RI_VAL_VNF_RUNNING = "active"
RI_VAL_PORT_STS_UP = "up"
RI_VAL_AUTH_GOOD = 'good'
RI_VAL_AUTH_BAD = 'bad'

## coll res
RES_SC = 1
RES_FA = 0

## coll err
ERR_NO_VAL = "No Value"
ERR_INV_VAL = "Invalid Value"
ERR_NO_PRV_VAL = "No Prev Value"
ERR_INV_PRV_VAL = "Invalid Prev Value"
ERR_NO_ORG_VAL = "No Origin Value"
ERR_INV_ORG_VAL = "Invalid Origin Value"
ERR_MNS_VAL = "Minus Value"


## DB
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
## CC:: 서브쿼리의 조건에 해당하는 레코드가 있을 경우에만, 넘겨 준 값들로 테이블에 Insert 하겠다는 의미. 조건에 부합하지 않으면 Insert되지 않음.

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



class _PrintLogger(object):
    def info(self, _txt):
        print(str(_txt))
    def error(self, _err):
        print(str(_err))
    def debug(self, _txt):
        print(str(_txt))
    def exception(self, _e):
        print(str(_e))


class RawLog(object):

    def __init__(self, _cdttm, _rg, _hname, _key, _cmd, _res, _body):
        self.cdttm = _cdttm
        self.rg = _rg
        self.hname = _hname
        self.key = _key
        self.cmd = _cmd
        self.res = _res
        self.body = _body

    def to_list(self):
        return [self.cdttm, self.rg, self.hname, self.key, self.body, self.cmd, self.res]


class DBParam(object):

    def __init__(self, mod_dttm, region, hostname, step1, step2, step3, step4, obj, mon_val, mon_val_str, alert_type, _c_result, _c_error=None):
        self.mod = mod_dttm
        self.rg = region
        self.nm = hostname
        self.st1 = step1
        self.st2 = step2
        self.st3 = step3
        self.st4 = step4
        self.obj = obj
        self.val = mon_val
        self.val_str = mon_val_str
        self.atype = alert_type
        self.result = _c_result
        self.error = _c_error


class DBWriter(threading.Thread):
    def __init__(self, _host, _port, _id, _pw, _thrs, _params, _ltt, _logger=None, _type='pxy'):
        threading.Thread.__init__(self)

        self.thrs = _thrs
        self.params = _params
        self._runtime = None
        self.ltt = _ltt
        self.logger = _PrintLogger() if _logger == None else _logger
        self.dbc = pymysql.connect(host=_host, port=_port, user=_id, passwd=_pw,
                                   autocommit=True, connect_timeout=10, use_unicode=True, charset="utf8")
        self.cur = self.dbc.cursor()
        self._type = _type

    def return_runtime(self):
        return self._runtime

    def reconn(self):
        self.dbc.ping(reconnect=True)
        self.cur = self.dbc.cursor()

    def run(self):
        _tprv = datetime.now()
        _cnt = 0
        while True:
            try:
                _p = self.params.pop(0)
                _cnt = _cnt + 1
                try:
                    _res = True
                    try:
                        ## mod_dttm=%s, mon_val=%s, mon_val_str=%s, c_result=%s, c_error=%s, item_chk=0
                        ## where region=%s and hostname=%s and step1=%s and step2=%s and step3=%s and step4=%s and obj=%s and mod_dttm < %s ;
                        
                        ## CC:: 수집된 데이터는 switch 여부를 판단함. --> DBWriter __init__()할 때, _type을 별도로 주어야 함.
                        if self._type == 'pxy':
                            sql = SQL_UPD_PXY
                        else:
                            sql = SQL_UPD_PXYSW
                        self.cur.execute(sql, (_p.mod, _p.val, _p.val_str, _p.result, _p.error, _p.rg, _p.nm, _p.st1, _p.st2, _p.st3, _p.st4, _p.obj, _p.mod))
                        _rcnt = self.cur.rowcount
                        _rid = self.cur.lastrowid
                    except Exception as e:
                        _res = False
                        _rcnt = str(e)
                        self.logger.exception(e)

#                     self.logger.debug("up: %s, %s"%( str(_rcnt), str(_rid) ))
                    if not _res:
                        # self.logger.error("FAIL: %s Info DB-Update Error, err=%s, sql=%s, param=%s"%( self.ltt, str(_rcnt), str(self.upd_sql), str(_p) ))
                        pass
                    else:
                        if _rcnt < 1 :
                            _res = True
                            try:
                                ## (mod_dttm, region, hostname, step1, step2, step3, step4, obj,
                                ## mon_val, mon_val_str, alert_type, c_result, c_error)
                                ## select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                ## where region=%s and hostname=%s and step1=%s and step2=%s and step3=%s and step4=%s and obj=%s
                                if self._type == 'pxy':
                                    sql = SQL_INS_PXY
                                else:
                                    sql = SQL_INS_PXYSW
                                self.cur.execute(sql, (_p.mod, _p.rg, _p.nm, _p.st1, _p.st2, _p.st3, _p.st4, _p.obj,
                                                               _p.val, _p.val_str, _p.atype, _p.result, _p.error,
                                                                _p.rg, _p.nm, _p.st1, _p.st2, _p.st3, _p.st4, _p.obj))
                                _rcnt = self.cur.rowcount
                                _rid = self.cur.lastrowid
                            except Exception as e:
                                _res = False
                                _rcnt = str(e)
                                self.logger.exception(e)
#                             self.logger.debug("in: %s, %s"%( str(_rcnt), str(_rid) ))
                            # if not _res:
                            #     self.logger.error("FAIL: %s Info DB-Insert Error, err=%s, sql=%s, param=%s"%( self.ltt, str(_rcnt), str(self.ins_sql), str(_p) ))
                            # else: self.logger.info("SUCC: New %s Item Inserted, rcnt=%s, rid=%s"%( self.ltt, str(_rcnt), str(_rid) ))
                except Exception as e:
                    # self.logger.exception(e)
                    # self.logger.error("FAIL: %s Info DB-Execute Error, data=%s, exc=%s"%( self.ltt, str(_p), str(e) ))
                    print(e)
            except Exception as e:
                sleep(0.1)

                _thr_end = True
                for _tt in self.thrs:
                    if not _tt.is_done():
                        _thr_end = False
                        break

                if _thr_end :
                    if len(self.params) > 0 :
                        continue
                    else:
                        break

        try:
            if self.dbc is not None : self.dbc.close()
        except Exception as e:
            self.logger.exception(e)
            self.logger.error("FAIL: %s DBC Close Error, exc=%s"%( self.ltt, str(e) ))
        self._runtime = datetime.now() - _tprv
#         self.logger.debug("DBT End, total data=%s"%str(_cnt))
        return


class RawWriter(threading.Thread):
    def __init__(self, _fpfx, _params, _ltt, _logger=None):
        threading.Thread.__init__(self)

        self._done = False
        self.fpfx = _fpfx
        self.params = _params
        self.ltt = _ltt
        self.logger = _PrintLogger if _logger == None else _logger
        self.finish = False

    def done(self):
        self._done = True

    def run(self):
        # create file per day

        _dir = os.path.dirname(self.fpfx)
        if not os.path.isdir(_dir):
            os.mkdir(_dir)

        _fname = self.fpfx + "." + datetime.now().strftime("%Y%m%d") + ".csv"
        cf = open(_fname, "a")
        cw = csv.writer(cf)
        while True:
            try:
                _p = self.params.pop(0)
                cw.writerow(_p.to_list())
            except Exception as e:
                sleep(0.1)

                if self._done :
                    if len(self.params) > 0 :
                        continue
                    else:
                        break

        try:
            if cf is not None : cf.close()
        except Exception as e:
            self.logger.exception(e)
            self.logger.error("FAIL: %s File Close Error, exc=%s"%( self.ltt, str(e) ))

        self.finish = True
        return


def show_runtime(_total, _thread, _db_t, _show_host, _lnm, _logger):
    _LEN = 50
    _logger.info("#"*_LEN)

    _hcnt = 1 if len(_thread) == 0 else len(_thread)
    _logger.info("# %s TOTAL:  *Time: %s, Avg:%s, Host: %s"%( _lnm, str(_total), str(_total/_hcnt), str(_hcnt) ))

    ## Only SSH
    _htotal = None
    _hhstr = []

    ## by item
    _key_list = []
    _itotal = {}
    _iavg = {}
    _iMin = {}
    _iMax = {}
    _sMax = None
    _sMin = None
    for _thr in _thread :
        _trt = _thr.return_runtime()
        if _htotal == None:
            _htotal = _trt
        else:
            _htotal = _htotal + _trt

        if _sMax == None: _sMax = _trt
        else:
            if _sMax < _trt :
                _sMax = _trt
        if _sMin == None: _sMin = _trt
        else:
            if _sMin > _trt :
                _sMin = _trt

        _hhstr.append(_thr.hname + ": " + str(_trt))

        _itrs = _thr.return_item_delay()
        for _ikey in _itrs:
            if not _ikey in _key_list :
                _key_list.append(_ikey)

            _idelay = _itrs[_ikey]
            if not _ikey in _itotal :
                _itotal[_ikey] = _idelay
            else:
                _itotal[_ikey] = _itotal[_ikey] + _idelay

            if not _ikey in _iMin :
                _iMin[_ikey] = _idelay
            else:
                if _iMin[_ikey] > _idelay :
                    _iMin[_ikey] = _idelay

            if not _ikey in _iMax :
                _iMax[_ikey] = _idelay
            else:
                if _iMax[_ikey] < _idelay :
                    _iMax[_ikey] = _idelay

    for _totkey in _itotal:
        _iavg[_totkey] = _itotal[_totkey]/_hcnt

    _logger.info("# Only SSH: ")
    if _htotal == None : _htotal = 0
    _logger.info("#   - *MAX: %s, MIN:%s, total: %s, avg: %s"%( str(_sMax), str(_sMin), str(_htotal), str(_htotal/_hcnt) ))
    if _show_host:
        for _hh in _hhstr:
            _logger.info("#       %s"%( _hh ))


    _logger.info("# By Item: ")

    _totStr = ""
    _avgStr = ""
    _minStr = ""
    _maxStr = ""
    for _k in _key_list:
        _tstr = str(_itotal[_k])
        _astr = str(_iavg[_k])
        _minstr = str(_iMin[_k])
        _maxstr = str(_iMax[_k])

        _totStr = _totStr + ( _k + ": %14s"%(_tstr) + ", ")
        _avgStr = _avgStr + ( _k + ": %14s"%(_astr) + ", ")
        _minStr = _minStr + ( _k + ": %14s"%(_minstr) + ", ")
        _maxStr = _maxStr + ( _k + ": %14s"%(_maxstr) + ", ")


    _logger.info("#    - *Max: %s"%str( _maxStr ))
    _logger.info("#    - Min: %s"%str( _minStr ))
    _logger.info("#    - avg: %s"%str( _avgStr ))
    _logger.info("#    - total: %s"%str( _totStr ))


    ## only db
    _logger.info("# Only DB: ")
    if type(_db_t) == dict:
        _dbtotal = None
        _db_tlist = []
        for _dt in _db_t:
            _drt = _db_t[_dt]
            _db_tlist.append(_drt)
            if _dbtotal == None:
                _dbtotal = _drt
            else:
                _dbtotal = _dbtotal + _drt

        if _dbtotal == None : _dbtotal = 0
        _logger.info("#    - *Max=%s, Min=%s, total: %s, avg: %s"%( str(max(_db_tlist)), str(min(_db_tlist)), str(_dbtotal), str(_dbtotal/_hcnt) ))
    else:
        _logger.info("#    - *Total: %s"%( str(_db_t) ))

    _logger.info("#"*_LEN)


def exec_cmd(cmd, inputtext=None):
    """run shell command on *unix/windows"""

    if platform.system() == 'Windows':
        proc = subprocess.Popen(cmd,
            stdin=subprocess.PIPE, \
            stdout=subprocess.PIPE, \
            stderr=subprocess.PIPE, \
            shell=True)
    else:
        proc = subprocess.Popen(cmd,
            stdin=subprocess.PIPE, \
            stdout=subprocess.PIPE, \
            stderr=subprocess.PIPE, \
            shell=True, \
            close_fds=True)

    (response, error) = proc.communicate(inputtext)
    if error != '':
        return False, error
    else:
        return True, response.strip()


def loadConfig(_cfgFile, _lnm, _logger):
    # QQQQQQQQQ
    # _logger.info("@@@ Load Config: %s"%( _lnm ))

    with open(_cfgFile, "r") as _cf:
        _ycfg = ruamel.yaml.load(_cf, Loader=ruamel.yaml.RoundTripLoader)
        _cfg = json.loads(json.dumps(_ycfg))
    return _cfg



def get_steps(_rkey, _step3=None, _step2=None):
    #print('key={}'.format(_rkey))
    if _rkey == None or str(_rkey).strip() == "":
        return False, None, "No Raw-Key, key=%s"%( str(_rkey) )

    if str(_rkey).find(PKEY_SVR) > -1 or PKEY_CMS_SVR in _rkey or PKEY_5SVR in _rkey:
        _st1 = ST1_SVR
    elif PKEY_SWITCH_CISCO in _rkey:
        _st1 = ST1_SW
    elif PKEY_CISCO_PRE in _rkey or str(_rkey).find(PKEY_PCF_SNET) > -1 or str(_rkey).find(PKEY_PCF_ORACLE) > -1 \
      or PKEY_SAMSUNG_PRE in _rkey or PKEY_ARIEL_PRE in _rkey or PKEY_ELUON_PRE in _rkey or PKEY_VM in _rkey or PKEY_5VM in _rkey:
        _st1 = ST1_VM
    elif PKEY_STORAGE_EMC in _rkey:
        _st1 = ST1_ST
    else:
        return False, None, "Invalid Raw-Key(step1), key=%s"%( str(_rkey) )

    ## common
    # alarm
    if str(_rkey).find(PKEY_CONN_SSH) > 0 :
        return True, [_st1, ST2_NET, ST3_CONN, _rkey], None
    if str(_rkey).find(PKEY_PORT_STATUS_ALL_OPER) > 0 :
        return True, [_st1, ST2_PORT, ST3_STATUS, _rkey], None
    if str(_rkey).find(PKEY_PORT_LINK_ALL_CARR) > 0 :
        return True, [_st1, ST2_PORT, ST3_LINK, _rkey], None
    if str(_rkey).find(PKEY_PROC_OPENSTACK_STATUS) > 0 :
        return True, [_st1, ST2_PROC, ST3_STATUS, _rkey], None
    if str(_rkey).find(PKEY_PROC_LIBVIRTD_STATUS) > 0 :
        return True, [_st1, ST2_PROC, ST3_STATUS, _rkey], None
    if str(_rkey).find(PKEY_STORAGE_STATUS) > 0 :
        return True, [_st1, ST2_ST, ST3_STATUS, _rkey], None
    if str(_rkey).find(PKEY_STORAGE_LINK) > 0 :
        return True, [_st1, ST2_ST, ST3_LINK, _rkey], None
    if str(_rkey).find(PKEY_TEMP_DEGREE_HWMON) > 0 :
        return True, [_st1, ST2_TEMP, _step3, _rkey], None
    if str(_rkey).find(PKEY_TEMP_DEGREE_IPMI) > 0 :
        return True, [_st1, ST2_TEMP, _step3, _rkey], None
    if PKEY_AUTH_STATUS in _rkey:   
        return True, [_st1, ST2_AUTH, ST3_STATUS, _rkey], None
    if PKEY_PROC_STATUS_PS in _rkey:
        return True, [_st1, ST2_PROC, ST3_STATUS, _rkey], None

    # perf
    if str(_rkey).find(PKEY_CPU_USE_TOP) > 0 :
        return True, [_st1, ST2_CPU, _step3, _rkey], None
    if str(_rkey).find(PKEY_MEM_USE_PROC) > 0 :
        return True, [_st1, ST2_MEM, _step3, _rkey], None
    if str(_rkey).find(PKEY_FS_USE_DF) > 0 :
        return True, [_st1, ST2_FS, _step3, _rkey], None
    if str(_rkey).find(PKEY_PORT_BYTE_STAT) > 0 :
        return True, [_st1, ST2_PORT, _step3, _rkey], None
    if str(_rkey).find(PKEY_PORT_PACKET_STAT) > 0 :
        return True, [_st1, ST2_PORT, _step3, _rkey], None
    if str(_rkey).find(PKEY_PORT_ERROR_STAT) > 0 :
        return True, [_st1, ST2_PORT, _step3, _rkey], None
    if str(_rkey).find(PKEY_PORT_DROP_STAT) > 0 :
        return True, [_st1, ST2_PORT, _step3, _rkey], None


    ## server
    if str(_rkey).find(PKEY_SVR) > -1 or PKEY_CMS_SVR in _rkey or PKEY_5SVR in _rkey:
        # alarm
        if str(_rkey).find(PKEY_FAN_ALL_IPMI) > 0 :
            return True, [_st1, ST2_FAN, ST3_STATUS_ALL, _rkey], None
        if str(_rkey).find(PKEY_FAN_IPMI) > 0 :
            return True, [_st1, ST2_FAN, ST3_STATUS, _rkey], None
        if str(_rkey).find(PKEY_PSU_STATUS_DMI) > 0 :
            return True, [_st1, ST2_PSU, ST3_STATUS, _rkey], None
        if str(_rkey).find(PKEY_VM_STATUS_ALL_VIR) > 0 :
            return True, [_st1, ST2_VM, ST3_STATUS, _rkey], None
        # perf

    #   CS SW
    elif PKEY_SWITCH_CISCO in _rkey:
        if PKEY_VCS_PORT_STATUS in _rkey:
            return True, [_st1, ST2_PORT, ST3_STATUS, _rkey], None
        if PKEY_VCS_PORT_LINK_SHOW in _rkey:
            return True, [_st1, ST2_PORT, ST3_LINK, _rkey], None
        if PKEY_VCS_FAN_STATUS in _rkey:
            return True, [_st1, ST2_FAN, ST3_STATUS, _rkey], None
        if PKEY_VCS_POWER_STATUS in _rkey:
            return True, [_st1, ST2_PSU, ST3_STATUS, _rkey], None
        #PKEY_AUTH_STATUS
        if PKEY_VCS_CPU_USE_SHOW in _rkey:
            return True, [_st1, ST2_CPU, _step3, _rkey], None
        if PKEY_VCS_MEM_USE_SHOW in _rkey:
            return True, [_st1, ST2_MEM, ST3_UTIL, _rkey], None
        if PKEY_VCS_TEMP_STATUS in _rkey:
            return True, [_st1, ST2_TEMP, _step3, _rkey], None
        #PKEY_FS_USE_DF
        if PKEY_VCS_PORT_BPS_SHOW in _rkey:
            return True, [_st1, ST2_PORT, _step3, _rkey], None
        if PKEY_VCS_PORT_PPS_SHOW in _rkey:
            return True, [_st1, ST2_PORT, _step3, _rkey], None
        if PKEY_VCS_PORT_ERROR_SHOW in _rkey:
            return True, [_st1, ST2_PORT, _step3, _rkey], None
        if PKEY_VCS_PORT_CRC_SHOW in _rkey:
            return True, [_st1, ST2_PORT, _step3, _rkey], None

    #   CS AMF/SMF/UPF
    #elif str(_rkey).find(PKEY_AMF_CISCO) > -1 :
    elif PKEY_CISCO_PRE in _rkey:
        # alarm
        if str(_rkey).find(PKEY_VCS_VNF_STATUS) > 0 :
            return True, [_st1, ST2_VDU, ST3_STATUS, _rkey], None
        if str(_rkey).find(PKEY_VCS_PROC_STATUS) > 0 :
            return True, [_st1, ST2_PROC, _step3, _rkey], None
        if str(_rkey).find(PKEY_VCS_PORT_STATUS) > 0 :
            return True, [_st1, ST2_PORT, _step3, _rkey], None
        if str(_rkey).find(PKEY_VCS_DIPORT_STATUS) > 0 :
            return True, [_st1, ST2_PORT, ST3_LINK, _rkey], None
        #PKEY_AUTH_STATUS
        # perf
        if str(_rkey).find(PKEY_VCS_CPU_USE_SHOW) > 0 :
            return True, [_st1, _step2, _step3, _rkey], None
        if str(_rkey).find(PKEY_VCS_PORT_BPS_SHOW) > 0 :
            return True, [_st1, ST2_PORT, _step3, _rkey], None
        if str(_rkey).find(PKEY_VCS_PORT_PPS_SHOW) > 0 :
            return True, [_st1, ST2_PORT, _step3, _rkey], None
        if str(_rkey).find(PKEY_VCS_DIPORT_BPS_SHOW) > 0 :
            return True, [_st1, ST2_PORT, _step3, _rkey], None
        if str(_rkey).find(PKEY_VCS_DIPORT_PPS_SHOW) > 0 :
            return True, [_st1, ST2_PORT, _step3, _rkey], None

    # s-net/oracle PCF
    elif str(_rkey).find(PKEY_PCF_SNET) > -1 or str(_rkey).find(PKEY_PCF_ORACLE) > -1:
        # alarm
        if str(_rkey).find(PKEY_VOR_PROC_STATUS) > 0 :
            return True, [_st1, ST2_PROC, ST3_STATUS, _rkey], None

        # perf


    #   SS VNF/CU
    #elif str(_rkey).find(PKEY_SMF_SAMSUNG) > -1 or str(_rkey).find(PKEY_UPF_SAMSUNG) > -1 or str(_rkey).find(PKEY_AMF_SAMSUNG) > -1
    elif PKEY_SAMSUNG_PRE in _rkey:
        # alarm
        if str(_rkey).find(PKEY_VSS_PROC_STATUS) > 0 :
            return True, [_st1, ST2_PROC, ST3_STATUS, _rkey], None
        if str(_rkey).find(PKEY_VSS_VNF_STATUS) > 0 :
            return True, [_st1, ST2_VDU, ST3_STATUS, _rkey], None
        if str(_rkey).find(PKEY_VSS_PORT_STATUS) > 0 :
            return True, [_st1, ST2_PORT, ST3_STATUS, _rkey], None
        if PKEY_VSS_NE_LIST_SHOW in _rkey:
            return True, [_st1, ST2_NE, ST3_STATUS, _rkey], None
        if PKEY_VSS_NE_CONN_SHOW in _rkey:
            return True, [_st1, ST2_NE, ST3_CONN, _rkey], None

        # perf
        if str(_rkey).find(PKEY_VSS_CPU_USE) > 0 :
            return True, [_st1, _step2, ST3_UTIL, _rkey], None
        if str(_rkey).find(PKEY_VSS_FS_USE) > 0 :
            return True, [_st1, ST2_FS, ST3_UTIL, _rkey], None
        if str(_rkey).find(PKEY_VSS_EXT_PORT_INFO) > 0 :
            return True, [_st1, ST2_PORT, _step3, _rkey], None
        if str(_rkey).find(PKEY_VSS_DPDK_INT_PORT_INFO) > 0 :
            return True, [_st1, ST2_PORT, _step3, _rkey], None
        if str(_rkey).find(PKEY_VSS_IPC_INT_PORT_INFO) > 0 :
            return True, [_st1, ST2_PORT, _step3, _rkey], None
        if PKEY_VSS_CPU_LOAD_SHOW in _rkey:
            return True, [_st1, ST2_CPU, ST3_LOAD, _rkey], None
        if PKEY_VSS_MEM_UTIL_SHOW in _rkey:
            return True, [_st1, ST2_MEM, ST3_UTIL, _rkey], None
        if PKEY_VSS_FS_UTIL_SHOW in _rkey:
            return True, [_st1, ST2_FS, ST3_UTIL, _rkey], None

    #   EMC ST
    elif PKEY_STORAGE_EMC in _rkey:
         if PKEY_VEMC_ST_STATUS in _rkey:
            return True, [_st1, ST2_ST, ST3_STATUS, _rkey], None
         if PKEY_VEMC_PORT_STATUS in _rkey:
            return True, [_st1, ST2_PORT, ST3_STATUS, _rkey], None
         if PKEY_VEMC_PORT_LINK_SHOW in _rkey:
            return True, [_st1, ST2_PORT, ST3_LINK, _rkey], None
         if PKEY_VEMC_FAN_STATUS in _rkey:
            return True, [_st1, ST2_FAN, ST3_STATUS, _rkey], None
         if PKEY_VEMC_POWER_STATUS in _rkey:
            return True, [_st1, ST2_PSU, ST3_STATUS, _rkey], None
         if PKEY_VEMC_PROC_STATUS in _rkey:
            return True, [_st1, ST2_PROC, ST3_STATUS, _rkey], None
         #PKEY_CPU_USE_TOP
         if PKEY_VEMC_MEM_STATUS in _rkey:
            return True, [_st1, ST2_MEM, ST3_STATUS, _rkey], None
         if PKEY_VEMC_MEM_UTIL_SHOW in _rkey:
            return True, [_st1, ST2_MEM, ST3_UTIL, _rkey], None
         if PKEY_VEMC_DISK_USE_SHOW in _rkey:
            return True, [_st1, ST2_FS, ST3_UTIL, _rkey], None
         if PKEY_VEMC_TEMP_DEGREE_SHOW in _rkey:
            return True, [_st1, ST2_TEMP, _step3, _rkey], None
         #PKEY_PORT_BYTE_STAT
         #PKEY_PORT_PACKET_STAT
         #PKEY_PORT_ERROR_STAT
         #PKEY_PORT_DROP_STAT

    #   ELUON
    elif PKEY_ELUON_PRE in _rkey:
         if PKEY_VEL_VNF_STATUS in _rkey:
            return True, [_st1, ST2_VDU, ST3_STATUS, _rkey], None
         elif PKEY_VEL_PROC_STATUS in _rkey:
            return True, [_st1, ST2_PROC, _step3, _rkey], None
         elif PKEY_VEL_PORT_STATUS in _rkey:
            return True, [_st1, ST2_PORT, _step3, _rkey], None
         elif PKEY_VEL_CPU_UTIL_SHOW in _rkey:
            return True, [_st1, ST2_CPU, ST3_UTIL, _rkey], None
         elif PKEY_VEL_MEM_UTIL_SHOW in _rkey:
            return True, [_st1, ST2_MEM, ST3_UTIL, _rkey], None
         elif PKEY_VEL_FS_UTIL_SHOW in _rkey:
            return True, [_st1, ST2_FS, ST3_UTIL, _rkey], None

    #   ARIEL
    elif PKEY_ARIEL_PRE in _rkey:
         if PKEY_VAR_VNF_STATUS in _rkey:
            return True, [_st1, ST2_VDU, ST3_STATUS, _rkey], None
         elif PKEY_VAR_PROC_STATUS in _rkey:
            return True, [_st1, ST2_PROC, _step3, _rkey], None
         elif PKEY_VAR_PORT_STATUS in _rkey:
            return True, [_st1, ST2_PORT, _step3, _rkey], None
         elif PKEY_VAR_CPU_LOAD_SHOW in _rkey:
            return True, [_st1, ST2_CPU, ST3_LOAD, _rkey], None
         elif PKEY_VAR_MEM_UTIL_SHOW in _rkey:
            return True, [_st1, ST2_MEM, ST3_UTIL, _rkey], None
         elif PKEY_VAR_PORT_BPS_SHOW in _rkey:
            return True, [_st1, ST2_PORT, _step3, _rkey], None
         elif PKEY_VAR_PORT_PPS_SHOW in _rkey:
            return True, [_st1, ST2_PORT, _step3, _rkey], None
         #PKEY_AUTH_STATUS

    return False, None, "No Step Info, key=%s"%( _rkey)


def get_alert_type(_is1, _is2, _is3, _is4):
    if _is3 in (ST3_CONN, ST3_LINK, ST3_STATUS, ST3_STATUS_ALL, ST3_GRP_STATUS) :
        ## CC:: conn, link, status, status_all, grp_status
        return ATYPE_STATUS ## CC:: status
    ## constant, fluctuation
    if _is3 in (ST3_LOAD, ST3_UTIL, ST3_DEGREE, ST3_RX_BPS, ST3_RX_PPS, ST3_RX_UTIL, ST3_TX_BPS, ST3_TX_PPS, ST3_TX_UTIL):
        ## CC:: load, util, degree, rx_bps, rx_pps, rx_util, tx_bps, tx_pps, tx_util
        return ATYPE_RANGE  ## CC:: range
    ## increase
    if _is3 in (ST3_RX_DROP, ST3_RX_ERR, ST3_TX_DROP, ST3_TX_ERR):
        ## CC:: rx_drop, rx_err, tx_drop, tx_err
        return ATYPE_RANGE  ## CC:: range
    ## 0, fluctuation
    if _is3 in (ST3_RX_DROP_PPS_NORM, ST3_TX_DROP_PPS_NORM, ST3_RX_DROP_GAP, ST3_RX_DROP_PPS, ST3_TX_DROP_GAP, ST3_TX_DROP_PPS, ST3_RX_ERR_GAP, ST3_RX_ERR_PPS, ST3_TX_ERR_GAP, ST3_TX_ERR_PPS,
                ST3_RX_UTIL_RATE, ST3_TX_UTIL_RATE):
        ## CC:: rx_drop_pps_norm, tx_drop_pps_norm, rx_drop_gap, rx_drop_pps, tx_drop_gap, tx_drop_pps, rx_err_gap, rx_err_pps, tx_err_gap, tx_err_pps, 
        ## CC:: rx_util_rate, tx_util_rate
        return ATYPE_RANGE  ## CC:: range
    return ATYPE_UNKNOWN    ## CC:: unknown


def load_item_val(_bdir, _hname, _item_type):
    _fname = _bdir + "/" + _hname + "." + _item_type + ".csv"
    _iv_dict = {}
    if not os.path.isfile(_fname) :
        return _iv_dict

    cf = open(_fname, "r")
    cr = csv.reader(cf)
    # type + "$$$$" + obj : dttm, data, 1-diff
    for _l in cr:
        _iv_dict[_l[0] + "$$$$" + _l[1]] = [_l[2], _l[3], _l[4]]
    return _iv_dict

def save_item_val(_bdir, _hname, _item_type, _ival_dict):
    _fname = _bdir + "/" + _hname + "." + _item_type + ".csv"
    _dir = os.path.dirname(_fname)
    if not os.path.isdir(_dir):
        os.makedirs(_dir)

    cf = open(_fname, "w")
    cw = csv.writer(cf)
    # type + "$$$$" + obj : dttm, data, 1-diff
    for _iv in _ival_dict:
        _l = str(_iv).split("$$$$") + _ival_dict[_iv]
        cw.writerow(_l)

    cf.close()


