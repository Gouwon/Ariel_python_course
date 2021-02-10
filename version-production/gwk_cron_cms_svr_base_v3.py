#!/usr/bin/python2
#-*- coding: utf-8 -*-

## default LIB
import json, os, threading
from datetime import datetime
from dateutil import parser
import re

#for TEST
import sys

## open LIB
import ruamel.yaml
import paramiko
import warnings
from time import sleep
warnings.filterwarnings(action='ignore', module='.*paramiko.*')

## custom LIB
from util.ko_logger import ko_logger
import util.gwk_fgpxy_lib as flib
from util.gwk_fgpxy_lib import RawLog, DBParam, DBWriter, RawWriter, get_steps, get_alert_type, show_runtime, exec_cmd, loadConfig
from util.db_mng import DBManager


## Abstract path
_SELF_PATH = os.path.dirname(os.path.abspath(__file__))


## setting for daemon
L_DIR = 'fgmon_pxy'
DEF_LOG_DIR = '/var/log/%s'%L_DIR
DEF_RAW_DIR = '%s/%s'%(DEF_LOG_DIR, "raw")
DEF_DAT_DIR = '%s/%s'%(DEF_LOG_DIR, "dat")
# DEF_AUTH_CRT = './key/%s_crt.pem'%TITLE
# DEF_AUTH_KEY = './key/%s_key.pem'%TITLE
# DEF_AUTH_CER = './key/%s_cac.pem'%TITLE

logger = None
def create_logger(_log_name):
    global logger
    logger = ko_logger(tag=_log_name, logdir=DEF_LOG_DIR, loglevel="debug", logConsole=False).get_instance()

_LNM = None

CMD_IS_RUN = """ ps -ef | grep -v grep | grep -v "%s" | grep -v "%s" | grep -c "%s" """
# CMD_IS_RUN = """ ps -ef | grep -v grep | grep "%s" """%( os.path.basename(str(__file__)) )



SQL_DATA_Q = []
DB_POOL_CNT = 2

RAW_DATA_Q = []

DEF_MAX_DTTM_GAP = 60*30

## Alarm
INAME_A_SSH = "ssh"
INAME_A_VM = "vm"
INAME_A_PORT = "port"
INAME_A_PSU = "psu"
INAME_A_FAN = "fan"
## Perf
INAME_P_CPU = "cpu_info"
INAME_P_MEM = "mem_info"
INAME_P_FS = "fs_info"
INAME_P_PORT = "port_info"
INAME_P_TEMP = "temp_info"
## for TEST
TEST_CRON_PROC = 'proc'



def log_info(_txt):
    logger.info(_txt)


def is_run(_pid, _ppid, _basenm):
    _is_run_cmd = CMD_IS_RUN%( str(_pid), str(_ppid), _basenm )
    _res, _ret = exec_cmd(_is_run_cmd)
    if not _res :
        logger.error("FAIL: IS_RUN Check Error, exec_err=%s, cmd=%s"%( str(_ret), _is_run_cmd ))
        return 0

    try:
#         logger.debug(_ret)
#         logger.debug("pid: "+str(os.getpid()))
#         logger.debug("ppid: "+str(os.getppid()))
        return int(_ret)
    except Exception as e:
        logger.error("FAIL: IS_RUN Check Error, ret=%s, cmd=%s"%( str(_ret), _is_run_cmd ))
        logger.exception(e)
        return 0


def run_ssh_cmd(_ssh, _cmd):
    try:
        _std = _ssh.exec_command(_cmd)
        _out = str(_std[1].read()).strip()
        _err = str(_std[2].read()).strip()

        if _err != "" :
            return False, _err
        else:
            return True, _out
    except Exception as e:
        # logger.exception(e)
        print("run_ssh_cmd error::{}".format(e))
        return False, str(e)

def get_ssh_status(_rg, _hname, _is_conn):
    _c_dttm = str(datetime.now())
    _conn_key = '%s.%s'%( flib.PKEY_CMS_SVR, flib.PKEY_CONN_SSH )
    _conn_cmd = 'ssh'
    _conn_cmd_res = True
    _conn_cmd_body = 'succ' if _is_conn else 'fail'
    ## Raw Logging
    RAW_DATA_Q.append( RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body) )

    ## DB
    _res, _st, _err = get_steps(_conn_key)
    if not _res :
        logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
        return
    _atype = get_alert_type(*_st)

    SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], "", None, _conn_cmd_body, _atype, flib.RES_SC) )
    return

def get_vm_status(_ssh, _rg, _hname):
    '''
    dj-amf02-cf2-d4fb4f51:running
    '''
    try:
        retry = 1
        maxtry = 3
        while retry <= maxtry:
            _cmd1 = """ virsh list --all | grep -E "^([[:space:]]*[0-9]+)" | awk '{print $2":"$3}' """
            _cmd2 = """ virsh list --all  """
            _res, _cmd_body = run_ssh_cmd(_ssh, _cmd1)
            if not _res :
                _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
                logger.error("FAIL: SSH CMD RUN Error, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))
            if not _cmd_body:
                logger.info('FAIL: SSH CMD RUN Error No-Return, host={}({}) cmd={} err={} try={}/{}'.format(_hname, _rg, _cmd1, _cmd_body, retry, maxtry))
                retry += 1
                sleep(0.1)
            else:
                break

        _c_dttm = str(datetime.now())
        _conn_key = "%s.%s"%( flib.PKEY_CMS_SVR, flib.PKEY_VM_STATUS_ALL_VIR )
        _conn_cmd = _cmd1.strip()
        _conn_cmd_res = False if str(_cmd_body).find("error:") > -1 else True
        _conn_cmd_body = _cmd_body
        ## Raw Logging
        RAW_DATA_Q.append( RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body) )

        _res, _st, _err = get_steps(_conn_key)
        if not _res :
            logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
            return
        _atype = get_alert_type(*_st)

        ## DB
        if str(_conn_cmd_body).strip() == "" :
            SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], 'No VM', None, 'No VM', _atype, flib.RES_SC) )
        else:
            SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], 'No VM', None, 'running', _atype, flib.RES_SC) )
            for _ll in str(_conn_cmd_body).splitlines():
                _lparts = _ll.split(":")
                _obj = str(_lparts[0]).strip()
                if len(_lparts) < 2 or _obj == "" :
                    logger.error("FAIL: RawData Parse Error, No Obj, host=%s, data=%s"%( _hname, _ll ))
                    continue

                lp2 = str(_lparts[1]).strip().lower()
                if lp2 == "" :
                    SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _obj, None, lp2, _atype, flib.RES_FA, flib.ERR_NO_VAL) )
                    logger.warning("FAIL: RawData Parse Error, No Value, host=%s, obj=%s, data=%s"%( _hname, _obj, _ll ))
                    continue

                SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _obj, None, lp2, _atype, flib.RES_SC) )

        return
    except Exception as e:
        logger.exception(e)
        logger.error("FAIL: VM GET Error, host=%s(%s), exc=%s"%( str(_hname), _rg, str(e) ))
        return

def get_port_status(_ssh, _rg, _hname):
    try:
        '''
        em4:down
        p1p1_0:down
        p1p1:up
        '''
        ## port status
        _cmd_port_status = """ for _ifname in `command grep PCI_SLOT_NAME /sys/class/net/*/device/uevent | awk -F '/' '{print $5}'`;do   echo -n "$_ifname:"; cat /sys/class/net/$_ifname/operstate 2>&1 ; done; """
        _cmd_port_status_chk = """ ls -al /sys/class/net/*/operstate """
        _key_port = "%s.%s"%( flib.PKEY_CMS_SVR, flib.PKEY_PORT_STATUS_ALL_OPER )
        try:
            _res, _cmd_body = run_ssh_cmd(_ssh, _cmd_port_status)
            if not _res :
                _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd_port_status_chk)
                logger.error("FAIL: SSH CMD RUN Error, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd_port_status, str(_cmd_body), str(_chK_ret) ))
            if _cmd_body == "":
                _res = False
                _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd_port_status_chk)
                logger.error("FAIL: SSH CMD No-Return, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd_port_status, str(_cmd_body), str(_chK_ret) ))

            _c_dttm = str(datetime.now())
            _conn_key = _key_port
            _conn_cmd = _cmd_port_status.strip()
            _conn_cmd_res = _res
            _conn_cmd_body = _cmd_body

            ## Raw Logging
            RAW_DATA_Q.append( RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body) )

            ## DB
            _res, _st, _err = get_steps(_conn_key)
            if not _res :
                logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
                return
            _atype = get_alert_type(*_st)

            for _ll in str(_conn_cmd_body).splitlines():
                _lparts = _ll.split(":")
                _obj = str(_lparts[0]).strip()
                if len(_lparts) < 2 or _obj == "" :
                    logger.error("FAIL: RawData Parse Error, No Obj, host=%s, data=%s"%( _hname, _ll ))
                    continue

                lp2 = str(_lparts[1]).strip().lower()
                if lp2 == "" :
                    SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _obj, None, lp2, _atype, flib.RES_FA, flib.ERR_NO_VAL) )
                    logger.warning("FAIL: RawData Parse Error, No Status, host=%s, obj=%s, data=%s"%( _hname, _obj, _ll ))
                    continue

                SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _obj, None, lp2, _atype, flib.RES_SC) )

        except Exception as e:
            logger.exception(e)
            logger.error("FAIL: Port-Status Get Exception, host=%s(%s), exc=%s"%( str(_hname), _rg, str(e) ))

        ## link status
        '''
        em1:cat: /sys/class/net/em1/carrier: Invalid argument
        p1p1:1
        p4p2_0:cat: /sys/class/net/p4p2_0/carrier: Invalid argument
        '''
        _cmd_port_link = """ for _ifname in `command grep PCI_SLOT_NAME /sys/class/net/*/device/uevent | awk -F '/' '{print $5}'`;do   echo -n "$_ifname:"; cat /sys/class/net/$_ifname/carrier 2>&1 ; done; """
        _cmd_port_link_chk = """ ls -al /sys/class/net/*/carrier """
        _key_link = "%s.%s"%( flib.PKEY_CMS_SVR, flib.PKEY_PORT_LINK_ALL_CARR )
        try:
            _res, _cmd_body = run_ssh_cmd(_ssh, _cmd_port_link)
            if not _res :
                _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd_port_link_chk)
                logger.error("FAIL: SSH CMD RUN Error, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd_port_link, str(_cmd_body), str(_chK_ret) ))
            if _cmd_body == "":
                _res = False
                _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd_port_link_chk)
                logger.error("FAIL: SSH CMD No-Return, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd_port_link, str(_cmd_body), str(_chK_ret) ))

            _c_dttm = str(datetime.now())
            _conn_key = _key_link
            _conn_cmd = _cmd_port_link.strip()
            _conn_cmd_res = _res
            _conn_cmd_body = _cmd_body

            ## Raw Logging
            RAW_DATA_Q.append( RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body) )

            ## DB
            _res, _st, _err = get_steps(_conn_key)
            if not _res :
                logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
                return
            _atype = get_alert_type(*_st)

            for _ll in str(_conn_cmd_body).splitlines():
                _lparts = _ll.split(":")
                _obj = str(_lparts[0]).strip()
                if len(_lparts) < 2 or _obj == "" :
                    logger.error("FAIL: RawData Parse Error, No Obj, host=%s, data=%s"%( _hname, _ll ))
                    continue

                lp2 = str(_lparts[1]).strip().lower()
                if lp2 == "" :
                    SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _obj, None, lp2, _atype, flib.RES_FA, flib.ERR_NO_VAL) )
                    logger.warning("FAIL: RawData Parse Error, No Status, host=%s, obj=%s, data=%s"%( _hname, _obj, _ll ))
                    continue

                SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _obj, None, lp2, _atype, flib.RES_SC) )

        except Exception as e:
            logger.exception(e)
            logger.error("FAIL: Port-Link Get Exception, host=%s(%s), exc=%s"%( str(_hname), _rg, str(e) ))

        return
    except Exception as e:
        logger.exception(e)
        logger.error("FAIL: Port GET Error, host=%s(%s), exc=%s"%( str(_hname), _rg, str(e) ))
        return

def get_psu_status(_ssh, _rg, _hname):
    '''
    Safe
    '''
    try:
        _cmd1 = """ dmidecode -t chassis | grep -i "power supply" | awk -F ':' '{print $2}' """
        _cmd2 = """ dmidecode -t chassis  """
        _res, _cmd_body = run_ssh_cmd(_ssh, _cmd1)
        if not _res :
            _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
            logger.error("FAIL: SSH CMD RUN-Error, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

        if _cmd_body == "":
            _res = False
            _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
            logger.error("FAIL: SSH CMD No-Return, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

        _c_dttm = str(datetime.now())
        _conn_key = "%s.%s"%( flib.PKEY_CMS_SVR, flib.PKEY_PSU_STATUS_DMI )
        _conn_obj = ""
        _conn_cmd = _cmd1.strip()
        _conn_cmd_res = _res
        _conn_cmd_body = _cmd_body

        ## Raw Logging
        RAW_DATA_Q.append( RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body) )

        ## DB
        _res, _st, _err = get_steps(_conn_key)
        if not _res :
            logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
            return
        _atype = get_alert_type(*_st)

        SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], "", None, _conn_cmd_body, _atype, flib.RES_SC) )
        return
    except Exception as e:
        logger.exception(e)
        logger.error("FAIL: PSU GET Error, host=%s(%s), exc=%s"%( str(_hname), _rg, str(e) ))
        return

def get_fan_status(_ssh, _rg, _hname, _sdr=False):
    try:
        '''
        false
        '''
        _cmd1 = """ ipmitool chassis status | grep -i "fan" | awk -F ':' '{print $2}' """
        _cmd2 = """ ipmitool chassis status """
        _res, _cmd_body = run_ssh_cmd(_ssh, _cmd1)
        if not _res :
            _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
            logger.error("FAIL: SSH CMD RUN Error, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

        if _cmd_body == "":
            _res = False
            _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
            logger.error("FAIL: SSH CMD No-Return, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

        _c_dttm = str(datetime.now())
        _conn_key = "%s.%s"%( flib.PKEY_CMS_SVR, flib.PKEY_FAN_ALL_IPMI )
        _conn_obj = ""
        _conn_cmd = _cmd1.strip()
        _conn_cmd_res = _res
        _conn_cmd_body = _cmd_body

        ## Raw Logging
        RAW_DATA_Q.append( RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body) )

        ## DB
        _res, _st, _err = get_steps(_conn_key)
        if not _res :
            logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
            return
        _atype = get_alert_type(*_st)

        SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], "", None, _conn_cmd_body, _atype, flib.RES_SC) )


        if _sdr:
            '''
            Fan1             : ok
            Fan2             : ok
            Fan3             : ok
            Fan4             : ok
            Fan5             : ok
            Fan6             : ok
            or
            Fan 6            : ok
            Fan 6 DutyCycle  : ok
            Fan 6 Presence   : ok
            Fans             : ok
            '''
            _cmd1 = """ ipmitool sdr type "fan" | grep -vi "redundancy" | awk -F '|' '{print $1":"$3}' """
            _cmd2 = """ ipmitool sdr type "fan" | grep -vi "redundancy" """
            _res, _cmd_body = run_ssh_cmd(_ssh, _cmd1)
            if not _res :
                _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
                logger.error("FAIL: SSH CMD RUN Error, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

            if _cmd_body == "":
                _res = False
                _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
                logger.error("FAIL: SSH CMD No-Return, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

            _c_dttm = str(datetime.now())
            _conn_key = "%s.%s"%( flib.PKEY_CMS_SVR, flib.PKEY_FAN_IPMI )
            _conn_obj = ""
            _conn_cmd = _cmd1.strip()
            _conn_cmd_res = _res
            _conn_cmd_body = _cmd_body

            ## Raw Logging
            RAW_DATA_Q.append( RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body) )

            ## DB
            _res, _st, _err = get_steps(_conn_key)
            if not _res :
                logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
                return
            _atype = get_alert_type(*_st)

            for _ll in str(_conn_cmd_body).splitlines():
                _lparts = _ll.split(":")
                _obj = str(_lparts[0]).strip()
                if len(_lparts) < 2 or _obj == "" :
                    logger.error("FAIL: RawData Parse Error, No Obj, host=%s, data=%s"%( _hname, _ll ))
                    continue

                _obj_p = _obj.split()
                if len(_obj_p) == 1 :
                    if str(_obj_p[0]).lower() == "fans":
                        continue
                    else:
                        _obj=_obj_p[0]
                elif len(_obj_p) == 2 :
                    if str(_obj_p[0]).lower() == 'fan' and str(_obj_p[1]).isdigit() :
                        _obj = _obj_p[0] + _obj_p[1]
                    else:
                        continue
                else:
                    continue

                lp2 = str(_lparts[1]).strip().lower()
                if lp2 == "" :
                    SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _obj, None, lp2, _atype, flib.RES_FA, flib.ERR_NO_VAL) )
                    logger.warning("FAIL: RawData Parse Error, No Status, host=%s, obj=%s, data=%s"%( _hname, _obj, _ll ))
                    continue

                SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _obj, None, lp2, _atype, flib.RES_SC) )
        return
    except Exception as e:
        logger.exception(e)
        logger.error("FAIL: FAN GET Error, host=%s(%s), exc=%s"%( str(_hname), _rg, str(e) ))
        return

def get_proc_openstack_status(_ssh, _rg, _hname):
    try:
        '''
        MainPID=2592 Id=neutron-server.service ActiveState=active
        MainPID=1474 Id=openstack-aodh-evaluator.service ActiveState=active
        MainPID=1497 Id=openstack-aodh-listener.service ActiveState=active
        MainPID=1495 Id=openstack-aodh-notifier.service ActiveState=active
        MainPID=1468 Id=openstack-ceilometer-central.service ActiveState=active
        MainPID=1472 Id=openstack-ceilometer-collector.service ActiveState=active
        MainPID=1466 Id=openstack-ceilometer-notification.service ActiveState=active
        MainPID=1496 Id=openstack-cinder-api.service ActiveState=active
        MainPID=1469 Id=openstack-cinder-scheduler.service ActiveState=active
        MainPID=8193 Id=openstack-cinder-volume.service ActiveState=active
        MainPID=1493 Id=openstack-glance-api.service ActiveState=active
        MainPID=1471 Id=openstack-glance-registry.service ActiveState=active
        MainPID=1470 Id=openstack-gnocchi-metricd.service ActiveState=active
        MainPID=1467 Id=openstack-gnocchi-statsd.service ActiveState=active
        MainPID=1475 Id=openstack-heat-api-cfn.service ActiveState=active
        MainPID=1458 Id=openstack-heat-api.service ActiveState=active
        MainPID=3387 Id=openstack-heat-engine.service ActiveState=active
        MainPID=0 Id=openstack-losetup.service ActiveState=active
        MainPID=3267 Id=openstack-nova-api.service ActiveState=active
        MainPID=3284 Id=openstack-nova-cert.service ActiveState=active
        MainPID=2671 Id=openstack-nova-conductor.service ActiveState=active
        MainPID=2695 Id=openstack-nova-consoleauth.service ActiveState=active
        MainPID=1463 Id=openstack-nova-novncproxy.service ActiveState=active
        MainPID=2720 Id=openstack-nova-scheduler.service ActiveState=active
        '''
        _cmd1 = 'openstack-service status'
        _res, _cmd_body = run_ssh_cmd(_ssh, _cmd1)
        if not _res or not _cmd_body:
            logger.error("FAIL: SSH CMD RUN Error, host={}({}), cmd={}, err={}".format( str(_hname), _rg, _cmd1, str(_cmd_body) ))

        _c_dttm = str(datetime.now())
        _conn_key = '{}.{}'.format( flib.PKEY_CMS_SVR, flib.PKEY_PROC_OPENSTACK_STATUS )
        #cmn.svr.proc.openstack.show
        _conn_obj = ""
        _conn_cmd = _cmd1.strip()
        _conn_cmd_res = _res
        _conn_cmd_body = _cmd_body

        _res, _st, _err = get_steps(_conn_key)
        #svr > proc > status
        if not _res :
            logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
            return
        _atype = get_alert_type(*_st)

        for _ll in str(_conn_cmd_body).splitlines():
            #0            1                         2
            #MainPID=2592 Id=neutron-server.service ActiveState=active
            _lparts = _ll.split()
            try:
                #0  1
                #Id=neutron-server.service
                lp1 = _lparts[1].split('=')
                _obj = lp1[1]
                #0           1
                #ActiveState=active
                lp2 = _lparts[2].split('=')
                lp2_1 = lp2[1]
            except Exception as e:
                print('exception={}'.format(e))
                continue

            if not _obj:
                logger.error("FAIL: RawData Parse Error, No Obj, host=%s, data=%s"%( _hname, _ll ))
                continue

            SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _obj, None, lp2_1, _atype, flib.RES_SC) )

        return
    except Exception as e:
        logger.exception(e)
        logger.error("FAIL: FAN GET Error, host=%s(%s), exc=%s"%( str(_hname), _rg, str(e) ))
        return

def get_proc_libvirtd_status(_ssh, _rg, _hname):
    try:
        '''
        ● libvirtd.service - Virtualization daemon
           Loaded: loaded (/usr/lib/systemd/system/libvirtd.service; enabled; vendor preset: enabled)
           Active: active (running) since Tue 2020-05-19 12:25:23 KST; 2 days ago
             Docs: man:libvirtd(8)
                   http://libvirt.org
         Main PID: 2677 (libvirtd)
            Tasks: 18 (limit: 32768)
           CGroup: /system.slice/libvirtd.service
                   ├─2677 /usr/sbin/libvirtd --listen
                   ├─4707 /usr/sbin/dnsmasq --conf-file=/var/lib/libvirt/dnsmasq/default.conf --leasefile-ro --dhcp-script=/usr/libexec/libvirt_leaseshelper
                   └─4708 /usr/sbin/dnsmasq --conf-file=/var/lib/libvirt/dnsmasq/default.conf --leasefile-ro --dhcp-script=/usr/libexec/libvirt_leaseshelper

        May 19 12:25:22 tbccgni01nova0307 systemd[1]: Starting Virtualization daemon...
        May 19 12:25:23 tbccgni01nova0307 systemd[1]: Started Virtualization daemon.
        May 19 12:25:31 tbccgni01nova0307 dnsmasq[4707]: started, version 2.76 cachesize 150
        May 19 12:25:31 tbccgni01nova0307 dnsmasq[4707]: compile time options: IPv6 GNU-getopt DBus no-i18n IDN DHCP DHCPv6 no-Lua TFTP no-conntrack ipset auth no-DNSSEC loop-detect inotify
        May 19 12:25:31 tbccgni01nova0307 dnsmasq-dhcp[4707]: DHCP, IP range 192.168.122.2 -- 192.168.122.254, lease time 1h
        May 19 12:25:31 tbccgni01nova0307 dnsmasq-dhcp[4707]: DHCP, sockets bound exclusively to interface virbr0
        May 19 12:25:31 tbccgni01nova0307 dnsmasq[4707]: no servers found in /etc/resolv.conf, will retry
        May 19 12:25:31 tbccgni01nova0307 dnsmasq[4707]: read /etc/hosts - 61 addresses
        May 19 12:25:31 tbccgni01nova0307 dnsmasq[4707]: read /var/lib/libvirt/dnsmasq/default.addnhosts - 0 addresses
        May 19 12:25:31 tbccgni01nova0307 dnsmasq-dhcp[4707]: read /var/lib/libvirt/dnsmasq/default.hostsfile
        '''
        _cmd1 = 'systemctl status libvirtd'
        _res, _cmd_body = run_ssh_cmd(_ssh, _cmd1)
        if not _res or not _cmd_body:
            logger.error("FAIL: SSH CMD RUN Error, host={}({}), cmd={}, err={}".format( str(_hname), _rg, _cmd1, str(_cmd_body) ))

        #print('body={}'.format(_cmd_body))

        _c_dttm = str(datetime.now())
        _conn_key = '{}.{}'.format( flib.PKEY_CMS_SVR, flib.PKEY_PROC_LIBVIRTD_STATUS )
        #cmn.svr.proc.libvirtd.show
        _conn_obj = ""
        _conn_cmd = _cmd1.strip()
        _conn_cmd_res = _res
        _conn_cmd_body = _cmd_body

        _res, _st, _err = get_steps(_conn_key)
        #svr > proc > status
        if not _res :
            logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
            return
        _atype = get_alert_type(*_st)


        for _ll in str(_conn_cmd_body).splitlines():
            #0       1      2
            #Active: active (running) since Wed 2020-04-08 00:13:07 KST; 1 months 13 days ago
            #Active: inactive (dead)
            #print('ll={}'.format(_ll))
            g = re.search('[\s]+Active:', _ll)
            if g:
               _lparts = _ll.split()
               lp1 = _lparts[1]
               #print('lp1={}'.format(lp1))
               SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], 'libvirtd.service', None, lp1, _atype, flib.RES_SC) )
               break

        return
    except Exception as e:
        logger.exception(e)
        logger.error("FAIL: FAN GET Error, host=%s(%s), exc=%s"%( str(_hname), _rg, str(e) ))
        return

## (_c_dttm, str(self.hname), _conn_key, _conn_obj, _conn_cmd, _conn_cmd_res, _conn_cmd_body, _c_dttm, _conn_cmd, _conn_cmd_res, _conn_cmd_body)
def get_cpu_info(_ssh, _rg, _hname):
    '''
    load average: 3.22, 3.06, 3.12
    %Cpu(s):  3.9 us,  0.5 sy,  0.0 ni, 95.6 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st
    ----------------
    load average: 1.89, 1.85, 2.26
    Cpu(s):  8.1%us,  2.4%sy,  0.2%ni, 89.1%id,  0.1%wa,  0.0%hi,  0.0%si,  0.0%st
    '''
    try:
        _cmd1 = """ top -bn 1 | head -n 5 | grep -o "%Cpu.*\|load.*" """
        _cmd2 = """ top -bn 1 """
        _res, _cmd_body = run_ssh_cmd(_ssh, _cmd1)
        if not _res :
            _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
            logger.error("FAIL: SSH CMD RUN-Error, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

        if _cmd_body == "":
            _res = False
            _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
            logger.error("FAIL: SSH CMD No-Return, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

        _c_dttm = str(datetime.now())
        _conn_key = "%s.%s"%( flib.PKEY_CMS_SVR, flib.PKEY_CPU_USE_TOP )
        _conn_cmd = _cmd1.strip()
        _conn_cmd_res = _res
        _conn_cmd_body = _cmd_body

        ## Raw Logging
        RAW_DATA_Q.append( RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body) )

        ## DB
        _res, _steps_load, _err = get_steps(_conn_key, flib.ST3_LOAD)
        if not _res :
            logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
            return

        _res, _steps_util, _err = get_steps(_conn_key, flib.ST3_UTIL)
        if not _res :
            logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
            return

        _atype_load = get_alert_type(*_steps_load)
        _atype_util = get_alert_type(*_steps_util)

        for _ll in str(_conn_cmd_body).splitlines():

            _lllow = str(_ll).lower()
            if _lllow.find("load average") > -1 :
                _lparts = _lllow.split(":")
                if len(_lparts) < 2:
                    SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _steps_load[0], _steps_load[1], _steps_load[2], _steps_load[3], "", None, None, _atype_load, flib.RES_FA, flib.ERR_NO_VAL) )
                    logger.error("FAIL: RawData Parse Error, Invalid CPU Load Data, host=%s, line=%s"%( _hname, str(_lllow) ))
                    continue

                _lvals = str(_lparts[1]).split(",")
                if len(_lvals) < 1:
                    SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _steps_load[0], _steps_load[1], _steps_load[2], _steps_load[3], "", None, None, _atype_load, flib.RES_FA, flib.ERR_NO_VAL) )
                    logger.error("FAIL: RawData Parse Error, Invalid CPU Load Data, host=%s, line=%s"%( _hname, str(_lllow) ))
                    continue

                try:
                    _cload1m = round(float(_lvals[0]), 2)
                    SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _steps_load[0], _steps_load[1], _steps_load[2], _steps_load[3], "", _cload1m, None, _atype_load, flib.RES_SC) )
                except Exception as e:
                    SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _steps_load[0], _steps_load[1], _steps_load[2], _steps_load[3], "", None, None, _atype_load, flib.RES_FA, flib.ERR_INV_VAL) )
                    logger.error("FAIL: RawData Parse Error, Invalid CPU Load Value, exc=%s, host=%s, val=%s, line=%s"%( str(e), _hname, str(_lvals[0]), str(_lllow) ))
                    continue
            elif _lllow.find("cpu(s)") > -1 :
                _lparts = _lllow.split(":")
                if len(_lparts) < 2:
                    SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _steps_util[0], _steps_util[1], _steps_util[2], _steps_util[3], "", None, None, _atype_util, flib.RES_FA, flib.ERR_NO_VAL) )
                    logger.error("FAIL: RawData Parse Error, Invalid CPU Util Data, host=%s, line=%s"%( _hname, str(_lllow) ))
                    continue

                _lvals = str(_lparts[1]).split(",")
                _is_find = False
                _lval = None
                for _tlval in _lvals:
                    if str(_tlval).lower().find('id') > -1 :
                        _is_find = True
                        _lval = _tlval
                        break

                if _is_find :
                    try:
                        if _lval.find("%") > -1 :
                            _cutil = round(100 - float(_lval.split("%")[0]), 2)
                        else:
                            _cutil = round(100 - float(_lval.split()[0]), 2)
                        SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _steps_util[0], _steps_util[1], _steps_util[2], _steps_util[3], "", _cutil, None, _atype_util, flib.RES_SC) )
                    except Exception as e:
                        SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _steps_util[0], _steps_util[1], _steps_util[2], _steps_util[3], "", None, None, _atype_util, flib.RES_FA, flib.ERR_INV_VAL) )
                        logger.error("FAIL: RawData Parse Error, Invalid CPU Util Value, exc=%s, host=%s, val=%s, line=%s"%( str(e), _hname, str(_lval), str(_lllow) ))
                        continue
                else:
                    SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _steps_util[0], _steps_util[1], _steps_util[2], _steps_util[3], "", None, None, _atype_util, flib.RES_FA, flib.ERR_NO_VAL) )
                    logger.error("FAIL: RawData Parse Error, Invalid CPU Util Data, host=%s, line=%s"%( _hname, str(_lllow) ))
                    continue
            else:
                logger.warning("FAIL: RawData Parse Error, Invalid CPU Type, host=%s, line=%s"%( _hname, str(_lllow) ))
                continue

        return
    except Exception as e:
        logger.exception(e)
        logger.error("FAIL: CPU-Info GET Error, host=%s(%s), exc=%s"%( str(_hname), _rg, str(e) ))
        return

def get_mem_info(_ssh, _rg, _hname):
    '''
    MemTotal:       527888496 kB
    MemFree:        90967716 kB
    MemAvailable:   92011876 kB
    Buffers:            2100 kB
    Cached:          5107228 kB
    ----------------
    MemTotal:       61841836 kB
    MemFree:        47000536 kB
    Buffers:          704772 kB
    Cached:          6669476 kB
    '''
    try:
        _cmd1 = """ cat /proc/meminfo | grep "^MemTotal\|^MemAvailable\|^MemFree\|^Buffers\|^Cached" """
        _cmd2 = """ cat /proc/meminfo """
        _res, _cmd_body = run_ssh_cmd(_ssh, _cmd1)
        if not _res :
            _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
            logger.error("FAIL: SSH CMD RUN Error, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

        if _cmd_body == "":
            _res = False
            _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
            logger.error("FAIL: SSH CMD No-Return, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

        _c_dttm = str(datetime.now())
        _conn_key = "%s.%s"%( flib.PKEY_CMS_SVR, flib.PKEY_MEM_USE_PROC )
        _conn_cmd = _cmd1.strip()
        _conn_cmd_res = _res
        _conn_cmd_body = _cmd_body

        ## Raw Logging
        RAW_DATA_Q.append( RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body) )

        ## DB
        _res, _st, _err = get_steps(_conn_key, flib.ST3_UTIL)
        if not _res :
            logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
            return
        _atype = get_alert_type(*_st)

        _total = None
        _free = None
        _avail = None
        _buff = None
        _cache = None
        for _ll in str(_conn_cmd_body).splitlines():

            _lllow = str(_ll).lower()
            _lparts = _lllow.split(":")
            if len(_lparts) < 2:
                logger.error("FAIL: RawData Parse Error, Invalid MEM Data, host=%s, line=%s"%( _hname, str(_lllow) ))
                continue

            _mem_type = str(_lparts[0]).strip()

            _lvals = str(_lparts[1]).split()
            if len(_lvals) < 1:
                logger.error("FAIL: RawData Parse Error, Invalid MEM Data, host=%s, line=%s"%( _hname, str(_lllow) ))
                continue
            try:
                _mem_val = float(str(_lvals[0]).strip())

                if _mem_type.find('MemTotal'.lower()) > -1 :
                    _total = _mem_val
                elif _mem_type.find('MemFree'.lower()) > -1 :
                    _free = _mem_val
                elif _mem_type.find('MemAvailable'.lower()) > -1 :
                    _avail = _mem_val
                elif _mem_type.find('Buffers'.lower()) > -1 :
                    _buff = _mem_val
                elif _mem_type.find('Cached'.lower()) > -1 :
                    _cache = _mem_val
                else:
                    logger.warning("FAIL: RawData Parse Error, Invalid MEM Type, host=%s, line=%s"%( _hname, str(_lllow) ))
                    continue
            except Exception as e:
                logger.error("FAIL: RawData Parse Error, Invalid MEM Value, exc=%s, host=%s, val=%s, line=%s"%( str(e), _hname, str(_lvals[0]), str(_lllow) ))
                continue

        if None in (_total, _free, _buff, _cache) or _total == 0 :
            SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], "", None, None, _atype, flib.RES_FA, flib.ERR_INV_VAL) )
            # logger.error("FAIL: Invalid Raw Data, No MEM Parsed Data, host=%s, data=%s"%( _hname, _data ))
            return

        _used = _total - _free - _buff - _cache
        _util = round(_used/_total*100, 2)

        SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], "", _util, None, _atype, flib.RES_SC) )

        return
    except Exception as e:
        logger.exception(e)
        logger.error("FAIL: MEM-Info GET Error, host=%s(%s), exc=%s"%( str(_hname), _rg, str(e) ))
        return

def get_fs_info(_ssh, _rg, _hname):
    '''
    /dev/sda3      xfs      2340847876 2565756 2338282120   1% /
    /dev/sda2      xfs         1038336  161412     876924  16% /boot
    ---------
    /dev/mapper/vgroot-camiant_log     ext4      3997376 1693340   2094324      45% /var/camiant/log
    /dev/mapper/vgroot-camiant_tmp     ext4      8125880   18416   7688036       1% /var/camiant/tmp
    /dev/mapper/vgroot-camiant_upgrade ext4       999320    1536    945356       1% /var/camiant/upgrade
    '''
    try:
        _cmd1 = """ df -TP | grep "xfs\|ext4\|ext3" """
        _cmd2 = """ df -TP """
        _res, _cmd_body = run_ssh_cmd(_ssh, _cmd1)
        if not _res :
            _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
            logger.error("FAIL: SSH CMD RUN Error, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

        if _cmd_body == "":
            _res = False
            _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
            logger.error("FAIL: SSH CMD No-Return, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

        _c_dttm = str(datetime.now())
        _conn_key = "%s.%s"%( flib.PKEY_CMS_SVR, flib.PKEY_FS_USE_DF )
        _conn_cmd = _cmd1.strip()
        _conn_cmd_res = _res
        _conn_cmd_body = _cmd_body

        ## Raw Logging
        RAW_DATA_Q.append( RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body) )

        ## DB
        _res, _st, _err = get_steps(_conn_key, flib.ST3_UTIL)
        if not _res :
            logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
            return
        _atype = get_alert_type(*_st)

        for _ll in str(_conn_cmd_body).splitlines():
            _lparts = _ll.split()
            if len(_lparts) < 7:
                logger.error("FAIL: RawData Parse Error, Invalid FS Util Data, host=%s, line=%s"%( _hname, str(_ll) ))
                continue

            _obj = str(_lparts[6]).strip()
            if _obj == "" :
                logger.error("FAIL: RawData Parse Error, No FS Util Obj, host=%s, line=%s"%( _hname, _ll ))
                continue

            try:
                _util = round(float(str(_lparts[5]).replace("%", "")), 2)
                SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _obj, _util, None, _atype, flib.RES_SC) )
            except Exception as e:
                SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _obj, None, None, _atype, flib.RES_FA, flib.ERR_INV_VAL) )
                logger.error("FAIL: RawData Parse Error, Invalid FS Util Value, exc=%s, host=%s, val=%s, line=%s"%( str(e), _hname, str(_lparts[5]), str(_ll) ))
                continue

        return
    except Exception as e:
        logger.exception(e)
        logger.error("FAIL: FS-Info GET Error, host=%s(%s), exc=%s"%( str(_hname), _rg, str(e) ))
        return



def _get_port_byte(_c_dttm_str, _rg, _hname, _ikey, _data):
    '''
    p1p1_0:rx_bytes:0
    p1p1_0:tx_bytes:0
    p1p1_0:speed:cat: /sys/class/net/p1p1_0/speed: Invalid argument
    p1p1:rx_bytes:0
    p1p1:tx_bytes:70
    p1p1:speed:10000
    p1p2:rx_bytes:21825008718
    p1p2:tx_bytes:140
    p1p2:speed:10000
    -------------
    eth0:rx_bytes:412761873823
    eth0:tx_bytes:163722369982
    eth1:rx_bytes:567688478
    eth1:tx_bytes:23376922
    eth2:rx_bytes:77872
    eth2:tx_bytes:11062
    eth3:rx_bytes:2335647033
    eth3:tx_bytes:962347923
    '''

    _res, _steps_rxb, _err = get_steps(_ikey, flib.ST3_RX_BPS)
    if not _res :
        logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _data, _err ))
        return

    _res, _steps_txb, _err = get_steps(_ikey, flib.ST3_TX_BPS)
    if not _res :
        logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _data, _err ))
        return

    _res, _steps_rxu, _err = get_steps(_ikey, flib.ST3_RX_UTIL)
    if not _res :
        logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _data, _err ))
        return

    _res, _steps_txu, _err = get_steps(_ikey, flib.ST3_TX_UTIL)
    if not _res :
        logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _data, _err ))
        return

    _res, _steps_rxr, _err = get_steps(_ikey, flib.ST3_RX_UTIL_RATE)
    if not _res :
        logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _data, _err ))
        return

    _res, _steps_txr, _err = get_steps(_ikey, flib.ST3_TX_UTIL_RATE)
    if not _res :
        logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _data, _err ))
        return

    _file_type = "byte"
    _atype = flib.ATYPE_RANGE
    _c_dttm = parser.parse(_c_dttm_str)

    ## parse curr data
    _obj_bytes = {}
    _speeds = {}
    for _ll in str(_data).splitlines():
        _lparts = _ll.split(":")
        if len(_lparts) < 3:
            logger.error("FAIL: RawData Parse Error, Invalid Port Byte Data, host=%s, line=%s"%( _hname, str(_ll) ))
            continue

        _obj = str(_lparts[0]).strip()
        _type = str(_lparts[1]).strip()
        _val = str(_lparts[2]).strip()

        if _obj == "" :
            logger.error("FAIL: RawData Parse Error, No Port Byte Obj, host=%s, line=%s"%( _hname, _ll ))
            continue

        if _type == "rx_bytes":
            if not _type in _obj_bytes : _obj_bytes[_type] = {}
            try:
                _obj_bytes[_type][_obj] = round(float( _val ), 2)*8
            except Exception as e:
                _obj_bytes[_type][_obj] = None
                logger.error("FAIL: RawData Parse Error, Invalid Port Value, exc=%s, host=%s, type=%s, val=%s, line=%s"%( str(e), _hname, _type, str(_val), str(_ll) ))
        elif _type == "tx_bytes":
            if not _type in _obj_bytes : _obj_bytes[_type] = {}
            try:
                _obj_bytes[_type][_obj] = round(float( _val ), 2)*8
            except Exception as e:
                _obj_bytes[_type][_obj] = None
                logger.error("FAIL: RawData Parse Error, Invalid Port Value, exc=%s, host=%s, type=%s, val=%s, line=%s"%( str(e), _hname, _type, str(_val), str(_ll) ))
        elif _type == "speed":
            try:
                if str(_val).find("cat") > -1:
                    _speeds[_obj] = None
                else:
                    _speeds[_obj] = round(float( _val )*1000000, 2)
            except Exception as e:
                _speeds[_obj] = None
                logger.error("FAIL: RawData Parse Error, Invalid Port Value, exc=%s, host=%s, type=%s, val=%s, line=%s"%( str(e), _hname, _type, str(_val), str(_ll) ))
        else:
            logger.warning("FAIL: RawData Parse Error, Invalid Port Byte Type, host=%s, line=%s"%( _hname, str(_ll) ))
            continue


    ## load prev_data
    # type + "$$$$" + obj : dttm, data, 1-diff
    _iv_dict = flib.load_item_val(DEF_DAT_DIR, _hname, _file_type)

    ## calc value
    for _i_type in _obj_bytes :
        if _i_type == "rx_bytes" :
            _steps_b = _steps_rxb
            _steps_u = _steps_rxu
            _steps_r = _steps_rxr
        else:
            _steps_b = _steps_txb
            _steps_u = _steps_txu
            _steps_r = _steps_txr

        for _obj in _obj_bytes[_i_type]:
            _v_bps, _v_util, _v_urate = [None]*3
            _r_bps, _r_util, _r_urate = [flib.RES_FA]*3
            _e_bps, _e_util, _e_urate = [None]*3
            _has_speed = (_obj in _speeds) and (_speeds[_obj] != None)

            _i_key = _i_type+ "$$$$" + _obj
            _mon_bytes = _obj_bytes[_i_type][_obj]

            if _mon_bytes == None :
                _e_bps, _e_util, _e_urate = [flib.ERR_INV_VAL, flib.ERR_INV_VAL, flib.ERR_NO_ORG_VAL]
            else:
                ## for data
                _new_1_diff = None

                if not _i_key in _iv_dict :
                    logger.error("FAIL: No PrevData, host=%s, type=%s, obj=%s"%( _hname, _i_type, _obj ))
                    _e_bps, _e_util, _e_urate = [flib.ERR_NO_PRV_VAL, flib.ERR_NO_PRV_VAL, flib.ERR_NO_ORG_VAL]
                else:
                    _p_dttm_str, _p_val_str, _p_1_diff_str =_iv_dict[_i_key]
                    try:
                        _gap_dttm =  (_c_dttm - parser.parse(_p_dttm_str)).total_seconds()
                        _gap_bytes = _mon_bytes - float(_p_val_str)
                        if _gap_dttm > DEF_MAX_DTTM_GAP :
                            logger.error("FAIL: Mon Value Get Error, Too Old Prv Value, host=%s, type=%s, obj=%s, val=%s, prv_dttm=%s"%( _hname, _i_type, _obj, str(_mon_bytes), _p_dttm_str ))
                            _e_bps, _e_util, _e_urate = [flib.ERR_INV_PRV_VAL, flib.ERR_INV_PRV_VAL, flib.ERR_NO_ORG_VAL]
                        else:
                            if _gap_bytes < 0 :
                                logger.error("FAIL: Mon Value Get Error, Minus Value Gap, host=%s, type=%s, obj=%s, gap_v=%s"%( _hname, _i_type, _obj, str(_gap_bytes)) )
                                _e_bps, _e_util, _e_urate = [flib.ERR_MNS_VAL, flib.ERR_MNS_VAL, flib.ERR_INV_ORG_VAL]
                            else:
                                _v_bps = round(_gap_bytes/_gap_dttm, 2)
                                _r_bps = flib.RES_SC
                                _new_1_diff = _v_bps

                                ## for util
                                if _has_speed :
                                    _v_util = (_v_bps/_speeds[_obj])*100
                                    _r_util = flib.RES_SC

                                    ## for util-rate
                                    if str(_p_1_diff_str).strip() == "" or _p_1_diff_str == None :
                                        _r_urate = flib.RES_FA
                                        _e_urate = flib.ERR_NO_PRV_VAL
                                    else:
                                        try:
                                            _prv_util = (float(_p_1_diff_str)/_speeds[_obj])*100
                                            _gap_util = abs(_v_util - _prv_util)
                                            _v_urate = round(_gap_util/_gap_dttm, 2)
                                            _r_urate = flib.RES_SC
                                        except Exception as e:
                                            logger.error("FAIL: PrevData Get Error, exc=%s, host=%s, type=%s, obj=%s, p_dttm=%s, p_val=%s"%( str(e), _hname, _i_type, _obj, str(_p_dttm_str), str(_p_1_diff_str) ))
                                            logger.exception(e)
                                            _r_urate = flib.RES_FA
                                            _e_urate = flib.ERR_INV_PRV_VAL
                    except Exception as e:
                        logger.error("FAIL: PrevData Get Error, exc=%s, host=%s, type=%s, obj=%s, p_dttm=%s, p_val=%s"%( str(e), _hname, _i_type, _obj, str(_p_dttm_str), str(_p_val_str) ))
                        logger.exception(e)
                        _e_bps, _e_util, _e_urate = [flib.ERR_INV_PRV_VAL, flib.ERR_INV_PRV_VAL, flib.ERR_INV_ORG_VAL]

                _iv_dict[_i_key] = [_c_dttm_str, _mon_bytes, _new_1_diff]
            SQL_DATA_Q.append( DBParam(_c_dttm_str, _rg, _hname, _steps_b[0], _steps_b[1], _steps_b[2], _steps_b[3], _obj, _v_bps, None, _atype, _r_bps, _e_bps) )
            if _has_speed : SQL_DATA_Q.append( DBParam(_c_dttm_str, _rg, _hname, _steps_u[0], _steps_u[1], _steps_u[2], _steps_u[3], _obj, _v_util, None, _atype, _r_util, _e_util) )
            if _has_speed : SQL_DATA_Q.append( DBParam(_c_dttm_str, _rg, _hname, _steps_r[0], _steps_r[1], _steps_r[2], _steps_r[3], _obj, _v_urate, None, _atype, _r_urate, _e_urate) )


    ## save data
    flib.save_item_val(DEF_DAT_DIR, _hname, _file_type, _iv_dict)

    return

def _get_port_packet(_c_dttm_str, _rg, _hname, _ikey, _data):
    '''
    em4:rx_packets:0
    em4:tx_packets:0
    p1p1:rx_packets:514363
    p1p1:tx_packets:1
    -------
    eth0:rx_packets:1037060549
    eth0:tx_packets:1024961574
    eth1:rx_packets:73010428
    eth1:tx_packets:51577847
    '''
    _res, _steps_rxp, _err = get_steps(_ikey, flib.ST3_RX_PPS)
    if not _res :
        logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _data, _err ))
        return

    _res, _steps_txp, _err = get_steps(_ikey, flib.ST3_TX_PPS)
    if not _res :
        logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _data, _err ))
        return

    _file_type = "packet"
    _atype = flib.ATYPE_RANGE
    _c_dttm = parser.parse(_c_dttm_str)

    ## parse curr data
    _obj_bytes = {}
    for _ll in str(_data).splitlines():
        _lparts = _ll.split(":")
        if len(_lparts) < 3:
            logger.error("FAIL: RawData Parse Error, Invalid Port Packet Data, host=%s, line=%s"%( _hname, str(_ll) ))
            continue

        _obj = str(_lparts[0]).strip()
        _type = str(_lparts[1]).strip()
        _val = str(_lparts[2]).strip()

        if _obj == "" :
            logger.error("FAIL: RawData Parse Error, No Port Packet Obj, host=%s, line=%s"%( _hname, _ll ))
            continue

        if _type == "rx_packets":
            if not _type in _obj_bytes : _obj_bytes[_type] = {}
            try:
                _obj_bytes[_type][_obj] = round(float( _val ), 2)
            except Exception as e:
                _obj_bytes[_type][_obj] = None
                logger.error("FAIL: RawData Parse Error, Invalid Port Value, exc=%s, host=%s, type=%s, val=%s, line=%s"%( str(e), _hname, _type, str(_val), str(_ll) ))
        elif _type == "tx_packets":
            if not _type in _obj_bytes : _obj_bytes[_type] = {}
            try:
                _obj_bytes[_type][_obj] = round(float( _val ), 2)
            except Exception as e:
                _obj_bytes[_type][_obj] = None
                logger.error("FAIL: RawData Parse Error, Invalid Port Value, exc=%s, host=%s, type=%s, val=%s, line=%s"%( str(e), _hname, _type, str(_val), str(_ll) ))
        else:
            logger.warning("FAIL: RawData Parse Error, Invalid Port Packet Type, host=%s, line=%s"%( _hname, str(_ll) ))
            continue


    ## load prev_data
    # type + "$$$$" + obj : dttm, data, 1-diff
    _iv_dict = flib.load_item_val(DEF_DAT_DIR, _hname, _file_type)

    ## calc value
    for _i_type in _obj_bytes :
        if _i_type == "rx_packets" :
            _steps_p = _steps_rxp
        else:
            _steps_p = _steps_txp

        for _obj in _obj_bytes[_i_type]:
            _v_pps  = None
            _r_pps = flib.RES_FA
            _e_pps = None

            _i_key = _i_type+ "$$$$" + _obj
            _mon_pkts = _obj_bytes[_i_type][_obj]

            if _mon_pkts == None :
                _e_pps = flib.ERR_INV_VAL
            else:
                ## for data
                _new_1_diff = None

                if not _i_key in _iv_dict :
                    logger.error("FAIL: No PrevData, host=%s, type=%s, obj=%s"%( _hname, _i_type, _obj ))
                    _e_pps = flib.ERR_NO_PRV_VAL
                else:
                    _p_dttm_str, _p_val_str, _p_1_diff_str =_iv_dict[_i_key]
                    try:
                        _gap_dttm =  (_c_dttm - parser.parse(_p_dttm_str)).total_seconds()
                        _gap_pkts = _mon_pkts - float(_p_val_str)
                        if _gap_dttm > DEF_MAX_DTTM_GAP :
                            logger.error("FAIL: Mon Value Get Error, Too Old Prv Value, host=%s, type=%s, obj=%s, val=%s, prv_dttm=%s"%( _hname, _i_type, _obj, str(_mon_pkts), _p_dttm_str ))
                            _e_pps = flib.ERR_INV_PRV_VAL
                        else:
                            if _gap_pkts < 0 :
                                logger.error("FAIL: Mon Value Get Error, Minus Value Gap, host=%s, type=%s, obj=%s, gap_v=%s"%( _hname, _i_type, _obj, str(_gap_pkts)) )
                                _e_pps = flib.ERR_MNS_VAL
                            else:
                                _v_pps = round(_gap_pkts/_gap_dttm, 2)
                                _r_pps = flib.RES_SC
                                _new_1_diff = _v_pps
                    except Exception as e:
                        logger.error("FAIL: PrevData Get Error, exc=%s, host=%s, type=%s, obj=%s, p_dttm=%s, p_val=%s"%( str(e), _hname, _i_type, _obj, str(_p_dttm_str), str(_p_val_str) ))
                        logger.exception(e)
                        _e_pps = flib.ERR_INV_PRV_VAL

                _iv_dict[_i_key] = [_c_dttm_str, _mon_pkts, _new_1_diff]
            SQL_DATA_Q.append( DBParam(_c_dttm_str, _rg, _hname, _steps_p[0], _steps_p[1], _steps_p[2], _steps_p[3], _obj, _v_pps, None, _atype, _r_pps, _e_pps) )


    ## save data
    flib.save_item_val(DEF_DAT_DIR, _hname, _file_type, _iv_dict)

    return

def _get_port_error(_c_dttm_str, _rg, _hname, _ikey, _data):
    '''
    eno4:rx_errors:0
    eno4:tx_errors:0
    ens1f0:rx_errors:0
    ens1f0:tx_errors:0
    ens1f1:rx_errors:0
    ens1f1:tx_errors:0
    ----------
    eth0:rx_errors:0
    eth0:tx_errors:0
    eth1:rx_errors:0
    eth1:tx_errors:0
    '''
    _res, _steps_rxe, _err = get_steps(_ikey, flib.ST3_RX_ERR_PPS)
    if not _res :
        logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _data, _err ))
        return

    _res, _steps_txe, _err = get_steps(_ikey, flib.ST3_TX_ERR_PPS)
    if not _res :
        logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _data, _err ))
        return

    _file_type = "error"
    _atype = flib.ATYPE_RANGE
    _c_dttm = parser.parse(_c_dttm_str)

    ## parse curr data
    _obj_bytes = {}
    for _ll in str(_data).splitlines():
        _lparts = _ll.split(":")
        if len(_lparts) < 3:
            logger.error("FAIL: RawData Parse Error, Invalid Port Error Data, host=%s, line=%s"%( _hname, str(_ll) ))
            continue

        _obj = str(_lparts[0]).strip()
        _type = str(_lparts[1]).strip()
        _val = str(_lparts[2]).strip()

        if _obj == "" :
            logger.error("FAIL: RawData Parse Error, No Port Error Obj, host=%s, line=%s"%( _hname, _ll ))
            continue

        if _type == "rx_errors":
            if not _type in _obj_bytes : _obj_bytes[_type] = {}
            try:
                _obj_bytes[_type][_obj] = round(float( _val ), 2)
            except Exception as e:
                _obj_bytes[_type][_obj] = None
                logger.error("FAIL: RawData Parse Error, Invalid Port Value, exc=%s, host=%s, type=%s, val=%s, line=%s"%( str(e), _hname, _type, str(_val), str(_ll) ))
        elif _type == "tx_errors":
            if not _type in _obj_bytes : _obj_bytes[_type] = {}
            try:
                _obj_bytes[_type][_obj] = round(float( _val ), 2)
            except Exception as e:
                _obj_bytes[_type][_obj] = None
                # logger.error("FAIL: RawData Parse Error, Invalid Port Value, exc=%s, host=%s, type=%s, val=%s, key=%s, line=%s"%( str(e), _hname, _type, str(_val), str(_ll) ))
                logger.error("FAIL: RawData Parse Error, Invalid Port Value, exc=%s, host=%s, type=%s, val=%s, line=%s"%( str(e), _hname, _type, str(_val), str(_ll) ))

        else:
            logger.warning("FAIL: RawData Parse Error, Invalid Port Error Type, host=%s, line=%s"%( _hname, str(_ll) ))
            continue


    ## load prev_data
    # type + "$$$$" + obj : dttm, data, 1-diff
    _iv_dict = flib.load_item_val(DEF_DAT_DIR, _hname, _file_type)

    ## calc value
    for _i_type in _obj_bytes :
        if _i_type == "rx_errors" :
            _steps_e = _steps_rxe
        else:
            _steps_e = _steps_txe

        for _obj in _obj_bytes[_i_type]:
            _v_e_pps  = None
            _r_e_pps = flib.RES_FA
            _e_e_pps = None

            _i_key = _i_type+ "$$$$" + _obj
            _mon_errs = _obj_bytes[_i_type][_obj]

            if _mon_errs == None :
                _e_e_pps = flib.ERR_INV_VAL
            else:
                ## for data
                _new_1_diff = None

                if not _i_key in _iv_dict :
                    logger.error("FAIL: No PrevData, host=%s, type=%s, obj=%s"%( _hname, _i_type, _obj ))
                    _e_e_pps = flib.ERR_NO_PRV_VAL
                else:
                    _p_dttm_str, _p_val_str, _p_1_diff_str =_iv_dict[_i_key]
                    try:
                        _gap_dttm =  (_c_dttm - parser.parse(_p_dttm_str)).total_seconds()
                        _gap_errs = _mon_errs - float(_p_val_str)
                        if _gap_dttm > DEF_MAX_DTTM_GAP :
                            logger.error("FAIL: Mon Value Get Error, Too Old Prv Value, host=%s, type=%s, obj=%s, val=%s, prv_dttm=%s"%( _hname, _i_type, _obj, str(_mon_errs), _p_dttm_str ))
                            _e_e_pps = flib.ERR_INV_PRV_VAL
                        else:
                            if _gap_errs < 0 :
                                logger.error("FAIL: Mon Value Get Error, Minus Value Gap, host=%s, type=%s, obj=%s, gap_v=%s"%( _hname, _i_type, _obj, str(_gap_errs)) )
                                _e_e_pps = flib.ERR_MNS_VAL
                            else:
                                _v_e_pps = round(_gap_errs/_gap_dttm, 2)
                                _r_e_pps = flib.RES_SC
                                _new_1_diff = _v_e_pps
                    except Exception as e:
                        logger.error("FAIL: PrevData Get Error, exc=%s, host=%s, type=%s, obj=%s, p_dttm=%s, p_val=%s"%( str(e), _hname, _i_type, _obj, str(_p_dttm_str), str(_p_val_str) ))
                        logger.exception(e)
                        _e_e_pps = flib.ERR_INV_PRV_VAL

                _iv_dict[_i_key] = [_c_dttm_str, _mon_errs, _new_1_diff]
            SQL_DATA_Q.append( DBParam(_c_dttm_str, _rg, _hname, _steps_e[0], _steps_e[1], _steps_e[2], _steps_e[3], _obj, _v_e_pps, None, _atype, _r_e_pps, _e_e_pps) )


    ## save data
    flib.save_item_val(DEF_DAT_DIR, _hname, _file_type, _iv_dict)

    return

def _get_port_drop(_c_dttm_str, _rg, _hname, _ikey, _data):
    '''
    em4:rx_dropped:0
    em4:tx_dropped:0
    p1p1_0:rx_dropped:0
    p1p1_0:tx_dropped:0
    p1p1:rx_dropped:0
    p1p1:tx_dropped:0
    -----------
    eth0:rx_dropped:0
    eth0:tx_dropped:0
    eth1:rx_dropped:0
    eth1:tx_dropped:0
    eth2:rx_dropped:0
    eth2:tx_dropped:0
    eth3:rx_dropped:0
    eth3:tx_dropped:0
    '''
    _res, _steps_rxd, _err = get_steps(_ikey, flib.ST3_RX_DROP_PPS)
    if not _res :
        logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _data, _err ))
        return

    _res, _steps_txd, _err = get_steps(_ikey, flib.ST3_TX_DROP_PPS)
    if not _res :
        logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _data, _err ))
        return

    _file_type = "drop"
    _atype = flib.ATYPE_RANGE
    _c_dttm = parser.parse(_c_dttm_str)

    ## parse curr data
    _obj_bytes = {}
    for _ll in str(_data).splitlines():
        _lparts = _ll.split(":")
        if len(_lparts) < 3:
            logger.error("FAIL: RawData Parse Error, Invalid Port Error Data, host=%s, line=%s"%( _hname, str(_ll) ))
            continue

        _obj = str(_lparts[0]).strip()
        _type = str(_lparts[1]).strip()
        _val = str(_lparts[2]).strip()

        if _obj == "" :
            logger.error("FAIL: RawData Parse Error, No Port Error Obj, host=%s, line=%s"%( _hname, _ll ))
            continue

        if _type == "rx_dropped":
            if not _type in _obj_bytes : _obj_bytes[_type] = {}
            try:
                _obj_bytes[_type][_obj] = round(float( _val ), 2)
            except Exception as e:
                _obj_bytes[_type][_obj] = None
                logger.error("FAIL: RawData Parse Error, Invalid Port Value, exc=%s, host=%s, type=%s, val=%s, line=%s"%( str(e), _hname, _type, str(_val), str(_ll) ))
        elif _type == "tx_dropped":
            if not _type in _obj_bytes : _obj_bytes[_type] = {}
            try:
                _obj_bytes[_type][_obj] = round(float( _val ), 2)
            except Exception as e:
                _obj_bytes[_type][_obj] = None
                # logger.error("FAIL: RawData Parse Error, Invalid Port Value, exc=%s, host=%s, type=%s, val=%s, key=%s, line=%s"%( str(e), _hname, _type, str(_val), str(_ll) ))
                logger.error("FAIL: RawData Parse Error, Invalid Port Value, exc=%s, host=%s, type=%s, val=%s, line=%s"%( str(e), _hname, _type, str(_val), str(_ll) ))

        else:
            logger.warning("FAIL: RawData Parse Error, Invalid Port Drop Type, host=%s, line=%s"%( _hname, str(_ll) ))
            continue


    ## load prev_data
    # type + "$$$$" + obj : dttm, data, 1-diff
    _iv_dict = flib.load_item_val(DEF_DAT_DIR, _hname, _file_type)

    ## calc value
    for _i_type in _obj_bytes :
        if _i_type == "rx_dropped" :
            _steps_d = _steps_rxd
        else:
            _steps_d = _steps_txd

        for _obj in _obj_bytes[_i_type]:
            _v_d_pps  = None
            _r_d_pps = flib.RES_FA
            _e_d_pps = None

            _i_key = _i_type+ "$$$$" + _obj
            _mon_drps = _obj_bytes[_i_type][_obj]

            if _mon_drps == None :
                _e_d_pps = flib.ERR_INV_VAL
            else:
                ## for data
                _new_1_diff = None

                if not _i_key in _iv_dict :
                    logger.error("FAIL: No PrevData, host=%s, type=%s, obj=%s"%( _hname, _i_type, _obj ))
                    _e_d_pps = flib.ERR_NO_PRV_VAL
                else:
                    _p_dttm_str, _p_val_str, _p_1_diff_str =_iv_dict[_i_key]
                    try:
                        _gap_dttm =  (_c_dttm - parser.parse(_p_dttm_str)).total_seconds()
                        _gap_drps = _mon_drps - float(_p_val_str)
                        if _gap_dttm > DEF_MAX_DTTM_GAP :
                            logger.error("FAIL: Mon Value Get Error, Too Old Prv Value, host=%s, type=%s, obj=%s, val=%s, prv_dttm=%s"%( _hname, _i_type, _obj, str(_mon_drps), _p_dttm_str ))
                            _e_d_pps = flib.ERR_INV_PRV_VAL
                        else:
                            if _gap_drps < 0 :
                                logger.error("FAIL: Mon Value Get Error, Minus Value Gap, host=%s, type=%s, obj=%s, gap_v=%s"%( _hname, _i_type, _obj, str(_gap_drps)) )
                                _e_d_pps = flib.ERR_MNS_VAL
                            else:
                                _v_d_pps = round(_gap_drps/_gap_dttm, 2)
                                _r_d_pps = flib.RES_SC
                                _new_1_diff = _v_d_pps
                    except Exception as e:
                        logger.error("FAIL: PrevData Get Error, exc=%s, host=%s, type=%s, obj=%s, p_dttm=%s, p_val=%s"%( str(e), _hname, _i_type, _obj, str(_p_dttm_str), str(_p_val_str) ))
                        logger.exception(e)
                        _e_d_pps = flib.ERR_INV_PRV_VAL

                _iv_dict[_i_key] = [_c_dttm_str, _mon_drps, _new_1_diff]
            SQL_DATA_Q.append( DBParam(_c_dttm_str, _rg, _hname, _steps_d[0], _steps_d[1], _steps_d[2], _steps_d[3], _obj, _v_d_pps, None, _atype, _r_d_pps, _e_d_pps) )


    ## save data
    flib.save_item_val(DEF_DAT_DIR, _hname, _file_type, _iv_dict)

    return


def get_port_info(_ssh, _rg, _hname):
    _cmd_byte = """
for _ifname in `command grep PCI_SLOT_NAME /sys/class/net/*/device/uevent | awk -F '/' '{print $5}'`;
do
  echo -n "$_ifname:rx_bytes:";
  cat /sys/class/net/$_ifname/statistics/rx_bytes 2>&1 ;

  echo -n "$_ifname:tx_bytes:";
  cat /sys/class/net/$_ifname/statistics/tx_bytes 2>&1 ;

  echo -n "$_ifname:speed:";
  cat /sys/class/net/$_ifname/speed 2>&1 ;
done;"""
    _cmd_packet = """
for _ifname in `command grep PCI_SLOT_NAME /sys/class/net/*/device/uevent | awk -F '/' '{print $5}'`;
do
  echo -n "$_ifname:rx_packets:";
  cat /sys/class/net/$_ifname/statistics/rx_packets;

  echo -n "$_ifname:tx_packets:";
  cat /sys/class/net/$_ifname/statistics/tx_packets;
done;"""
    _cmd_error = """
for _ifname in `command grep PCI_SLOT_NAME /sys/class/net/*/device/uevent | awk -F '/' '{print $5}'`;
do
  echo -n "$_ifname:rx_errors:";
  cat /sys/class/net/$_ifname/statistics/rx_errors 2>&1 ;

  echo -n "$_ifname:tx_errors:";
  cat /sys/class/net/$_ifname/statistics/tx_errors 2>&1 ;
done;"""
    _cmd_drop = """
for _ifname in `command grep PCI_SLOT_NAME /sys/class/net/*/device/uevent | awk -F '/' '{print $5}'`;
do
  echo -n "$_ifname:rx_dropped:"
  cat /sys/class/net/$_ifname/statistics/rx_dropped 2>&1 ;

  echo -n "$_ifname:tx_dropped:"
  cat /sys/class/net/$_ifname/statistics/tx_dropped 2>&1 ;
done;"""

    _cmd_chk = """ ls -al /sys/class/net/*/statistics """
    _cmd_list = [
                 (_cmd_byte, _cmd_chk, flib.PKEY_PORT_BYTE_STAT), # byte
                 (_cmd_packet, _cmd_chk, flib.PKEY_PORT_PACKET_STAT), # packet
                (_cmd_error, _cmd_chk, flib.PKEY_PORT_ERROR_STAT), # error
                (_cmd_drop, _cmd_chk, flib.PKEY_PORT_DROP_STAT)  # drop
                 ]

    for _cmd1, _cmd2, _ckey in _cmd_list:
        try:
            _res, _cmd_body = run_ssh_cmd(_ssh, _cmd1)
            if not _res :
                _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
                logger.error("FAIL: SSH CMD RUN Error, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

            if _cmd_body == "":
                _res = False
                _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
                logger.error("FAIL: SSH CMD No-Return, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

            _c_dttm = str(datetime.now())
            _conn_key = "%s.%s"%( flib.PKEY_CMS_SVR, _ckey )
            _conn_cmd = _cmd1.strip()
            _conn_cmd_res = _res
            _conn_cmd_body = _cmd_body

            ## Raw Logging
            RAW_DATA_Q.append( RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body) )

            ## DB
            if _ckey == flib.PKEY_PORT_BYTE_STAT :
                _get_port_byte(_c_dttm, _rg, _hname, _conn_key, _conn_cmd_body)
            elif _ckey == flib.PKEY_PORT_PACKET_STAT :
                _get_port_packet(_c_dttm, _rg, _hname, _conn_key, _conn_cmd_body)
            elif _ckey == flib.PKEY_PORT_ERROR_STAT :
                _get_port_error(_c_dttm, _rg, _hname, _conn_key, _conn_cmd_body)
            elif _ckey == flib.PKEY_PORT_DROP_STAT :
                _get_port_drop(_c_dttm, _rg, _hname, _conn_key, _conn_cmd_body)
            else:
                logger.error("FAIL: Invalid Port Info, host=%s(%s), ckey=%s, body=%s"%( _hname, _rg, _ckey, str(_conn_cmd_body) ))
        except Exception as e:
            logger.exception(e)
            logger.error("FAIL: Port-Info GET Error, host=%s(%s), cmd=%s, key=%s, exc=%s"%( str(_hname), _rg, str(_cmd1), str(_ckey), str(e) ))
    return

def _get_temp_info(_ssh, _rg, _hname):
    try:
        _cmd1 = """ command grep -H '' /sys/class/hwmon/hwmon*/temp1_input | awk -F '/' '{print $5":"$6}' | awk -F ':' '{print $1":"$3}' """
        _cmd2 = """ cat /sys/class/hwmon/hwmon*/temp1_input """
        _res, _cmd_body = run_ssh_cmd(_ssh, _cmd1)
        if not _res :
            _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
            logger.error("FAIL: SSH CMD RUN Error, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

        if _cmd_body == "":
            _res = False
            _chk_res, _chK_ret = run_ssh_cmd(_ssh, _cmd2)
            logger.error("FAIL: SSH CMD No-Return, host=%s(%s), cmd=%s, err=%s, chk=%s"%( str(_hname), _rg, _cmd1, str(_cmd_body), str(_chK_ret) ))

        _c_dttm = str(datetime.now())
        _conn_key = "%s.%s"%( flib.PKEY_CMS_SVR, flib.PKEY_TEMP_DEGREE_HWMON)
        _conn_obj = ""
        _conn_cmd = _cmd1.strip()
        _conn_cmd_res = _res
        _conn_cmd_body = _cmd_body

        ## Raw Logging
        RAW_DATA_Q.append( RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body) )

        ## DB
        _res, _st, _err = get_steps(_conn_key, flib.ST3_DEGREE)
        if not _res :
            logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
            return
        _atype = get_alert_type(*_st)

        for _ll in str(_conn_cmd_body).splitlines():
            _lparts = _ll.split(":")
            if len(_lparts) < 2:
                logger.error("FAIL: RawData Parse Error, Invalid Temp Data, host=%s, line=%s"%( _hname, str(_ll) ))
                continue

            _obj = str(_lparts[0]).strip()
            _val = str(_lparts[1]).strip()
            if _obj == "" :
                logger.error("FAIL: RawData Parse Error, No Temp Obj, host=%s, line=%s"%( _hname, _ll ))
                continue

            try:
                _mon_val = round(float(_val)/1000, 2)
                SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _obj, _mon_val, None, _atype, flib.RES_SC) )
            except Exception as e:
                SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _obj, _mon_val, None, _atype, flib.RES_FA, flib.ERR_INV_VAL) )
                logger.error("FAIL: RawData Parse Error, Invalid Temp Value, exc=%s, val=%s, key=%s, line=%s"%( str(e), str(_val), _conn_key, str(_ll) ))
                continue

        return
    except Exception as e:
        logger.exception(e)
        logger.error("FAIL: Temp-Info GET Error, host=%s(%s), exc=%s"%( str(_hname), _rg, str(e) ))
        return

def get_temp_info(_ssh, _rg, _hname):
    try:
        _cmd1 = '''ipmitool sdr | grep degree'''
        _res, _cmd_body = run_ssh_cmd(_ssh, _cmd1)
        if not _res or not _cmd_body:
            logger.error("FAIL: SSH CMD RUN Error/No-Return host={}({}) cmd={} res={} ret={}".format( str(_hname), _rg, _cmd1, _res, str(_cmd_body) ))
            _res = False

        _c_dttm = str(datetime.now())
        _conn_key = "%s.%s"%( flib.PKEY_CMS_SVR, flib.PKEY_TEMP_DEGREE_IPMI)
        _conn_obj = ""
        _conn_cmd = _cmd1.strip()
        _conn_cmd_res = _res
        _conn_cmd_body = _cmd_body

        ## Raw Logging
        RAW_DATA_Q.append( RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body) )

        ## DB
        _res, _st, _err = get_steps(_conn_key, flib.ST3_DEGREE)
        if not _res :
            logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
            return
        _atype = get_alert_type(*_st)

        if not _conn_cmd_res:
            #SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], '', None, None, _atype, flib.RES_FA, flib.ERR_INV_VAL) )
            return

        #print('hname={} body={}'.format(_hname, _conn_cmd_body))
        _obj = None
        _dgree = None
        _tdgree = 0
        for _ll in str(_conn_cmd_body).splitlines():
            #0                     1                   2
            #01-02-CPU 1         | 40 degrees C      | ok FOR HP
            #Temp                | xxx               | ok FOR DELL
            _lparts = _ll.split("|")
            _l0 = _lparts[0].strip()
            if len(_lparts) >= 3 and ('CPU' in _l0 or _l0 == 'Temp'):
                _obj = _l0
                _l1  = _lparts[1].strip().split()
                #0  1       2
                #40 degrees C
                _dgree = int(_l1[0])
                if 'CPU' in _obj:
                    #print('hname={} obj={} dgree={} FOR HP'.format(_hname, _obj, _dgree))
                    SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _obj, _dgree, None, _atype, flib.RES_SC) )
                else:
                    #print('d={} < {} FOR DELL'.format(_tdgree, _dgree))
                    if _tdgree < _dgree:
                        _tdgree = _dgree
        if _obj == 'Temp':
            #print('hname={} obj={} dgree={} FOR DELL'.format(_hname, _obj, _tdgree))
            SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _obj, _tdgree, None, _atype, flib.RES_SC) )

        return
    except Exception as e:
        logger.exception(e)
        logger.error("FAIL: Temp-Info GET Error, host=%s(%s), exc=%s"%( str(_hname), _rg, str(e) ))
        return

def get_storage_status(_ssh, _rg, _hname):
    try:
        '''
        iscsiadm -m node
        128.2.111.203:3260,-1 iqn.1992-04.com.emc:cx.ckm00183101822.b2
        128.2.111.212:3260,-1 iqn.1992-04.com.emc:cx.ckm00184400809.a2
        '''
        ## st status
        _cmd1 = 'iscsiadm -m node'
        _res, _cmd_body = run_ssh_cmd(_ssh, _cmd1)
        if not _res or not _cmd_body:
            logger.error("FAIL: SSH CMD RUN Error, host={}({}), cmd={}, err={}".format( str(_hname), _rg, _cmd1, str(_cmd_body) ))

        _c_dttm = str(datetime.now())
        _conn_key = '{}.{}'.format( flib.PKEY_CMS_SVR, flib.PKEY_STORAGE_STATUS )
        #cmn.svr.storage.status.show
        _conn_obj = ""
        _conn_cmd = _cmd1.strip()
        _conn_cmd_res = _res
        _conn_cmd_body = _cmd_body

        ## Raw Logging
        RAW_DATA_Q.append( RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body) )

        _res, _st, _err = get_steps(_conn_key)
        #svr > st > status
        if not _res :
            logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
            return
        _atype = get_alert_type(*_st)

        ha_c = 0
        ha_s = 'nok'
        for _ll in str(_conn_cmd_body).splitlines():
            #0            1                           2
            #128.2.111.203:3260,-1 iqn.1992-04.com.emc:cx.ckm00183101822.b2
            _lparts = _ll.split(':')
            if len(_lparts) > 2:
                ha_c += 1
            else:
                logger.error("FAIL: RawData Parse Error, No Obj, host=%s, data=%s"%( _hname, _ll ))

        if ha_c >= 2:
            ha_s = 'ok'
        SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], 'storage.ha.status', None, ha_s, _atype, flib.RES_SC) )

        return
    except Exception as e:
        logger.exception(e)
        logger.error("FAIL: Port GET Error, host=%s(%s), exc=%s"%( str(_hname), _rg, str(e) ))
        return

def get_storage_link(_ssh, _rg, _hname):
    try:
        '''
        PING 128.2.111.203 (128.2.111.203) 56(84) bytes of data.
        64 bytes from 128.2.111.203: icmp_seq=1 ttl=64 time=0.186 ms
        64 bytes from 128.2.111.203: icmp_seq=2 ttl=64 time=0.146 ms
        64 bytes from 128.2.111.203: icmp_seq=3 ttl=64 time=0.164 ms

        --- 128.2.111.203 ping statistics ---
        3 packets transmitted, 3 received, 0% packet loss, time 2000ms
        rtt min/avg/max/mdev = 0.146/0.165/0.186/0.019 ms
        PING 128.2.111.212 (128.2.111.212) 56(84) bytes of data.
        64 bytes from 128.2.111.212: icmp_seq=1 ttl=64 time=0.157 ms
        64 bytes from 128.2.111.212: icmp_seq=2 ttl=64 time=0.164 ms
        64 bytes from 128.2.111.212: icmp_seq=3 ttl=64 time=0.155 ms

        --- 128.2.111.212 ping statistics ---
        3 packets transmitted, 3 received, 0% packet loss, time 2000ms
        rtt min/avg/max/mdev = 0.155/0.158/0.164/0.015 ms
        ----------------------------

        PING 128.2.111.20 (128.2.111.20) 56(84) bytes of data.
        From 128.2.111.62 icmp_seq=1 Destination Host Unreachable
        From 128.2.111.62 icmp_seq=2 Destination Host Unreachable
        From 128.2.111.62 icmp_seq=3 Destination Host Unreachable

        --- 128.2.111.20 ping statistics ---
        3 packets transmitted, 0 received, +3 errors, 100% packet loss, time 2000ms
        pipe 3
        '''
        ## st status
        _cmd1 = """for cmd in `iscsiadm -m node | awk -F: '{print $1}'`; do ping -c3 $cmd; done"""
        _res, _cmd_body = run_ssh_cmd(_ssh, _cmd1)
        if not _res or not _cmd_body:
            logger.error("FAIL: SSH CMD RUN Error, host={}({}), cmd={}, err={}".format( str(_hname), _rg, _cmd1, str(_cmd_body) ))

        _c_dttm = str(datetime.now())
        _conn_key = '{}.{}'.format( flib.PKEY_CMS_SVR, flib.PKEY_STORAGE_LINK )
        #cmn.svr.storage.link.show
        _conn_obj = ""
        _conn_cmd = _cmd1.strip()
        _conn_cmd_res = _res
        _conn_cmd_body = _cmd_body

        ## Raw Logging
        RAW_DATA_Q.append( RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body) )

        _res, _st, _err = get_steps(_conn_key)
        #svr > st > link
        if not _res :
            logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s"%( _hname, _conn_cmd_body, _err ))
            return
        _atype = get_alert_type(*_st)

        ha_e = 0
        ha_s = 'up'
        for _ll in str(_conn_cmd_body).splitlines():
            g = re.search('[0-9] received,', _ll)
            if g:
                #0 1       2            3 4         5  6
                #3 packets transmitted, 3 received, 0% packet loss, time 2000ms
                #3 packets transmitted, 0 received, +3 errors, 100% packet loss, time 2000ms
                if 'errors' in _ll:
                    ha_e += 1
                    break
        if ha_e > 0:
            ha_s = 'down'
        SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], 'storage.ha.link', None, ha_s, _atype, flib.RES_SC) )

        return;

    except Exception as e:
        logger.exception(e)
        logger.error("FAIL: Port-Status Get Exception, host=%s(%s), exc=%s"%( str(_hname), _rg, str(e) ))


def get_proc_status(_ssh, _rg, _hname, _proclist):
    """
    ps -ef | awk '$3 !~ /^2$/ {print}'

    UID        PID  PPID  C STIME TTY          TIME CMD
    root         1     0  0 Jan18 ?        00:01:51 /usr/lib/systemd/systemd --switched-root --system --deserialize 21
    root         2     0  0 Jan18 ?        00:00:03 [kthreadd]
    root         3     2  0 Jan18 ?        00:02:37 [ksoftirqd/0]
    $1          $2    $3 $4    $5  $6      $7        $8

    정규표현식 /^2$/ 2로 시작하여 2로 끝나는 한 글자
    ~ 특정 레코드나 필드 내에서 일치하는 정규 표현식 패턴 검사
    ! 부정
    $3이 2로 끝나지 않는 줄을 출력
    """
    try:
        print("_proclist::{}".format(_proclist))
        _cmd1 = """ps -ef | awk '$3 !~ /^2$/ {print}' """
        _cmd2 = """ps -ef """
        _res, _cmd_body = run_ssh_cmd(_ssh, _cmd1)

        if not _res:
            _chk_res, _chk_ret = run_ssh_cmd(_ssh, _cmd2)
            # logger.error("FAIL: SSH RUN-Error, host=%s(%s), cmd=%s, err=%s, chk=%s"%(
            #             str(_hname), _rg, _cmd1, str(_cmd_body), str(_chk_ret)
            # ))
            print("FAIL: SSH RUN-Error, host=%s(%s), cmd=%s, err=%s, chk=%s"%(
                        str(_hname), _rg, _cmd1, str(_cmd_body), str(_chk_ret)
            ))

        if _cmd_body == "":
            _res = False
            _chk_res, _chk_ret = run_ssh_cmd(_ssh, _cmd2)
            # logger.error("FAIL: SSH CMD No-Return, host=%s(%s), cmd=%s, err=%s, chk=%s"%(
            #             str(_hname), _rg, _cmd1, str(_cmd_body), str(_chk_ret)
            # ))
            print("FAIL: SSH CMD No-Return, host=%s(%s), cmd=%s, err=%s, chk=%s"%(
                        str(_hname), _rg, _cmd1, str(_cmd_body), str(_chk_ret)
            ))

        _c_dttm = str(datetime.now())
        _conn_key = "{}.{}".format(flib.PKEY_CMS_SVR, flib.PKEY_PROC_STATUS_PS)
        _conn_cmd = _cmd1.strip()
        _conn_cmd_res = _res
        _conn_cmd_body = _cmd_body

        ## Raw Logging
        # RAW_DATA_Q.append(RawLog(_c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body))
        # print("RAW::_c_dttm:{}, _rg:{}, _hname:{}, _conn_key:{}, _conn_cmd:{}, _conn_cmd_res:{}, _conn_cmd_body:{}".format(
        #         _c_dttm, _rg, _hname, _conn_key, _conn_cmd, _conn_cmd_res, _conn_cmd_body
        # ))

        _sres, _st, _serr = get_steps(_conn_key)
        if not _sres:
            # logger.error("FAIL: Step Get Error, host=%s, val=%s, err=%s" % (_hname, _conn_cmd_body, _serr))
            print("FAIL: Step Get Error, host=%s, val=%s, err=%s" % (_hname, _conn_cmd_body, _serr))
            return

        _atype = get_alert_type(*_st)

        for _proc in _proclist:
            _stat_txt = 'off'
            for _ll in _conn_cmd_body.splitlines():
                if _proclist[_proc] in _ll:
                    print("\n\n_ll||_proclist[_proc]::{}||{}\n\n".format(_ll, _proclist[_proc]))
                # if _proclist[_proc] in _ll:
                    _stat_txt = 'on'
                    break

            print('hname={} proc={} stat={}'.format(_hname, _proc, _stat_txt))
            SQL_DATA_Q.append( DBParam(_c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _proc, None, _stat_txt, _atype, flib.RES_SC) )
            print("_c_dttm:{}, _rg:{}, _hname,:{}, _st[0]:{}, _st[1]:{}, _st[2]:{}, _st[3]:{}, _proc:{}, None:{}, _stat_txt:{}, _atype:{}, flib.RES_SC:{}".format(
                    _c_dttm, _rg, _hname, _st[0], _st[1], _st[2], _st[3], _proc, None, _stat_txt, _atype, flib.RES_SC
            ))

    except Exception as e:
        # logger.exception(e)
        # logger.error("FAIL: PROC Status GET Error, host=%s(%s), exc=%s"%(str(_hname), _rg, str(e)) )
        print("FAIL: PROC Status GET Error, host=%s(%s), exc=%s"%(str(_hname), _rg, str(e)))




class SshPxy(threading.Thread):

    def __init__(self, _items, _rg, _sname, _sip, _id, _pw, _extras, _port=22, _sshto=10):
        threading.Thread.__init__(self)
        self.item_list = _items
        self.region = _rg
        self.hname=_sname
        self.hip = _sip
        self.hid = _id
        self.hpw = _pw
        self.hport = 22 if _port == None else _port
        self.ssh_to = _sshto
        self._runtime = None
        self._item_delay = {}
        self._done = False
        self.proclist = _extras['proclist']     ## for TEST [sshd]
                                                ## CC:: conn_info_v3.yaml > cms.conn.proclist=[ssh daemon]

    def return_runtime(self):
        return self._runtime

    def return_item_delay(self):
        return self._item_delay

    def is_done(self):
        return self._done

    def run(self):
#         from time import sleep
#         sleep(5)
#         logger.debug(self.hname)
        global SQL_DATA_Q
        _tprv = datetime.now()
        _client = None
        _item = INAME_A_SSH
        try:
            _prv = datetime.now()

            ## ssh config
            _client = paramiko.SSHClient()
            _client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            for _i in range(5):
                try:
                    _client.load_system_host_keys()
                    break
                except Exception as re:
                    # logger.warning("FAIL: %s SSH System Key Get Error, e=%s, retry=%s"%( _LNM, str(re), str(_i+1) ))
                    sleep(1)

            ## ssh connect
            _is_conn = False
            try:
                _client.connect(self.hip, username=self.hid, password=self.hpw, port=self.hport, timeout=self.ssh_to)
                if _client.get_transport() != None and _client.get_transport().is_active() :
                    _c_dttm = str(datetime.now())
                    _conn_key = '{}.{}'.format(flib.PKEY_CMS_SVR, flib.PKEY_AUTH_STATUS)
                    _sres, _st, _serr = get_steps(_conn_key)
                    if _sres:
                        _atype = get_alert_type(*_st)
                        #                          (mod_dttm, region, hostname, step1, step2, step3, step4, obj, mon_val, mon_val_str, alert_type, _c_result, _c_error=None):
                        # SQL_DATA_Q.append( DBParam(_c_dttm, self.region, self.hname, _st[0], _st[1], _st[2], _st[3], 'ssh', None, flib.RI_VAL_AUTH_GOOD, _atype, flib.RES_SC) )
                    _is_conn = True
            except Exception as e:
                print(e)
                # logger.error("FAIL: %s SSH Connect Error, host=%s(%s), e=%s"%( _LNM, str(self.hname), str(self.hip), str(e) ))
                # logger.exception(e)
                if isinstance(e, paramiko.ssh_exception.AuthenticationException):
                    _c_dttm = str(datetime.now())
                    _conn_key = '{}.{}'.format(flib.PKEY_CMS_SVR, flib.PKEY_AUTH_STATUS)
                    _sres, _st, _serr = get_steps(_conn_key)
                    if _sres:
                        _atype = get_alert_type(*_st)
                        # SQL_DATA_Q.append( DBParam(_c_dttm, self.region, self.hname, _st[0], _st[1], _st[2], _st[3], 'ssh', None, flib.RI_VAL_AUTH_BAD, _atype, flib.RES_SC) )
                _is_conn = False

            _now = datetime.now()
            self._item_delay[_item] = _now - _prv
            _prv = _now

            if INAME_A_SSH in self.item_list :
                get_ssh_status(str(self.region), str(self.hname), _is_conn)
            #QQQQQQ
            _is_conn = True
            if _is_conn:
                for _iname in self.item_list :
                    if INAME_A_SSH == _iname :
                        continue

                    _item = _iname
                    print("_item::{}".format(_item))
                    ## alert
                    if _item == INAME_A_VM :
                        get_vm_status(_client, str(self.region), str(self.hname))
                    elif _item == INAME_A_PORT :
                        get_port_status(_client, str(self.region), str(self.hname))
                    elif _item == INAME_A_PSU :
                        get_psu_status(_client, str(self.region), str(self.hname))
                    elif _item == INAME_A_FAN :
                        get_fan_status(_client, str(self.region), str(self.hname))

                    ## perf
                    elif _item == INAME_P_CPU :
                        get_cpu_info(_client, str(self.region), str(self.hname))
                    elif _item == INAME_P_MEM :
                        get_mem_info(_client, str(self.region), str(self.hname))
                    elif _item == INAME_P_FS :
                        get_fs_info(_client, str(self.region), str(self.hname))
                    elif _item == INAME_P_PORT :
                        get_port_info(_client, str(self.region), str(self.hname))
                    elif _item == INAME_P_TEMP :
                        get_temp_info(_client, str(self.region), str(self.hname))
                    ## for TEST
                    elif _item == TEST_CRON_PROC :
                        get_proc_status(_client, str(self.region), str(self.hname), self.proclist)

                    else:
                        # logger.error("FAIL: %s UnSupported Item, i=%s, host=%s(%s)"%( _LNM, str(_iname), self.hname, self.hip ))
                        continue
                    _now = datetime.now()
                    self._item_delay[_item] = _now - _prv
                    _prv = _now
            else:
                # logger.warning("FAIL: %s SSH Connection Error, host=%s(%s)"%( _LNM, str(self.hname), str(self.hip) ))
                pass

            ## ssh close
            if _client is not None: _client.close()
            _client = None

            return
        except Exception as e:
            # logger.error("FAIL: %s SSH Proxy Exception, host=%s(%s), item=%s, err=%s"%( _LNM, str(self.hname), str(self.hip), str(_item), str(e) ))
            # logger.exception(e)
            print(e)

        finally:
            # if _client is not None:  _client.close()
            # _client = None

            self._runtime = datetime.now() - _tprv
            self._done = True
            return



def doCron(_cfgFile, _items, _period_m):
    global _LNM, SQL_DATA_Q, RAW_DATA_Q
    _LNM = "raw_%02dm"%_period_m
    print("from doCRON")
    # logger.info("@@@ Do Cron: %s"%( _LNM ))

    # QQQQQQQQQQQQQQ
    print("cccc:{}".format(_cfgFile))
    # _cfg = loadConfig(_cfgFile, _LNM, logger)
    _cfg = loadConfig(_cfgFile, _LNM, None)
    _dbinfo = _cfg['db_info_local']
    dbc_ = DBManager(_dbinfo['db_host'], _dbinfo['db_port'], _dbinfo['db_id'], _dbinfo['db_pw'])

    _tprv = datetime.now()
    try:
        ## read conn info file
        _db_info = _cfg['db_info_local']
        _info_file = _cfg['conn_info_file']
        _rg = _cfg['region']
        _acct = _cfg['account']

        if not os.path.isabs(_info_file):
            _info_file = os.path.join(_SELF_PATH, _info_file)

        print("_info_file/os.path.isfile(_info_file)::{}".format(_info_file, os.path.isfile(_info_file)))

        if os.path.isfile(_info_file):
            try:
                with open(_info_file, "r") as _infof:
                    _yiiii = ruamel.yaml.load(_infof, Loader=ruamel.yaml.RoundTripLoader)
                    _f_conn_list = json.loads(json.dumps(_yiiii))
            except Exception as e:
                print(e)

        # print("_f_conn_list::{}".format(_f_conn_list))

        ## Raw Writer
        # _fpfx = DEF_RAW_DIR + "/" + flib.PKEY_CMS_SVR
        # raw = RawWriter(_fpfx, RAW_DATA_Q, _LNM, logger)
        # raw.start()

        _thr_list = []
        _hname_list = []
        _s_prv = datetime.now()
        for _svc in _f_conn_list:
            if _svc != 'cms':
                continue

            for _f_conn_info in _f_conn_list[_svc]:
                _fc_vendor = _f_conn_info['vendor']
                _fc_ttype = _f_conn_info['ttype']
                _fc_ctype = _f_conn_info['ctype']

                if _fc_ttype != 'svr' :
                    continue
                if _fc_ctype != 'svr' :
                    continue
                print("_f_conn_list[_svc]::{}".format(_f_conn_list[_svc]))
                # for TEST
                _fc_extras = {'proclist': _f_conn_info['conn']['proclist']}
                print('\n\nfc_extras = {}\n\n'.format(_fc_extras))

                _fc_conn_dest = _f_conn_info['conn']['dest']
                if len(_fc_conn_dest) < 1 :
                    # logger.warning("FAIL: (%s) No Conn-Dest-Info, info=%s"%( _LNM, str(_f_conn_info) ))
                    continue

                _fc_conn_type = _f_conn_info['conn']['type']
                if _fc_conn_type != 'ssh' :
                    # logger.warning("FAIL: (%s) UnSupported Conn Method, info=%s"%( _LNM, str(_f_conn_info) ))
                    continue

                _cm_id = _cm_pw = _cm_port = None
                if 'comm_id' in _f_conn_info['conn'] :_cm_id = _f_conn_info['conn']['comm_id']
                if 'comm_pw' in _f_conn_info['conn'] :_cm_pw = _f_conn_info['conn']['comm_pw']
                if 'comm_port' in _f_conn_info['conn'] :_cm_port = _f_conn_info['conn']['comm_port']

                for _cdinfo in _fc_conn_dest :
                    _cdi_cid = _cdinfo['cid']
                    _cdi_ip = _cdinfo['ip']

                    _cdi_id = _cdinfo['id'] if 'id' in _cdinfo else _cm_id
                    _cdi_pw = _cdinfo['pw'] if 'pw' in _cdinfo else _cm_pw
                    _cdi_port = _cdinfo['port'] if 'port' in _cdinfo else _cm_port



                    if _cdi_id == None or _cdi_pw == None :
                        # logger.error("FAIL: (%s) No Conn ID or PW, info=%s, conn=%s"%( _LNM, str(_f_conn_info), str(_cdinfo) ))
                        continue

                    ## create and run SshProxy
                    try:
                        _sprxy = SshPxy(_items, _rg, _cdi_cid, _cdi_ip, _cdi_id, _cdi_pw, _fc_extras)
                        print("SshPxy _sprxy::{}".format(_sprxy.proclist))
                        # _sprxy.daemon = True
                        _sprxy.start()
                        _thr_list.append(_sprxy)
                        _hname_list.append(_cdi_cid)
                    except Exception as e:
                        # logger.error("FAIL: (%s) SSH-Thread Create Error, Target=%s"%( _LNM, str(_cdi_cid) ))
                        # logger.exception(e)
                        continue

                    # logger.info("SUCC: (%s) Run SSHProxy, host=%s"%( _LNM, str(_hname_list) ))

        # wait for sshproxy
        for _thr in _thr_list:
            _thr.join()

        
        print('len(SQL_DATA_Q)/SQL_DATA_Q::{}/{}'.format(len(SQL_DATA_Q), SQL_DATA_Q))
        print("TIME: (%s) ssh end, %s "%( _LNM, str(datetime.now()-_s_prv)) )

        # print("BEFORE SYS.EXIT()")
        # sys.exit()
        # print("AFTER SYS.EXIT()")
        ## DB Writer
        _db_list = []
        _d_prv = datetime.now()
        for _i in range(DB_POOL_CNT):
            _dbt = DBWriter(_db_info['db_host'], _db_info['db_port'], _db_info['db_id'], _db_info['db_pw'],
                            _thr_list, SQL_DATA_Q, _LNM, logger)
            _dbt.start()
            _db_list.append(_dbt)

        ## wait for sshproxy
        # for _thr in _thr_list:
        #     _thr.join()
        # logger.info("TIME: (%s) ssh end, %s "%( _LNM, str(datetime.now()-_s_prv) ))

        ## wait for DB
        _db_runt = {}
        _idx = 0
        for _dbc in _db_list:
            _dbc.join()
            _db_runt[str(_idx)] = _dbc.return_runtime()
            _idx = _idx + 1
        logger.info("TIME: (%s) db end, %s "%( _LNM, str(datetime.now()-_d_prv) ))

        # QQQQQQQQQQQQQ
        # raw.done()
        # while not raw.finish :
        #     sleep(0.1)
        # logger.info("SUCC: (%s) RawWriter Finish."%( _LNM ))

        # logger.info("SUCC: (%s) Finish SSHProxy"%( _LNM ))
        # show_runtime(datetime.now()-_tprv, _thr_list, _db_runt, True, _LNM, logger)
        # return True
    except Exception as e:
        # QQQQQQQQQQQQQQ
        # logger.error("FAIL: (%s) Exception Error, e=%s"%( _LNM, str(e) ))
        # logger.exception(e)
        return False

    dbc_.close()
