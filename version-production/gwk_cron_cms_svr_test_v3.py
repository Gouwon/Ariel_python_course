#!/usr/bin/python2

## default LIB
import os, sys

## custom LIB
import gwk_cron_cms_svr_base_v3 as ibase

_SELF_PATH = os.path.dirname(os.path.abspath(__file__))

TITLE_LOG = '5GIM Proxy CMS Server Cron(V3)'
CFG_NAME = 'fgmon_pxy_v3'
DEF_CONF = _SELF_PATH + '/conf/{}.yaml'.format(CFG_NAME)
## for Windows
# DEF_CONF = _SELF_PATH + '\\conf\\{}.yaml'.format(CFG_NAME)
MOD_NAME = 'cron_cms_svr_test_v3'

def main(_cfgFile):

    # ibase.create_logger(MOD_NAME)

    # ibase.log_info("---------------[[  %s Starting...  ]]---------------"%TITLE_LOG)

    # r_cnt = ibase.is_run(str(os.getpid()), str(os.getppid()), os.path.basename(str(__file__)) )
    # if r_cnt > 0 :
        # ibase.log_info("SKIP: OnGoing, cnt=%s"%str(r_cnt))
        # ibase.log_info("---------------[[  %s Exit !!!  ]]---------------"%TITLE_LOG)
        # return

    _items = [
        ibase.INAME_A_VM, ibase.INAME_A_PORT, ibase.INAME_A_PSU, 
        ibase.INAME_A_FAN, ibase.INAME_P_CPU, ibase.INAME_P_MEM, 
        ibase.INAME_P_FS, ibase.INAME_P_PORT, ibase.INAME_P_TEMP, 
        ibase.TEST_CRON_PROC
    ]
    argc = len(sys.argv)
    if argc > 1:
        _items = sys.argv[1].strip().split(',')
    print('cfg={} items={}'.format(_cfgFile, _items))
    _period_m = 1
    _res = ibase.doCron(_cfgFile, _items, _period_m)

    ## for debug
#     from time import sleep
#     sleep(10)

    # ibase.log_info("---------------[[  %s Exit !!! result=%s ]]---------------"%( TITLE_LOG, str(_res) ))




if __name__ == "__main__":

    cfgFile = DEF_CONF
    if len(sys.argv) > 2 :
        cfgFile = sys.argv[1]

    main(cfgFile)


