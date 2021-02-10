#-*- coding: utf-8 -*-

# Standard library
import sys
import time
import util

# from local
import common_lib as clb
from DBObject import DBAcceessObject
from EquipmentConnector import EquipmentConnector


COLLECTION_ITMES = ['proc']
CONF_DIRECTORY = 'conf'
SETTINGS_FILE = 'settings.yaml'
CONNECTION_INFO_FILE = 'connection_info.yaml'

EQUIPMENT_CMS = 'cms'
DBAO_POOL_COUNT = 2



if __name__ == '__main__':
    print('\n\n수집 프로그램을 시작합니다.')
    time.sleep(clb.INTERVAL)
    items = sys.argv[1].strip().split(',') if len(sys.argv) > 1 else COLLECTION_ITMES

    print('\n\n수집 항목은 {} 입니다.'.format(items))
    time.sleep(clb.INTERVAL)
    
    print('\n\n설정 정보를 설정 파일로부터 읽어옵니다...')
    time.sleep(clb.INTERVAL)
    SETTINGS_FILE_LOCATION = util.join_path(CONF_DIRECTORY, SETTINGS_FILE)
    CONNECTION_INFO_FILE_LOCATION = util.join_path(CONF_DIRECTORY, CONNECTION_INFO_FILE)

    print('\n\n기본 설정 정보를 {} 파일로부터 읽어옵니다...'.format(SETTINGS_FILE))
    time.sleep(clb.INTERVAL)
    setting_dict = util.convert_yaml_into_dict(SETTINGS_FILE_LOCATION)
    
    print('\n\n연결 설정 정보를 {} 파일로부터 읽어옵니다...'.format(CONNECTION_INFO_FILE))    
    time.sleep(clb.INTERVAL)
    connection_info_dict = util.convert_yaml_into_dict(CONNECTION_INFO_FILE_LOCATION)

    ssh_threads_pool = []
    dba_threads_pool = []

    for equipment in connection_info_dict:
        if equipment != EQUIPMENT_CMS:
            continue

        for i in connection_info_dict.get(equipment):
            equipment_ttype = i['ttype']
            equipment_ctype = i['ctype']
            equipment_vendor = i['vendor']
            equipment_conn_comm_id = i['conn']['comm_id']
            equipment_conn_comm_pw = i['conn']['comm_pw']
            equipment_conn_port = i['conn'].get('port', None)
            equipment_conn_proclist = i['conn'].get('proclist', None)
                        
            for j in i['conn']['dest']:
                equipment_conn_dest_ip = j['ip']
                equipment_conn_dest_cid = j['cid']
                
                ec_param = {
                    'items': items,
                    'ip':   equipment_conn_dest_ip,
                    'cid':  equipment_conn_dest_cid,
                    'id':   equipment_conn_comm_id,
                    'pw':   equipment_conn_comm_pw,
                    'port': equipment_conn_port,
                    'ttype': equipment_ttype,
                    'ctype': equipment_ctype,
                    'vendor': equipment_vendor,
                    'region': setting_dict['region']
                }
                
                if equipment_conn_proclist:
                    ec_param['extras'] = equipment_conn_proclist

                ssh_thread = EquipmentConnector(**ec_param)
                ssh_thread.start()
                ssh_threads_pool.append(ssh_thread)
    
    
    for ssh_thread in ssh_threads_pool:
        ssh_thread.join()

    SQL_DATA_Q = EquipmentConnector.get_SQL_DATA_Q()        
    print("SQL_DATA_Q::{}".format(SQL_DATA_Q))
    print('\n\nDB 저장을 위해 DB에 연결합니다.')
    time.sleep(clb.INTERVAL)
    db_info_dict = setting_dict.get('db_info_local', None)

    db_info_dict['data_q'] = SQL_DATA_Q
    print("\n\n------------------")
    time.sleep(clb.INTERVAL)
    print(db_info_dict)
    print("\n\n------------------")
    db_info_dict['ssh_threads'] = ssh_threads_pool

    for _ in range(DBAO_POOL_COUNT):
        dba_thread = DBAcceessObject(**db_info_dict)
        dba_thread.start()
        dba_threads_pool.append(dba_thread)

    for dba_thread in dba_threads_pool:
        dba_thread.join()
    print('\n\n수집 프로그램을 종료합니다.')
    time.sleep(clb.INTERVAL)
    sys.exit()