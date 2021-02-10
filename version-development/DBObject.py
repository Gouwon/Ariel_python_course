#-*- coding: utf-8 -*-

# Standard library
import time
from datetime import datetime
from threading import Thread, current_thread

# 3rd party library
import pymysql

# from local
import common_lib as clb
from fgpxy_lib import get_steps



class DBAcceessObject(Thread):
    """
    DB 접속 및 쿼리 실행 클래스
    fivegmon.fgm_pxy_item 테이블을 대상으로 수집된 자료를 insert|update 쿼리 수행
    ** 기존 사용하던 DB에 대한 쿼리 작업을 진행하기 때문에 기존의 SQL로 진행함.
    """
    def __init__(self, db_host, db_port, db_id, db_pw, data_q, ssh_threads, _type='pxy'):
        super().__init__()

        # DB 접속 객체
        self.conn = pymysql.connect(
            host=db_host, port=db_port, user=db_id, passwd=db_pw, 
            autocommit=True, connect_timeout=10, use_unicode=True, 
            charset='utf8'
        )
        self.data_q = data_q                    # DB 입력할 데이터 큐
        self.ssh_threads_pool = ssh_threads     # 실제 장비 수집 스레드 목록::해당 스레드 목록의 스레드들의 종료여부를 판단하여 DB 연결도 작업 이후 종료시킴.
        self._cursor = self.conn.cursor()       # DB 쿼리 실행 및 결과 반환하는 커서 객체
        self.type = _type                       # DB에 insert|update 되는 대상 테이블 구분. pxy|switch
        
    def run(self):
        while True:
            try:
                _sql_param = self.data_q.pop(0)     # DB에 입력할 데이터를 하나씩 뽑음.(FIFO)
                
                #TODO DB func 만들기
                # QQQQQQQ DB 테이블에 맞게 데이터 폼 맞추고, insert or update. 데이터 폼 맞추는 것과 관련하여 EC에서 폼을 맞출 것인지 생각해봐야함.
                # self._cursor.execute(SELECT_FROM_PRX_ITEM)
                # result = self._cursor.fetchall()
                # print(result)
                # time.sleep(INTERVAL)
                is_query_success = True 
                try:
                    print('\n\nDB에 수집된 자료를 저장합니다.')
                    time.sleep(clb.INTERVAL)
                    sql = clb.SQL_UPD_PXY if self.type == 'pxy' else clb.SQL_UPD_PXYSW
                    
                    self._cursor.execute(
                        sql, (
                            _sql_param.mod_dttm, _sql_param.mon_val, 
                            _sql_param.mon_val_str, _sql_param.c_result, 
                            _sql_param.c_error, _sql_param.region, 
                            _sql_param.hostname, _sql_param.step1, 
                            _sql_param.step2, _sql_param.step3, 
                            _sql_param.step4, _sql_param.obj, 
                            _sql_param.mod_dttm
                        )
                    )
                    _rcnt = self._cursor.rowcount
                    _rid = self._cursor.lastrowid
                except Exception as e:
                    is_query_success = False
                    print('DB run Error1::{}'.format(e))
                
                if is_query_success:
                    # 상기 Update 쿼리가 정상적으로 실행되었으나, 
                    # 조건에 해당하는 레코드가 없어서 반영이 안 되었을 경우, insert 작업 실시
                    if _rcnt < 1:
                        sql = clb.SQL_INS_PXY if self.type == 'pxy' else clb.SQL_INS_PXYSW
                        try:
                            self._cursor.execute(
                                sql, (
                                    _sql_param.mod_dttm, _sql_param.region,
                                    _sql_param.hostname, _sql_param.step1,
                                    _sql_param.step2, _sql_param.step3,
                                    _sql_param.step4, _sql_param.obj,
                                    _sql_param.mon_val, _sql_param.mon_val_str,
                                    _sql_param.alert_type, _sql_param.c_result,
                                    _sql_param.c_error, _sql_param.region,
                                    _sql_param.hostname, _sql_param.step1,
                                    _sql_param.step2, _sql_param.step3,
                                    _sql_param.step4, _sql_param.obj
                                )
                            )
                            _rcnt = self._cursor.rowcount
                            _rid = self._cursor.lastrowid
                        except Exception as e:
                            print('DB run Error2::{}'.format(e))
                            is_query_success = False
                
                if is_query_success:
                    time.sleep(clb.INTERVAL)
                    print('\n\nDB에 자료를 저장하였습니다.\n저장된 레코드 수::{}'.format(_rcnt))
                else:
                    time.sleep(clb.INTERVAL)
                    print('\n\nDB에 자료를 저장에 실패하였습니다.')

            except Exception as e:
                print(e)
                # 장비 수집 스레드의 종료 여부를 확인
                _thr_end = False if list(filter(lambda thr : not thr.is_done(), self.ssh_threads_pool)) else True
                time.sleep(clb.INTERVAL)

                # 작업 수집 스레드의 종료 여부(True/False), 데이터 큐의 존재 여부(0/not 0) --> DB 작업 종료/진행
                if _thr_end and len(self.data_q) == 0:
                    break
                else:
                    continue

        if self.conn is not None:       #  DB 작업 종료되어 while-loop 벗어나면 DB 연결 해제
            self.conn.close()    

        print('\n\n{} DB 연결을 종료합니다.'.format(current_thread().name))


class ValueObject:
    """
    DB 입력할 값을 처리하는 VO
    기존 DBParam Class를 대체하여 DB에 입력할 값을 처리
    """
    def __init__(
        self, client, obj, mon_val=None, 
        mon_val_str=None, c_error=None, step4=None, is_result_str=True,
        error=None
    ):
        self.mod_dttm = str(datetime.now())
        self.region = client.region
        self.hostname = client.conn_dest_cid
        self.step4 = step4
        self.step1, self.step2, self.step3 = self.set_step()
        self.alert_type = self.set_alert_type()

        self.obj = obj
        self.mon_val = mon_val
        self.mon_val_str = mon_val_str
        self.c_error = c_error
        self.c_result = 0 if c_error else 1

    def set_step(self):
        """
        DB의 칼럼 step1, step2, step3를 step4를 기반으로 반환하는 함수
        """
        _res, _st, _serr = get_steps(self.step4)
        return _st[0], _st[1], _st[2]
        
    def set_alert_type(self):
        """
        수집한 자료의 알람 타입을 반환하는 함수
        미리 정의된 알람 타입이 명시된 딕셔너리에서 자신이 해당하는 타입을 찾아서 반환함.
        """
        for key, value in clb.ALERT_TYPE_DICT.items():
            if (self.step3 is not None) and (self.step3 in value):
                return key
        
    def __repr__(self):
        return """{
            'DBParam' : {
                'mod_dttm': %s,
                'region': %s,
                'hostname': %s,
                'step1': %s,
                'step2': %s,
                'step3': %s,
                'step4': %s,
                'obj': %s, 
                'mon_val': %s,
                'mon_val_str': %s,
                'alert_type': %s,
                'c_result': %s,
                'c_error': %s
            }
        }""" % (
            self.mod_dttm, self.region, self.hostname, self.step1, self.step2, 
            self.step3, self.step4, self.obj, self.mon_val, self.mon_val_str,
            self.alert_type, self.c_result, self.c_error
        )

