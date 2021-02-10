#-*- coding: utf-8 -*-

# Standard library
import time
from threading import Thread

# 3rd party librar
import paramiko

# from local
import collect_funcs as cfn
import common_lib as clb
from DBObject import ValueObject


SQL_DATA_Q = []
SSH_CONNECTION_RETRY_NUMBER = 3



class EquipmentConnector(paramiko.client.SSHClient, Thread):
    """
    장비에 접속하여 수집 항목들을 수집하는 클래스
    paramiko의 SSHClient, threading.Thread 상속 클래스
    각 노드(IP)별로 객체 생성하여 사용
    """
    def __init__(self, items, ip, cid, id, pw, port, ttype, ctype, vendor, region, extras=None):
        super().__init__()
        Thread.__init__(self)
        self.gathering_items = items        # 장비에서 수집할 항목
        self.conn_dest_ip = ip              # 장비 접속 IP
        self.conn_dest_cid = cid            # 장비 hostname
        self.comm_id = id                   # 장비 접속 ID
        self.comm_pw = pw                   # 장비 접속 PW
        self.port = port if port else 22    # 장비 접속 포트
        self.ttype = ttype                  # 장비 타입
        self.ctype = ctype                  # 장비 연결 타입
        self.vendor = vendor                # 장비 공급사
        self.region = region
        self.extras = extras                # 장비 수집항목에 proc 있을 경우, 감시할 프로세스명 리스트
        self.done = False                   # 스레드의 장비 수집 종료 여부 플래그

    def is_done(self):
        """
        스레드가 종료되었는 지 여부 확인 함수
        DBAO(DBAccessObject)에서 장비 수집 스레드 상태 확인 시 사용
        """
        return self.done
    
    def _done(self):
        """
        스레드가 종료됨을 의미하는 플래그 설정 함수
        """
        self.done = True
 
    def run(self):
        """
        스레드가 start()되면 실행되는 함수
        실제 스레드 작업 내용을 정의하는 스레드 메인 함수
        """
        try:
            self.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            print('\n\n수집 장비::{} 로 SSH 연결을 시도합니다.'.format(self.conn_dest_cid))
            time.sleep(clb.INTERVAL)

            for _ in range(SSH_CONNECTION_RETRY_NUMBER):  # 서버 접속 시도 3번
                try:
                    self.connect(self.conn_dest_ip, username=self.comm_id, password=self.comm_pw, port=self.port, timeout=10)
                    break
                except Exception as e:
                    continue
            
            if self.get_transport() or self.get_transport().is_active():    # 서버 접속 시도가 성공인 경우에만 진행.
                print('\n\n수집 장비::{} 에 SSH 연결 되었습니다.'.format(self.conn_dest_cid))
                time.sleep(clb.INTERVAL)

                for item in self.gathering_items:   # 수집 항목별로 순회하면서 해당하는 항목의 함수를 실행
                    print('\n\n수집 장비::{} 로부터 수집 항목 {}을 수집합니다.'.format(self.conn_dest_cid, item))
                    time.sleep(clb.INTERVAL)
                    func = cfn.FUNC_MAPPING.get(item, None)

                    try:
                        if isinstance(func, list):  # 수집 항목에 할당된 함수가 여러 개인지 확인
                            funcs = func
                            for func in funcs:
                                result = func(self)
                        else:
                            result = func(self)
                        print('\n\n수집 장비::{} / 수집 항목 {} 을 수집하였습니다.\n\n{}'.format(self.conn_dest_cid, item, result))
                        time.sleep(clb.INTERVAL)
                    except Exception as e:
                        print('\n\n수집 장비::{} / 수집 항목 {} 수집에 실패하였습니다.\n\n{}'.format(self.conn_dest_cid, item, e))
                        print(e)
                        time.sleep(clb.INTERVAL)
                        continue

                    global SQL_DATA_Q   # 수집 항목 결과 저장
                    if isinstance(result, list):
                        for res in result:
                            vo = ValueObject(self, **res)
                            SQL_DATA_Q.append(vo)
                        
                    else:
                        vo = ValueObject(self, **result)
                        SQL_DATA_Q.append(vo)
            
        except Exception as e:
            print('\n\n수집 장비::{} 에 SSH 연결이 실패했습니다.'.format(self.conn_dest_cid))
            print("Error EquipmentConnector run()::{}".format(e))
            time.sleep(clb.INTERVAL)
        
        finally:
            if self is not None:    # 서버 접속 종료(무조건)
                self.close()
                self._done()
            print('\n\n수집 장비::{} SSH 연결을 종료합니다.'.format(self.conn_dest_cid))
            time.sleep(clb.INTERVAL)


    @staticmethod
    def get_SQL_DATA_Q():
        """
        EquipmentConnector 객체가 수집한 데이터를 공통으로 저장하는 SQL_DATA_Q를 반환
        클래스 외부에서 해당 데이터 결과만 사용할 때 호출
        """
        global SQL_DATA_Q
        return SQL_DATA_Q