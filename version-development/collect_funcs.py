#-*- coding: utf-8 -*-

# Standard library
import time

# from local
import common_lib as clb



def add_to_FUNC_MAPPING(func):
    def wrapper(*args, **kwargs):
        if func not in set(FUNC_MAPPING.keys()):
            FUNC_MAPPING[str(func.__name__)] = func
        func(*args, **kwargs)
    return wrapper

# @add_to_FUNC_MAPPING
def get_proc_status(client, *args):
    """
    접속한 서버의 프로세스 상태를 확인하는 함수.
    함수의 첫 번째 인자는 접속한 클라이언트 객체이고, 
    함수의 결과는 성공의 경우 리스트-튜플[ (), ()... ], 실패의 경우에는 에러를 일으킴
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
    print("프로세스 수집을 시작합니다.")
    time.sleep(clb.INTERVAL)
    cmd = "ps -ef | awk '$3 !~ /^2$/ {print}' "
    # cmd = 'pwd'
    result = []

    _stdin, _stdout, _stderr = client.exec_command(cmd)

    _stdout_ = str(_stdout.read()).strip()
    _stderr_ = str(_stderr.read()).strip()
    

    try:
        if client.extras is not None:
            if _stderr_ != '' :
                for _proc in client.extras:
                    _stat_txt = 'off'
                    for _ll in _stdout_.splitlines():
                        if client.extras.get(_proc) in _ll:
                            _stat_txt = 'on'
                            break
                    result.append({
                        'mon_val_str': _stat_txt,
                        'step4': 'cms.svr.proc.status.ps-ef', 
                        'obj': _proc
                    })
            # else:
            #     result.append({
            #         'mon_val_str': _stat_txt, 
            #         'step4': 'cms.svr.proc.status.ps-ef', 
            #         'obj': _proc
            #     })
                                            
                return result
        else:
            # result.append({
            #     'c_error': 'No Origin', 
            #     'step4': 'cms.svr.proc.status.ps-ef', 
            #     'obj': _proc
            # })
            print('Error:: 감시할 프로세스 목록이 없습니다.')
            
    except Exception as e:
        print('Error get_proc_status()::{}'.format(e))
        raise Exception(e)

    

FUNC_MAPPING = {'proc': get_proc_status}


# def asd(c, *sadf):

#     print("sadfsadfsadf   {}".format(sadf))
#     print("afsdfsda ............ {}".format(c))

# l = ['qq']
# asd("c", *l)