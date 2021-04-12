## default LIB
from time import sleep
import commentjson as json
from random import randrange

## open LIB
from tornado.web import RequestHandler


## custom LIB
from util.ko_logger import ko_logger
from apic_sim import INIT_VALUE_START

### for TEST 실행시 값 받아서 장애 테스트에 누적값의 초기값으로 사용.
INIT_VALUE = 0


## setting for daemon
TITLE = 'api'
MTITLE = 'apicsm'
DEF_LOG_DIR = '/var/log/%s'%MTITLE


logger = ko_logger(tag=TITLE, logdir=DEF_LOG_DIR, loglevel="debug", logConsole=False).get_instance()




## SIM CMD
TEST="test"
LOGIN="login"
FAULT_LIST="fault-list"




## SIM FUNC
def login():
    _ret = {"totalCount":"1", 'imdata':[{'aaaLogin':{'attributes':{'token':'kkkkkkkkkkkkkkkkkkk'}}}]}
    return _ret


def get_faults(_pod):
    with open("./test/faults.json", "r") as _cf:
        _jcfg = json.load(_cf)
    
    _ret = {}
    if _pod == "pod-1":
        _ret['totalCount'] = "%s"%str(len(_jcfg[_pod]))
        _ret['imdata'] = _jcfg[_pod]
    elif _pod == "pod-2":
        _ret['totalCount'] = "%s"%str(len(_jcfg[_pod]))
        _ret['imdata'] = _jcfg[_pod]
    elif _pod == "pod-3":
        _ret['totalCount'] = "%s"%str(len(_jcfg[_pod]))
        _ret['imdata'] = _jcfg[_pod]
    elif _pod == "pod-4":
        _ret['totalCount'] = "%s"%str(len(_jcfg[_pod]))
        _ret['imdata'] = _jcfg[_pod]
    else:
        _ret["totalCount"] = "0"
        _ret["imdata"] = []
    
    logger.debug(_ret)
    return _ret









class ApiHandler(RequestHandler):
    global INIT_VALUE
    
    def initialize(self, opCode, _cfg):
        self.opCode = opCode
        self.cfg = _cfg
    
    def post(self):
        logger.info(">>>> RECV POST-REQ, op=%s"%str(self.opCode))
        logger.debug(" URL: %s"%( str(self.request.uri) ))
        if self.opCode == TEST:
            sleep(10)
            _str_ret = "API POST Test OK\n"
        elif self.opCode == LOGIN :
            _ret = login()
            _str_ret = json.dumps(_ret)
        else:
            logger.error("UnSupported POST-URL..")
            _str_ret = "UnSupported POST-URL\n"
        
        self.write(_str_ret)
    
    
    def get(self, _param1=None, _param2=None):
        logger.info(">>>> RECV GET-REQ, op=%s"%str(self.opCode))
        logger.debug(" URL: %s"%( str(self.request.uri) ))
        logger.debug(" PARAM: %s, %s"%( str(_param1), str(_param2) ))
        try:
            if self.opCode == TEST:
                sleep(10)
                _str_ret = "API POST Test OK\n"
            elif self.opCode == FAULT_LIST : 
                _ret = get_faults(_param1)
                _str_ret = json.dumps(_ret)
            else:
                logger.error("UnSupported GET-URL..")
                _str_ret = "UnSupported POST-URL\n"
            
            self.write(_str_ret)
        except Exception as e:
            logger.error(e)
            self.write(str(e))

class RmonEtherStatsApiHandler(RequestHandler):
    def initialize(self, opCode, _cfg):
        self.opCode = opCode
        self.cfg = _cfg
        self.result_dict = None

    def prepare(self):
        _c = '''{"totalCount":"1", "imdata": [{"rmonEtherStats":{"attributes":{"broadcastPkts":"0",
                                                "cRCAlignErrors":"0",
                                                "childAction":"",
                                                "clearTs":"never",
                                                "collisions":"0",
                                                "dn":"topology/pod-1/node-1001/sys/phys-[eth1/1]/dbgEtherStats",
                                                "dropEvents":"0",
                                                "fragments":"0",
                                                "jabbers":"0",
                                                "modTs":"never",
                                                "multicastPkts":"0",
                                                "octets":"0",
                                                "oversizePkts":"0",
                                                "pkts":"0",
                                                "pkts1024to1518Octets":"0",
                                                "pkts128to255Octets":"0",
                                                "pkts256to511Octets":"0",
                                                "pkts512to1023Octets":"0",
                                                "pkts64Octets":"0",
                                                "pkts65to127Octets":"0",
                                                "rXNoErrors":"0",
                                                "rxGiantPkts":"0",
                                                "rxOversizePkts":"0",
                                                "status":"",
                                                "tXNoErrors":"0",
                                                "txGiantPkts":"0",
                                                "txOversizePkts":"0",
                                                "undersizePkts":"0"}}}]}'''
        self.result_dict = json.loads(_c)

    def get(self, pod_name=None, node_name=None):
        print(' URL: {}'.format(str(self.request.uri)) )
        print(' PARAM: {} {} '.format( str(pod_name), str(node_name)) )
        global INIT_VALUE
        result = None

        _b = INIT_VALUE
        INIT_VALUE += randrange(1, 300)
        self.result_dict['imdata'][0]['rmonEtherStats']['attributes']['cRCAlignErrors'] = str(INIT_VALUE)
        result = json.dumps(self.result_dict)

        print('\n\n before {} /// after {}  \n\n'.format(_b, INIT_VALUE))
        self.write(result)


    # def post(self, pod_name=None):
    #     print('RmonEtherStatsApiHandlerRmonEtherStatsApiHandlerRmonEtherStatsApiHandler')
    #     print('postpostpostpostpostpostpostpostpostpost')
    #     print(' URL: {}}'.format(str(self.request.uri)) )
    #     print(' PARAM: {} '.format( str(pod_name)) )
    #     result = None
    #     data = self.request.body if self.request.body else 'NO DATA TO SHOW WITH POST'
    #
    #     if data:
    #         j_data = json.loads(data)
    #         print('j_dataj_dataj_data ::: {}'.format(j_data))
    #         result = json.dumps(self.result_dict)
    #
    #     self.write(result)


class RmonIfInApiHandler(RequestHandler):
    def initialize(self, opCode, _cfg):
        self.opCode = opCode
        self.cfg = _cfg
        self.result_dict = None

    def prepare(self):
        _c = '''{"totalCount":"1", "imdata": [{"rmonIfIn":{"attributes":{"dn": "topology/pod-1/node-1001/sys/phys-[eth1/1]/dbgIfIn", 
                "broadcastPkts": "0", 
                "discards": "0", 
                "errors": "0", 
                "multicastPkts": "0", 
                "nUcastPkts": "0", 
                "octets": "0", 
                "ucastPkts": "0", 
                "unknownProtos": "0"}}}]}'''
        self.result_dict = json.loads(_c)

    def get(self, pod_name=None, node_name=None):
        print(' URL: {}'.format(str(self.request.uri)) )
        print(' PARAM: {} {} '.format( str(pod_name), str(node_name)) )
        global INIT_VALUE
        result = None

        _b = INIT_VALUE
        INIT_VALUE += randrange(1, 300)
        self.result_dict['imdata'][0]['rmonIfIn']['attributes']['errors'] = str(INIT_VALUE)
        result = json.dumps(self.result_dict)

        print('\n\n before {} /// after {}  \n\n'.format(_b, INIT_VALUE))
        self.write(result)


def url( _conf ):
    url = [ ('/test', ApiHandler, dict(opCode=TEST, _cfg=_conf)),
            (r'/api/aaaLogin.json', ApiHandler, dict(opCode=LOGIN, _cfg=_conf)),
            (r"/api/class/topology/(.*)/faultInfo.json", ApiHandler, dict(opCode=FAULT_LIST, _cfg=_conf)),
            (r'/api/class/topology/(?P<pod_name>.*)/rmonEtherStats.json', RmonEtherStatsApiHandler, dict(opCode=TEST, _cfg=_conf)),
            (r'/api/class/topology/(?P<pod_name>.*)/(?P<node_name>.*)/rmonEtherStats.json', RmonEtherStatsApiHandler, dict(opCode=TEST, _cfg=_conf)),
            (r'/api/class/topology/(?P<pod_name>.*)/rmonIfIn.json', RmonIfInApiHandler, dict(opCode=TEST, _cfg=_conf)),
            (r'/api/class/topology/(?P<pod_name>.*)/(?P<node_name>.*)/rmonIfIn.json', RmonIfInApiHandler, dict(opCode=TEST, _cfg=_conf)),
            ]
    return url


