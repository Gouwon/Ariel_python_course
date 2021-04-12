#!/usr/bin/python3

## default LIB
import sys, ssl, logging, json
from time import sleep

## open LIB
import ruamel.yaml
from tornado.web import Application
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

## custom LIB
import apicsm_api
from util.ko_logger import ko_logger


## setting for daemon
TITLE = 'apicsm'
TITLE_LOG = 'APIC Simulator'

DEF_CONF = './conf/%s.yaml'%TITLE
DEF_LOG_DIR = '/var/log/%s'%TITLE
DEF_AUTH_CRT = './key/%s_crt.pem'%TITLE
DEF_AUTH_KEY = './key/%s_key.pem'%TITLE
DEF_AUTH_CER = './key/%s_cac.pem'%TITLE
## for TEST QQQQQQQQQQQQQ
DEF_LOG_DIR = '/root/gwkim/5gIM/task/version-development/log/%s'%TITLE


_klogger = ko_logger(tag=TITLE, logdir=DEF_LOG_DIR, loglevel="debug", logConsole=False)
logger = _klogger.get_instance()


_log_torn_acc = logging.getLogger('tornado.access')
_log_torn_app = logging.getLogger('tornado.application')
_log_torn_gen = logging.getLogger('tornado.general')

_log_torn_acc.addHandler(_klogger.log_handler())
_log_torn_app.addHandler(_klogger.log_handler())
_log_torn_gen.addHandler(_klogger.log_handler())

### for TEST 실행시 값 받아서 장애 테스트에 누적값의 초기값으로 사용.
INIT_VALUE_START = 0


class httpSvrThread:
    
    def __init__(self, applictions, port, proc):
        self.svr = HTTPServer(applictions, ssl_options={
                "certfile": DEF_AUTH_CRT,
                "keyfile": DEF_AUTH_KEY,
#                 "ca_certs": DEF_AUTH_CER,
#                 "cert_reqs": ssl.CERT_REQUIRED
        })
#         self.svr = HTTPServer(applictions)
        self._port = port
        self._proc = proc
    
    def shutdown(self):
        logger.info('@@ Shutdown - ioloop.stop')        
        IOLoop().current().stop()
        logger.info('@@ Shutdown - svr.stop')
        self.svr.stop()
        
    def start(self):
        logger.info('@@ Run Https Server, port=%s'%str(self._port))
        self.svr.bind(self._port)
        try:
            self.svr.start(self._proc)
        except KeyboardInterrupt:
            logger.info('Keyboard Interrupt')
            self.shutdown()
        IOLoop().current().start()


def loadConfig(_cfgFile):
    logger.info("@@@ Load Config")
    
    with open(_cfgFile, "r") as _cf:
        _ycfg = ruamel.yaml.load(_cf, Loader=ruamel.yaml.RoundTripLoader)
        _cfg = json.loads(json.dumps(_ycfg))
    return _cfg


def makeApp(_cfg):
    logger.info("@@@ Make App")
    ## ToDo
    
    app = Application(apicsm_api.url(_cfg)
                      )
    return app


def startScheduler(_cfg):
    logger.info("@@@ Run Scheduler")
    ## ToDo


def onStart(_cfg):
    logger.info("@@@ Run On-Start")
    ## ToDo


def startAPI(_app, _cfg):
    logger.info("@@@ Run API Server")
    ## ToDo
    
    _port = _cfg['api_port'] if 'api_port' in _cfg else 50020
    _proc = _cfg['api_proc'] if 'api_proc' in _cfg else 1
    
    svr = httpSvrThread(_app, _port, _proc)
    svr.start()


def main(_cfgFile):
    
    logger.info("---------------[[  %s Starting...  ]]---------------"%TITLE_LOG)
    
    _cfg = loadConfig(_cfgFile)
    
    _app = makeApp(_cfg)
    
    startScheduler(_cfg)
    
    onStart(_cfg)
    
    sleep(3)
    
    startAPI(_app, _cfg)
    
    logger.info("---------------[[  %s Exit !!!  ]]---------------"%TITLE_LOG)




if __name__ == "__main__":

    cfgFile = DEF_CONF
    if len(sys.argv) > 3 :
        cfgFile = sys.argv[2]

    if len(sys.argv) >= 2:
        INIT_VALUE_START = sys.argv[1]
        apicsm_api.INIT_VALUE = int(INIT_VALUE_START)
    print('\n\n!!!!!!!!!!!!초기값은 {} 입니다!!!!!!!!!!!\n\n'.format(INIT_VALUE_START))

    main(cfgFile)


