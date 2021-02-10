# -*- coding: utf-8 -*-

import sys, os
import logging
from logging import handlers

logging.addLevelName(logging.WARNING, "WARN")

def excp(self, msg, *aa, **kk):
    """
    Convenience method for logging an ERROR with exception information.
    """
    kk['exc_info'] = 1
    self._log(logging.CRITICAL, msg, aa, **kk)
logging.Logger.fatal = excp

class ko_logger():
    def __init__(self, tag="fgmon", logdir="/var/log/fgmon/", loglevel="debug", logConsole=False, onlyLog=False, forSyslog=False):
        self.tag    = tag
        self.logdir = logdir
        self.loglevel = loglevel
        self.logHandler = None
        
        if forSyslog : 
            onlyLog = True
            logConsole = False
        
        self.log_fname = "%s.log" % (tag)
        if onlyLog :
            self.errlog_fname = None
            self.exc_fname = None
        else:
            self.errlog_fname = "%s.err" % (tag)
            self.exc_fname = "%s.exc" % (tag)

        #script_dir = os.path.dirname(inspect.stack()[-1][1])
        #logdir = os.path.join(script_dir, 'log')

        if os.path.exists(logdir):
            self.filelog_file_path = os.path.join(logdir, self.log_fname)
            if not onlyLog :
                self.errlog_file_path  = os.path.join(logdir, self.errlog_fname)
                self.exclog_file_path = os.path.join(logdir, self.exc_fname)
        else:
            try:
                os.makedirs(logdir)
            except Exception as err:
                print(err)
                logdir = "/var/log/"

            self.filelog_file_path = os.path.join(logdir, self.log_fname)
            if not onlyLog :
                self.errlog_file_path  = os.path.join(logdir, self.errlog_fname)
                self.exclog_file_path = os.path.join(logdir, self.exc_fname)
        
        self.logger = logging.getLogger(tag)

#         self.logger.fatal = excp
        
        if loglevel == "debug":
            self.logger.setLevel(logging.DEBUG)
        elif loglevel == "info":
            self.logger.setLevel(logging.INFO)
        elif loglevel == "warning":
            self.logger.setLevel(logging.WARN)
        elif loglevel == "error":
            self.logger.setLevel(logging.ERROR)
        elif loglevel == "critical":
            self.logger.setLevel(logging.CRITICAL)
        else:
            self.logger.setLevel(logging.INFO)
        
        if forSyslog : 
            self._set_syslog_log_env(self.filelog_file_path)
        else:
            self._set_file_log_env(self.filelog_file_path)
            if not onlyLog :
                self._set_error_log_env(self.errlog_file_path)
                self._set_exc_log_env(self.exclog_file_path)

        if logConsole:
            self._set_console_log_env()

    def get_instance(self):
        return logging.getLogger(self.tag)
    
    def log_handler(self):
        return self.logHandler
    
    def _set_error_log_env(self, logfile):

        errfmt = logging.Formatter("[%(asctime)s][%(levelname)-5s][%(module)s.%(lineno)04d] %(message)s")

        maxbytes = 1024 * 1024 * 10  # 10MB
        error_handler = handlers.RotatingFileHandler(logfile, maxBytes=maxbytes, backupCount=5)
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(errfmt)

        self.logger.addHandler(error_handler)

    def _set_exc_log_env(self, logfile):

        excfmt = logging.Formatter("[%(asctime)s][%(levelname)-5s][%(module)s.%(lineno)04d] %(message)s")

        maxbytes = 1024 * 1024 * 10  # 10MB
        exc_handler = handlers.RotatingFileHandler(logfile, maxBytes=maxbytes, backupCount=5)
        exc_handler.setLevel(logging.CRITICAL)
        exc_handler.setFormatter(excfmt)

        self.logger.addHandler(exc_handler)
    
    def _set_file_log_env(self, logfile):
        if self.loglevel == "debug":
#             stdfmt = logging.Formatter("%(asctime)s [%(threadName)s-%(filename)s/%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s")
            stdfmt = logging.Formatter("[%(asctime)s][%(levelname)-5s] %(message)s [%(module)s.%(lineno)04d]")
        else:
            stdfmt = logging.Formatter("[%(asctime)s][%(levelname)-5s] %(message)s [%(module)s.%(lineno)04d]")
            
        maxbytes = 1024 * 1024 * 10  # 10MB
        filelog_handler = handlers.RotatingFileHandler(logfile, maxBytes=maxbytes, backupCount=10)
        #filelog_handler = handlers.TimedRotatingFileHandler(logfile, when="midnight", backupCount=10)
        filelog_handler.setFormatter(stdfmt)
        self.logger.addHandler(filelog_handler)
        self.logHandler = filelog_handler

    def _set_syslog_log_env(self, logfile):
        stdfmt = logging.Formatter("[%(asctime)s] %(message)s")
        maxbytes = 1024 * 1024 * 100  # 10MB
        filelog_handler = handlers.RotatingFileHandler(logfile, maxBytes=maxbytes, backupCount=10)
        #filelog_handler = handlers.TimedRotatingFileHandler(logfile, when="midnight", backupCount=10)
        filelog_handler.setFormatter(stdfmt)
        self.logger.addHandler(filelog_handler)
        self.logHandler = filelog_handler

    def _set_console_log_env(self):
#         stdfmt = logging.Formatter("[%(asctime)s][%(levelname)-5s] %(message)s")
        stdfmt = logging.Formatter("[%(asctime)s][%(levelname)-5s] %(message)s [%(module)s.%(lineno)04d]")
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(stdfmt)
        self.logger.addHandler(stdout_handler)


