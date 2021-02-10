## default LIB

## open LIB
import pymysql, warnings
from time import sleep

MAX_RECONN = 3
MAX_RETRY_LOCK = 3

class DBManager():
    
    def __init__(self, _host, _port, _id, _pw, _logger=None, _db=None, _to=10):
        self.host = _host
        self.port = _port
        self.id = _id
        self.pw = _pw
        self.to = _to
        self.dbName = _db
        self.logger = _logger
        self.conn = pymysql.connect(host=_host, port=_port, user=_id, passwd=_pw, 
                                    connect_timeout=_to, use_unicode=True, charset="utf8", db=_db)
    
    def __del__(self):
        self.logger = None
        if self.conn != None : 
            if self.conn.open : self.conn.close()
        self.conn = None
    
    def reconn(self):
        if self.conn.open :
            self.conn.close()
        self.conn = pymysql.connect(host=self.host, port=self.port, user=self.id, passwd=self.pw, 
                                    connect_timeout=self.to, use_unicode=True, charset="utf8", db=self.dbName)
        return True
    
    def chk_conn(self):
        for _i in range(MAX_RECONN):
            try:
                self.conn.ping(False)
                return True
            except Exception as e:
                if self.logger != None : self.logger.warning("FAIL: DB PING, retry..., retry=%s"%str(_i))
            
            try:
                self.conn.ping(True)
                if self.logger != None : self.logger.info("SUCC: DB PING, retry=%s"%str(_i))
                return True
            except Exception as e:
                if self.logger != None : self.logger.warning("FAIL: DB PING, retry fail..., retry=%s"%str(_i))
            
            sleep(1)
        
        if self.logger != None : self.logger.error("FAIL: DB Conn Chk Error, retry=%s"%str(MAX_RECONN))
        return False
    
    def commit(self):
        self.conn.commit()
    
    def select(self, sql, _params=(), _all=True, _commit=True):
        if self.chk_conn() : 
            if _commit: self.conn.commit()
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(sql, _params)
                    if _all:
                        _result = cursor.fetchall()
                    else:
                        _result = cursor.fetchone()
                if _commit: self.conn.commit()
                return True, _result
            except Exception as e:
                if self.logger != None : self.logger.error("FAIL: DB SELECT ERROR, sql=%s, params=%s"%( str(sql), str(_params) ))
                if self.logger != None : self.logger.exception(e)
                return False, str(e)
        else:
            return False, "No DB-Conn"
    
    def insert(self, sql, _params=(), _commit=True):
        if self.chk_conn() : 
            if _commit: self.conn.commit()
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    with self.conn.cursor() as cur:
                        _retry = 0
                        while True:
                            try:
                                cur.execute(sql, _params)
                                rcnt = cur.rowcount
                                rid = cur.lastrowid
                                break
                            except pymysql.err.OperationalError as dbe:
                                _errno = dbe.args[0]
                                if (_errno == 1213) and (_retry < MAX_RETRY_LOCK) :
                                    _retry += 1
                                    if self.logger != None : 
                                        self.logger.warning("FAIL: DB INSERT LOCK, retry=%s/%s, sql=%s, param=%s, rcnt=%s, rid=%s"%( 
                                                            str(_retry), str(MAX_RETRY_LOCK), str(sql), str(_params), str(cur.rowcount), str(cur.lastrowid) ))
                                    sleep(0.3)
                                else:
                                    raise dbe
                        
                    if _commit: self.conn.commit()
                    return True, rcnt, rid
                except Exception as e:
                    if self.logger != None : self.logger.error("FAIL: DB INSERT ERROR, sql=%s, params=%s"%( str(sql), str(_params) ))
                    if self.logger != None : self.logger.exception(e)
                    return False, str(e), None
        else:
            return False, "No DB-Conn", None
    
    def update(self, sql, _params=(), _commit=True):
        if self.chk_conn() : 
            if _commit: self.conn.commit()
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    with self.conn.cursor() as cursor:
                        _retry = 0
                        while True:
                            try:
                                cursor.execute(sql, _params)
                                rcnt = cursor.rowcount
                                rid = cursor.lastrowid
                                break
                            except pymysql.err.OperationalError as dbe:
                                _errno = dbe.args[0]
                                if (_errno == 1213) and (_retry < MAX_RETRY_LOCK) :
                                    _retry += 1
                                    if self.logger != None : 
                                        self.logger.warning("FAIL: DB UPDATE LOCK, retry=%s/%s, sql=%s, param=%s, rcnt=%s, rid=%s"%( 
                                                            str(_retry), str(MAX_RETRY_LOCK), str(sql), str(_params), str(cursor.rowcount), str(cursor.lastrowid) ))
                                    sleep(0.3)
                                else:
                                    raise dbe
                    if _commit: self.conn.commit()
                    return True, rcnt, rid
                except Exception as e:
                    if self.logger != None : self.logger.error("FAIL: DB UPDATE ERROR, sql=%s, params=%s"%( str(sql), str(_params) ))
                    if self.logger != None : self.logger.exception(e)
                    return False, str(e), None
        else:
            return False, "No DB-Conn", None
    
    def exec_sql(self, sql, _params=(), _commit=True):
        _reconn = False
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            while True:
                try:
                    if _commit: self.conn.commit()
                    with self.conn.cursor() as cursor:
                        _retry = 0
                        while True:
                            try:
                                cursor.execute(sql, _params)
                                rcnt = cursor.rowcount
                                rid = cursor.lastrowid
                                break
                            except pymysql.err.OperationalError as dbe:
                                _errno = dbe.args[0]
                                if (_errno == 1213) and (_retry < MAX_RETRY_LOCK) :
                                    _retry += 1
                                    if self.logger != None : 
                                        self.logger.warning("FAIL: DB EXEC LOCK, retry=%s/%s, sql=%s, param=%s, rcnt=%s, rid=%s"%( 
                                                            str(_retry), str(MAX_RETRY_LOCK), str(sql), str(_params), str(cursor.rowcount), str(cursor.lastrowid) ))
                                    sleep(0.3)
                                else:
                                    raise dbe
                    if _commit: self.conn.commit()
                    return True, rcnt, rid
                except Exception as e:
                    if self.logger != None : self.logger.error("FAIL: DB EXEC ERROR, sql=%s, params=%s"%( str(sql), str(_params) ))
                    if self.logger != None : self.logger.exception(e)
                    if not _reconn :
                        self.reconn()
                        _reconn = True
                    else:
                        return False, str(e), None
    
    def delete(self, sql, _params=()):
        if self.chk_conn() : 
            self.conn.commit()
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(sql, _params)
                    rcnt = cursor.rowcount
                self.conn.commit()
                return True, rcnt
            except Exception as e:
                if self.logger != None : self.logger.error("FAIL: DB DELETE ERROR, sql=%s, params=%s"%( str(sql), str(_params) ))
                if self.logger != None : self.logger.exception(e)
                return False, str(e)
        else:
            return False, "No DB-Conn"
    
    
    def close(self):
        if self.conn is not None : 
            if self.conn.open : 
                self.conn.close()
            self.conn = None
    
    def conn_port(self):
        if self.conn is not None and self.conn._sock is not None :  
            _sname = self.conn._sock.getsockname()
            if len(_sname) == 2:
                return _sname[1]
            return _sname
        else: return None


