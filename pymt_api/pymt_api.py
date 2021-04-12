# -*- coding: utf-8 -*-
import logging
import socket
import io
import sys
import numpy as np
import pandas as pd
from datetime import datetime
import pytz


@property
def is_connected(self) -> bool:
    return self.connected


def set_timeout(self,
                timeout_in_seconds: int = 60
                ):

    self.timeout_value = timeout_in_seconds
    self.sock.settimeout(self.timeout_value)
    self.sock.setblocking(1)
    return


def disconnect(self):

    self.sock.close()
    self.connected = False
    return True


def connect(self, **kwargs) -> bool:

    for k in ['server', 'port']:
        if k in kwargs.keys():
            setattr(self, k, kwargs[k])

    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.sock.setblocking(True)

    if self.debug:
        print(self.server, self.port)
    if len(self.instrument_list) == 0:
        self.socket_error_message = 'Broker Instrument list not available or empty'
        return False

    try:
        self.sock.connect((self.server, self.port))
        try:
            data_received = self.sock.recv(1000000)
            logging.info(
                f'Connected to socket-server: {self.sock} received data: {str(data_received):20s}')

            self.connected = True
            self.socket_error = 0
            self.socket_error_message = ''
            return True
        except socket.error as msg:
            self.socket_error = 100
            self.socket_error_message = f'Could not connect to server [{msg}].'
            self.connected = False
            return False
    except socket.error as msg:
        logging.error(
            f'Couldnt connect with the socket-server: {self.sock} terminating program {msg}')
        self.connected = False
        self.socket_error = 101
        self.socket_error_message = 'Could not connect to server.'
        return False

#################################################################################################


def get_table(self, command, cols, field, tz='UTC', **kwargs):
    ok, resp = self.send_command(command)
    if ok and resp[0:5] == command[0:5] and resp[-1] == '!':

        t0 = datetime(1970, 1, 1)
        t1 = datetime.now()
        if 'date_from' in kwargs.keys():
            t0 = kwargs['date_from']
        if 'date_to' in kwargs.keys():
            t1 = kwargs['date_to']

        df = pd.read_table(io.StringIO(resp[resp.index('#', 5)+1:-1]), sep='$', lineterminator='#',
                           header=None,
                           names=np.array(cols)[:, 0],
                           dtype=np.array(cols)[:, 1],
                           ).fillna('')
        for f in df.columns[df.columns.str.contains('_time')]:
            df[f] = pd.to_datetime(df[f], unit='s').dt.tz_localize(self.TZ_SERVER).dt.tz_convert(tz)
        query_filter = ((df[field] >= t0.astimezone()) & (df[field] <= t1.astimezone()))
        df = df.loc[query_filter]
    else:
        df = None

    return df


#################################################################################################

# F000
def check_connection(self) -> bool:
    fnc = sys._getframe().f_code.co_name
    self.command = self.commands[fnc]
    self.ret_desc = self.ret_descs[fnc]
    self.command_return_error = ''

    _ok, _resp = self.send_command(self.command)
    logging.debug(f'{_ok}, {_resp}')

    result = (_ok and _resp[5:7] == "OK")
    self.timeout = result
    self.command_OK = result

    return result


# F001
def get_static_account_info(self) -> dict:
    fnc = sys._getframe().f_code.co_name
    self.command = self.commands[fnc]
    self.ret_desc = self.ret_descs[fnc]
    self.command_return_error = ''
    _ok, _resp = self.send_command(self.command)
    logging.debug(f'{_ok}, {_resp}')

    if _ok and _resp[:5] == self.command[:5] and _resp[-2:] == '#!':
        self.command_OK = True
        result = self.resp2dict(_resp, self.ret_desc)
    else:
        self.command_return_error = _resp.split('#')[2]
        self.command_OK = False
        result = None

    return result


# F002
def get_dynamic_account_info(self) -> dict:
    fnc = sys._getframe().f_code.co_name
    self.command = self.commands[fnc]
    self.ret_desc = self.ret_descs[fnc]
    self.command_return_error = ''
    _ok, _resp = self.send_command(self.command)
    logging.debug(f'{_ok}, {_resp}')

    if _ok and _resp[:5] == self.command[:5] and _resp[-2:] == '#!':
        self.command_OK = True
        result = self.resp2dict(_resp, self.ret_desc)
    else:
        self.command_return_error = _resp.split('#')[2]
        self.command_OK = False
        result = None

    return result


# F003
def get_instrument_info(self,
                        instrument: str = 'EURUSD') -> dict:
    fnc = sys._getframe().f_code.co_name
    self.command = self.commands[fnc]
    self.ret_desc = self.ret_descs[fnc]
    self.command_return_error = ''

    if self.check_instrument_name(instrument.upper()) is None:
        return None
    
    self.command += '1#' + self.instrument + '#'
    _ok, _resp = self.send_command(self.command)
    logging.debug(f'{_ok}, {_resp}')

    if _ok and _resp[:5] == self.command[:5] and _resp[-2:] == '#!':
        self.command_OK = True
        result = {self.ret_desc[0][0]: str(self.instrument_name_universal)}
        _ret = self.resp2dict(_resp, self.ret_desc, 1)
        for k in _ret.keys():
            result[k] = _ret[k]
    else:
        self.command_return_error = _resp.split('#')[2]
        self.command_OK = False
        result = None

    return result


# F004
def check_instrument(self,
                     instrument: str = 'EURUSD') -> str:
    fnc = sys._getframe().f_code.co_name
    self.command = self.commands[fnc]
    self.ret_desc = self.ret_descs[fnc]
    self.command_return_error = ''

    if self.check_instrument_name(instrument.upper()) is None:
        return None

    self.command += '1#' + self.instrument + '#'
    _ok, _resp = self.send_command(self.command)
    logging.debug(f'{_ok}, {_resp}')

    result = _resp.split('#')[2]
    if _ok and _resp[:5] == self.command[:5] and _resp[-2:] == '#!':
        self.command_OK = True
        result = True, result 
    else:
        self.command_return_error = result
        self.command_OK = False
        result = False, result

    return result


# F005
def get_broker_server_time(self) -> datetime:
    fnc = sys._getframe().f_code.co_name
    self.command = self.commands[fnc]
    self.ret_desc = self.ret_descs[fnc]
    self.command_return_error = ''
    _ok, _resp = self.send_command(self.command)
    logging.debug(f'{_ok}, {_resp}')

    if _ok and _resp[:5] == self.command[:5] and _resp[-2:] == '#!':
        self.command_OK = True
        t0, t1 = _resp.split('#')[2:-1]
        result = (
            datetime.fromtimestamp(int(t0)).astimezone(pytz.timezone(self.TZ_SERVER)), 
            datetime.fromtimestamp(int(t0)).astimezone(pytz.timezone('UTC')))

    else:
        self.command_return_error = _resp.split('#')[2]
        self.command_OK = False
        result = None, None

    return result


# F007
def get_instruments(self) -> list:
    fnc = sys._getframe().f_code.co_name
    self.command = self.commands[fnc]
    self.ret_desc = self.ret_descs[fnc]
    self.command_return_error = ''
    _ok, _resp = self.send_command(self.command)
    logging.debug(f'{_ok}, {_resp}')

    if _ok and _resp[:5] == self.command[:5] and _resp[-2:] == '#!':
        self.command_OK = True
        result = _resp.split('#')[2:-1]
    else:
        self.command_return_error = _resp.split('#')[2]
        self.command_OK = False
        result = None

    return result


# F060
def get_open_orders(self, **kwargs):
    fnc = sys._getframe().f_code.co_name
    command = self.commands[fnc]
    cols = self.ret_descs[fnc]
    df = self.get_table(command, cols, 'open_time', **kwargs)
    return df


# F061
def get_open_positions(self, **kwargs):
    fnc = sys._getframe().f_code.co_name
    command = self.commands[fnc]
    cols = self.ret_descs[fnc]
    df = self.get_table(command, cols, 'open_time', **kwargs)
    return df


# F063
def get_closed_positions(self, **kwargs):
    fnc = sys._getframe().f_code.co_name
    command = self.commands[fnc]
    cols = self.ret_descs[fnc]
    df = self.get_table(command, cols, 'close_time', **kwargs)
    return df


# F065
def get_deleted_orders(self, **kwargs):
    fnc = sys._getframe().f_code.co_name
    command = self.commands[fnc]
    cols = self.ret_descs[fnc]
    df = self.get_table(command, cols, 'delete_time', **kwargs)
    return df


##################################################################################

def send_command(self,
                 command):
    self.command = command + "!"
    self.timeout = False
    self.sock.send(bytes(self.command, "utf-8"))
    try:
        data_received = ''
        while True:
            data_received = data_received + self.sock.recv(500000).decode()
            if data_received.endswith('!'):
                break
        return True, data_received
    except socket.timeout as msg:
        self.timeout = True
        print(msg)
        return False, None


def get_timeframe_value(self,
                        timeframe: str = 'D1') -> int:

    self.tf = 16408  # mt5.TIMEFRAME_D1
    timeframe.upper()
    if timeframe == 'MN1':
        self.tf = 49153  # mt5.TIMEFRAME_MN1
    if timeframe == 'W1':
        self.tf = 32769  # mt5.TIMEFRAME_W1
    if timeframe == 'D1':
        self.tf = 16408  # mt5.TIMEFRAME_D1
    if timeframe == 'H12':
        self.tf = 16396  # mt5.TIMEFRAME_H12
    if timeframe == 'H8':
        self.tf = 16392  # mt5.TIMEFRAME_H8
    if timeframe == 'H6':
        self.tf = 16390  # mt5.TIMEFRAME_H6
    if timeframe == 'H4':
        self.tf = 16388  # mt5.TIMEFRAME_H4
    if timeframe == 'H3':
        self.tf = 16387  # mt5.TIMEFRAME_H3
    if timeframe == 'H2':
        self.tf = 16386  # mt5.TIMEFRAME_H2
    if timeframe == 'H1':
        self.tf = 16385  # mt5.TIMEFRAME_H1
    if timeframe == 'M30':
        self.tf = 30  # mt5.TIMEFRAME_M30
    if timeframe == 'M20':
        self.tf = 20  # mt5.TIMEFRAME_M20
    if timeframe == 'M15':
        self.tf = 15  # mt5.TIMEFRAME_M15
    if timeframe == 'M12':
        self.tf = 12  # mt5.TIMEFRAME_M12
    if timeframe == 'M10':
        self.tf = 10  # mt5.TIMEFRAME_M10
    if timeframe == 'M6':
        self.tf = 6  # mt5.TIMEFRAME_M6
    if timeframe == 'M5':
        self.tf = 5  # mt5.TIMEFRAME_M5
    if timeframe == 'M4':
        self.tf = 4  # mt5.TIMEFRAME_M4
    if timeframe == 'M3':
        self.tf = 3  # mt5.TIMEFRAME_M3
    if timeframe == 'M2':
        self.tf = 2  # mt5.TIMEFRAME_M2
    if timeframe == 'M1':
        self.tf = 1  # mt5.TIMEFRAME_M1

    return self.tf


def resp2dict(self,
              resp: str,
              cols: list,
              shift: int = 1) -> dict:
    ret = {}
    vals = resp.split('#')[2:-1]
    keys = np.array(cols)[shift:, 0]
    dtypes = np.array(cols)[shift:, 1]
    for i, k in enumerate(keys):
        ret[k] = dtypes[i](vals[i])
    return ret


def get_broker_instrument_name(self,
                               instrumentname: str = '') -> str:
    self.command_return_error = ''
    result = 'none'
    if instrumentname in self.instrument_list.keys():
        self.command_OK = True
        result = self.instrument_list[instrumentname]
    else:
        self.command_OK = False
        self.command_return_error = 'Instrument not in broker list'
    return result


def get_universal_instrument_name(self,
                                  instrumentname: str = '') -> str:
    result = self.instruments_df.query(f'broker=="{instrumentname}"').universal
    if len(result) != 0:
        result = result.values[0]
        self.command_OK = True
    else:
        result = 'none'
        self.command_OK = False
        
    self.instrumentname = result
    return result


def check_instrument_name(self, instrumentname: str) -> str:
    self.instrument_name_universal = instrumentname.upper()
    self.instrument = self.get_broker_instrument_name(self.instrument_name_universal)
    if self.instrument is None or self.instrument == 'none':
        self.command_return_error = 'Instrument not in broker list'
        self.command_OK = False
        return None
    return self.instrument

