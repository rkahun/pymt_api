# -*- coding: utf-8 -*-
import os
import logging
import yaml
import pandas as pd
from datetime import datetime

DIR = os.path.split(__file__)[0]

__all__ = ['PyMT_API']

class PyMT_API:

    __version__: str = '0.0.7'

    from .vars import (
        commands, ret_descs
    )

    from .pymt_api import (
        is_connected, set_timeout, connect, disconnect,
        get_table, resp2dict, check_instrument_name,
        check_connection, get_static_account_info, get_dynamic_account_info,
        get_instrument_info, check_instrument, get_instruments,
        get_broker_server_time, send_command,
        get_open_orders, get_deleted_orders, get_open_positions, get_closed_positions,
        get_timeframe_value, get_broker_instrument_name, get_universal_instrument_name
    )

    def __init__(self, **kwargs):
        self.agent: str = 'default'
        self.config_file: str = os.path.join(DIR, 'config', 'config.yaml')
        self.config: dict = {}
        self.server: str = '127.0.0.1'
        self.port: int = 9876
        self.TZ_SERVER: str = 'UTC'
        self.TZ_LOCAL:  str = 'UTC'
        self.logfile: str = 'pymt_api.log'
        self.loglevel: str = 'WARN'
        self.logformat: str = '%(asctime)-15s %(levelname)-6s %(name)s: %(message)s'
        
        self.socket_error: int = 0
        self.socket_error_message: str = ''
        self.order_return_message: str = ''
        self.order_error: int = 0
        self.connected: bool = False
        self.timeout: bool = False
        self.command: str = ''
        self.command_OK: bool = False
        self.command_return_error: str = ''
        self.debug: bool = False
        self.max_bars: int = 5000
        self.max_ticks: int = 5000
        self.timeout_value: int = 60
        self.instrument_list: dict = {}
        self.instrument_name_broker: str = ''
        self.instrument_name_universal: str = ''
        self.instrument: str = ''
        self.date_from: datetime = datetime(1970, 1, 1).astimezone()
        self.date_to: datetime = datetime.now().astimezone()
        self.TZ_UTC: str = 'UTC'
        
        if 'agent' in kwargs.keys(): self.agent = kwargs['agent']
        if 'config_file' in kwargs.keys():
            self.config_file = kwargs['config_file']
        with open(self.config_file, 'r') as f:
            _config = yaml.load(f, Loader=yaml.FullLoader)[self.agent]
            for k, v in _config.items():
                setattr(self, k, v)
                
        for k, v in kwargs.items():
            if k != 'config':
                setattr(self, k, v)  
                
        _df = pd.DataFrame(self.instrument_list, index=[0]).transpose().reset_index()
        _df.columns = ['universal', 'broker']
        self.instruments_df = _df
        
        logging.basicConfig(filename=self.logfile, format=self.logformat,
                            level=self.loglevel, encoding='utf-8')

        logging.info(f'Initialized. ({self.agent:s} agent, {len(self.instruments_df):d} instruments)')

