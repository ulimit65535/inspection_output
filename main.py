# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import asyncio
import datetime
import json
import sys

import aiohttp
import pandas as pd
import logging
import pytz
import xlrd
import yaml


logging.basicConfig(stream=sys.stdout, format='[%(levelname)s %(asctime)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)


class BasicConfig:
    """该类初始化一些配置"""
    def __init__(self, config_file='config.yml', host_file='ip_list.xls'):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config = yaml.load(f.read(), Loader=yaml.FullLoader)
        except Exception as e:
            logging.error('加载配置文件失败:{}'.format(e))
            sys.exit(1)

        try:
            workbook = xlrd.open_workbook(host_file)
            sheet = workbook.sheets()[0]
            self.ip_list = sheet.col_values(0)
        except Exception as e:
            logging.error('加载主机列表文件失败:{}'.format(e))
            sys.exit(1)

    def get_config(self):
        return self.config

    def get_ip_list(self):
        return self.ip_list

    def get_bk_config(self):
        try:
            bk_config = self.config['bk']
            if 'cookie' not in bk_config:
                logging.error('缺少cookie配置项.')
                sys.exit(1)
            if 'csrf_token' not in bk_config:
                logging.error('缺少csrf_token配置项.')
                sys.exit(1)
            if 'max_threads' not in bk_config:
                logging.error('缺少max_threads配置项.')
                sys.exit(1)
            return bk_config
        except Exception as e:
            logging.error('缺少bk配置项:{}'.format(e))


class BkRoles(object):
    def __init__(self, ip_list, bk_config):
        self._cookie = bk_config['cookie']
        self._csrf_token = bk_config['csrf_token']
        self._ip_list = ip_list
        self._max_threads = bk_config['max_threads']
        self._host = bk_config['host']
        self._index_id_dict = {
            'cpu总使用率': 7,
            '应用内存使用率': 99,
            '数据盘使用率': 81
        }
        self._bk_biz_id = 102
        self._url = self._host + '/o/bk_monitor/rest/v1/performance/host_index/graph_point/'
        self.results = []
        utc_tz = pytz.timezone('Asia/Shanghai')
        now = datetime.datetime.now(tz=utc_tz)
        ago_1h = now - datetime.timedelta(hours=1)
        self._time_range = "{} -- {}".format(ago_1h.isoformat(), now.isoformat())

    def _get_header(self):
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/81.0.4044.138 Safari/537.36',
            'Accept-Encoding': 'gzip',
            'Cookie': self._cookie,
            'referer': self._host + '/o/bk_monitor/102/bp/',
            'X-CSRFToken': self._csrf_token
        }

    # 获取结果
    async def get_results(self, ip):
        host_id = '{}|2'.format(ip)
        request_param = {
            'bk_biz_id': self._bk_biz_id,
            'dimension_field_value': "",
            'host_id': host_id,
            'time_range': self._time_range,
            'time_step': 1
        }
        result_dict = {}
        for key, value in self._index_id_dict.items():
            request_param['index_id'] = value
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(self._url, data=request_param, headers=self._get_header()) as response:
                        data = await response.read()
                        data_list = json.loads(data)['data']['data']['series']
                        if key == '数据盘使用率':
                            mount_point_list = []
                            for data in data_list:
                                if data['name'].startswith('[挂载点: /data') or data['name'].startswith('[挂载点: /apps'):
                                    data_list_new = []
                                    for item in data['data']:
                                        try:
                                            item = float(item[1])
                                            data_list_new.append(item)
                                        except:
                                            pass
                                    data_max = max(data_list_new)
                                    mount_point_list.append(data_max)
                            if mount_point_list:
                                data_max = max(mount_point_list)
                                result_dict[key] = str(data_max) + "%"
                            else:
                                data_max = '/'
                                result_dict[key] = data_max
                        else:
                            data = data_list[0]
                            item_dict = {}
                            item_dict['data'] = data['data']
                            if data['name'] == '总览':
                                item_dict['name'] = key
                            else:
                                item_dict['name'] = data['name']
                            data_list_new = []
                            for item in item_dict['data']:
                                try:
                                    item = float(item[1])
                                    data_list_new.append(item)
                                except:
                                    pass
                            data_max = max(data_list_new)
                            if item_dict['name']:
                                result_dict[item_dict['name']] = str(data_max) + "%"
                            else:
                                result_dict[key] = str(data_max) + "%"
            except Exception:
                await asyncio.sleep(self._max_threads)  # 这里
        result_dict['ip'] = ip
        self.results.append(result_dict)

    # 处理任务（从队列中获取链接）
    async def handle_tasks(self, task_id, work_queue):
        while not work_queue.empty():
            current_ip = await work_queue.get()
            try:
                task_status = await self.get_results(current_ip)
            except Exception:
                logging.exception('Error for {}'.format(current_ip), exc_info=True)

    def eventloop(self):
        q = asyncio.Queue()  # 队列
        [q.put_nowait(ip) for ip in self._ip_list]  # 直接放进队列中
        #  ------------------------------------------------------
        event_loop = asyncio.get_event_loop()  # 获取事件循环
        tasks = [self.handle_tasks(task_id, q, ) for task_id in range(self._max_threads)]
        # -------------------------------------------------------
        try:
            # 用这个协程启动循环，协程返回时这个方法将停止循环。
            event_loop.run_until_complete(asyncio.wait(tasks))
        except KeyboardInterrupt:
            for task in asyncio.Task.all_tasks():
                task.cancel()
            event_loop.stop()
        finally:
            event_loop.close()


if __name__ == '__main__':
    basic_config = BasicConfig()
    ip_list = basic_config.get_ip_list()
    bk_config = basic_config.get_bk_config()

    bk = BkRoles(ip_list, bk_config)
    bk.eventloop()

    pf = pd.DataFrame(list(bk.results))
    order = ['ip', 'cpu总使用率', '应用内存使用率', '数据盘使用率']
    pf = pf[order]
    pf.to_excel('output/result.xls', encoding='utf-8', index=False)