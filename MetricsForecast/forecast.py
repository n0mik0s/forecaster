import os
import elasticsearch
import elasticsearch.helpers
import pandas as pd
import warnings
import json
import datetime

from fbprophet import Prophet

warnings.filterwarnings("ignore")

q_cpu = {
  "_source": [
    "cpu_total",
    "node",
    "@timestamp"],
  "sort": [
    {"@timestamp": {"order": "asc"}}
  ],
  "query": {
    "bool": {
      "must": [
        {"match": {"metricset_module": "system"}},
        {"match": {"metricset_name": "cpu"}},
        {"match": {"node": "node"}}
      ]
    }
  }
}

q_memory = {
  "_source": [
    "memory_actual_used",
    "node",
    "@timestamp"],
  "sort": [
    {"@timestamp": {"order": "asc"}}
  ],
  "query": {
    "bool": {
      "must": [
        {"match": {"metricset_module": "system"}},
        {"match": {"metricset_name": "memory"}},
        {"match": {"node": "node"}}
      ]
    }
  }
}

q_fs = {
  "_source": [
    "filesystem_used_pct",
    "filesystem_mount_point",
    "node",
    "@timestamp"],
  "sort": [
    {"@timestamp": {"order": "asc"}}
  ],
  "query": {
    "bool": {
      "must": [
        {"match": {"metricset_module": "system"}},
        {"match": {"metricset_name": "filesystem"}},
        {"match": {"node": "node"}}
      ]
    }
  }
}

qs = {'cpu': q_cpu,
      'fs': q_fs,
      'memory': q_memory}

fperfstat_map = {
    "mappings": {
        "fperfstat": {
            "properties": {
                "ds": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ssZ||yyyy-MM-dd"},
                "node": {"type": "keyword"},
                "metricset_name": {"type": "keyword"},
                "filesystem_mount_point": {"type": "keyword"},
                "filesystem_used_pct": {"type": "float"},
                "memory_actual_used": {"type": "float"},
                'cpu_total': {"type": "float"},
                "trend": {"type": "float"},
                "yhat_lower": {"type": "float"},
                "yhat_upper": {"type": "float"},
                "trend_lower": {"type": "float"},
                "trend_upper": {"type": "float"},
                "additive_terms": {"type": "float"},
                "additive_terms_lower": {"type": "float"},
                "additive_terms_upper": {"type": "float"},
                "daily": {"type": "float"},
                "daily_lower": {"type": "float"},
                "daily_upper": {"type": "float"},
                "weekly": {"type": "float"},
                "weekly_lower": {"type": "float"},
                "weekly_upper": {"type": "float"},
                "multiplicative_terms": {"type": "float"},
                "multiplicative_terms_lower": {"type": "float"},
                "multiplicative_terms_upper": {"type": "float"},
                "yhat": {"type": "float"}
            }
        }
    }
}

class MetricsForecast():
    def __init__(self, conf_yml, root_dir, node):
        self.root_dir = root_dir
        self.conf_yml = conf_yml
        self.node = node

        try:
            self.es_user = self.conf_yml['es_index_details']['user']
            self.es_pass = self.conf_yml['es_index_details']['password']
            self.es_hosts = self.conf_yml['es_cluster_nodes']
            self.es_port = self.conf_yml['es_conn_details']['port']
            self.es_cacerts = str(self.root_dir) + os.sep + str(self.conf_yml['es_conn_details']['ca_certs'])
            self.es_verify_certs = self.conf_yml['es_conn_details']['verify_certs']
            self.es_use_ssl = self.conf_yml['es_conn_details']['use_ssl']

            try:
                if self.es_use_ssl:
                    self.es_eng = elasticsearch.Elasticsearch(
                        self.es_hosts,
                        port=self.es_port,
                        http_auth=(self.es_user + ':' + self.es_pass),
                        verify_certs=True,
                        use_ssl=True,
                        ca_certs=self.es_cacerts
                    )
                else:
                    self.es_eng = elasticsearch.Elasticsearch(
                        self.es_hosts,
                        port=self.es_port,
                        http_auth=(self.es_user + ':' + self.es_pass)
                    )
            except Exception as exc:
                print('ERR: [metricsmean:init] Error with establishing connection to an elastic cluster:', exc)
                exit(1)
        except Exception as exc:
            print('ERR: [metricsmean:init] Error with config properties choosing:', exc)
            exit(1)

    def _eselect(self, query, index='abigdata_metricbeat-*', scroll='15s', size=1000, request_timeout=30):
        query = query
        index = index
        scroll = scroll
        size = size
        request_timeout = request_timeout

        try:
            self.scan_res = elasticsearch.helpers.scan(client=self.es_eng,
                                                   request_timeout=request_timeout,
                                                   query=query,
                                                   scroll=scroll,
                                                   size=size,
                                                   index=index,
                                                   clear_scroll=False,
                                                   raise_on_error=False)
        except Exception as err:
            print('ERR: [metricsmean:eselect]', err)
            return False
        else:
            return list(self.scan_res)

    def _einsert(self, arr):
        arr = arr
        index = self.conf_yml['es_index_details']['f_ipattern']
        type = list(fperfstat_map['mappings'].keys())[0]
        out_index_shards = self.conf_yml['es_index_details']['out_index_shards']
        out_index_replicas = self.conf_yml['es_index_details']['out_index_replicas']

        body = {
            "settings": {
                "number_of_shards": out_index_shards,
                "number_of_replicas": out_index_replicas
            },
            "mappings": fperfstat_map["mappings"]
        }

        actions = [
            {
                "_index": index,
                "_type": type,
                "_source": js
            }
            for js in arr
        ]

        if self.es_eng.indices.exists(index=index):
            try:
                sett = self.es_eng.indices.get_settings(index=index)
                creation_date = int(sett[index]['settings']['index']['creation_date'])
                current_ts = int(datetime.datetime.now().timestamp())
                delta = current_ts - creation_date

                if delta > 3600:
                    try:
                        self.es_eng.indices.delete(index=index)
                    except Exception as err:
                        print('ERR: [forecast:_einsert]', err)
                        return False
            except Exception as err:
                print('ERR: [forecast:_einsert]', err)
                return False
        else:
            try:
                self.es_eng.indices.create(index=index, body=body)
            except Exception as err:
                print('ERR: [forecast:_einsert]', err)
                return False
        
        try:
            elasticsearch.helpers.bulk(self.es_eng, actions, chunk_size=1000, request_timeout=30)
        except Exception as err:
            print('ERR: [forecast:_einsert]', err)
            return False
        else:
            return True

    def _df_from_arr(self, arr):
        arr = arr
        ds = []
        y = []

        for d in arr:
            k = list(d.keys())[0]
            v = d[k]
            ds.append(pd.to_datetime(k))
            y.append(v)

        df = pd.DataFrame.from_dict({'ds': ds, 'y': y})

        return (df)

    def _fb(self, df):
        df = df
        period = int(len(df.index)*0.2)

        m = Prophet(yearly_seasonality=False, daily_seasonality=True)
        m.fit(df)
        future = m.make_future_dataframe(periods=period)
        forecast = m.predict(future)

        return forecast

    def forecast(self):
        scroll = self.conf_yml['es_helpers_scan']['scroll'],
        size = self.conf_yml['es_helpers_scan']['size']
        request_timeout = self.conf_yml['es_helpers_scan']['request_timeout']

        dsumm = {'cpu': [],
                  'fs': {},
                  'memory': []}

        arr_to_einsert = []

        for item_metric in qs.keys():
            qs[item_metric]['query']['bool']['must'][2]['match']['node'] = str(self.node)
            res = self._eselect(qs[item_metric], scroll=scroll, size=size, request_timeout=request_timeout)

            for res_item in res:
                if item_metric in 'fs':
                    filesystem_mount_point = res_item['_source']['filesystem_mount_point']
                    filesystem_used_pct = res_item['_source']['filesystem_used_pct']
                    timestamp = res_item['_source']['@timestamp']

                    if filesystem_mount_point not in dsumm['fs'].keys():
                        dsumm['fs'][filesystem_mount_point] = []

                    dsumm['fs'][filesystem_mount_point].append({timestamp: filesystem_used_pct})
                    arr_to_einsert.append(json.dumps({'node': self.node,
                                                      'metricset_name': 'fs',
                                                      'filesystem_mount_point': filesystem_mount_point,
                                                      'filesystem_used_pct': filesystem_used_pct,
                                                      'ds': timestamp}))
                elif (item_metric in 'cpu'):
                    cpu_total = res_item['_source']['cpu_total']
                    timestamp = res_item['_source']['@timestamp']

                    dsumm['cpu'].append({timestamp: cpu_total})
                    arr_to_einsert.append(json.dumps({'node': self.node,
                                                      'metricset_name': 'cpu',
                                                      'cpu_total': cpu_total,
                                                      'ds': timestamp}))
                elif (item_metric in 'memory'):
                    memory_used = res_item['_source']['memory_actual_used']
                    timestamp = res_item['_source']['@timestamp']

                    dsumm['memory'].append({timestamp: memory_used})
                    arr_to_einsert.append(json.dumps({'node': self.node,
                                                      'metricset_name': 'memory',
                                                      'memory_actual_used': memory_used,
                                                      'ds': timestamp}))

        for metric in dsumm.keys():
            if metric in 'fs':
                for mpoint in dsumm['fs']:
                    df = self._df_from_arr(dsumm['fs'][mpoint])

                    if len(df.index) > 1:
                        fdf = self._fb(df)
                        fdf['metricset_name'] = 'fs'
                        fdf['node'] = self.node
                        fdf['filesystem_mount_point'] = mpoint
                        fjs = json.loads(fdf.to_json(orient='table', date_unit='s', index=False))
                        for item in fjs['data']:
                            arr_to_einsert.append(json.dumps(item))
            else:
                df = self._df_from_arr(dsumm[metric])

                if len(df.index) > 1:
                    fdf = self._fb(df)
                    fdf['metricset_name'] = metric
                    fdf['node'] = self.node
                    fjs = json.loads(fdf.to_json(orient='table', date_unit='s', index=False))
                    for item in fjs['data']:
                        arr_to_einsert.append(json.dumps(item))

        if self._einsert(arr_to_einsert):
            return True
        else:
            return False