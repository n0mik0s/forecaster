import argparse
import os
import multiprocessing as mp
import pprint

from GetConf.getconf import GetConf
from MetricsForecast.forecast import MetricsForecast

def mp_wrapper(list_to_wrapper):
    _conf_yml = list_to_wrapper[0]
    _node = list_to_wrapper[1]
    _root_dir = list_to_wrapper[2]

    _obj = MetricsForecast(conf_yml=_conf_yml, root_dir=_root_dir, node=_node)

    return _obj.forecast()

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(usage='%(prog)s [options]')
    argparser.add_argument('-c', '--conf',
                           help='Set full path to the configuration file.')
    argparser.add_argument('--version', action='version', version='%(prog)s 0.0')
    args = argparser.parse_args()

    root_dir = os.path.dirname(os.path.realpath(__file__))
    pp = pprint.PrettyPrinter(indent=4)

    if str(args.conf) in 'None':
        args.conf = str(root_dir) + os.sep + 'conf.yml'

    if os.path.isfile(args.conf):
        conf_yaml = GetConf(args.conf).get()
        if not conf_yaml:
            exit(1)
        if (len(conf_yaml['nodes_list']) < 1):
            print('INF: The list of nodes is empty. Nothing to do.')
            exit(0)

        if conf_yaml['multiprocessing']:
            if (conf_yaml['processess'] >= int(mp.cpu_count())):
                conf_yaml['processess'] = int(mp.cpu_count() - 2)

            with mp.Pool(conf_yaml['processess']) as pool:
                list_to_wrapper = list(zip([conf_yaml for x in range(len(conf_yaml['nodes_list']))], conf_yaml['nodes_list'], [root_dir for x in range(len(conf_yaml['nodes_list']))]))
                pool.map(mp_wrapper, list_to_wrapper)
        else:
            for node in conf_yaml['nodes_list']:
                obj = MetricsForecast(conf_yml=conf_yaml, root_dir=root_dir, node=node)
                obj.forecast()
    else:
        print('ERR: [main]: Please provide correct confpath.')
        exit(1)