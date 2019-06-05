import yaml

class GetConf():

    def __init__(self, file):
        self.file = file

    def get(self):
        with open(self.file, 'r') as self.stream:
            try:
                self.conf_yaml = yaml.full_load(self.stream)
            except yaml.YAMLError as err:
                print('ERR: [GetConf:get]', err)
                return False
            else:
                return self.conf_yaml