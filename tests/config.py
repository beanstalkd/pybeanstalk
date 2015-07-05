import os, ConfigParser

class ConfigWrapper(object):
    def __init__(self, configfile, section):
        self.section = section
        self.config = ConfigParser.ConfigParser()
        self.config.read(configfile)
    def __getattr__(self, attr):
        return self.config.get(self.section, attr)

def get_config(section_name, configfile="tests.cfg"):
    if os.path.isfile(os.getcwd() + os.sep + configfile):
        configfile = os.getcwd() + os.sep + configfile
    elif os.path.isfile(os.path.dirname(__file__) + os.sep + configfile):
        configfile = os.path.dirname(__file__) + os.sep + configfile
    else:
        raise Exception("cannot find the test config file")
    return ConfigWrapper(configfile, section_name)
