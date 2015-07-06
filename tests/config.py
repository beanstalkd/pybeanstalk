import os, ConfigParser

class ConfigWrapper(object):
    def __init__(self, configfile, section):
        self.section = section
        self.config = ConfigParser.ConfigParser()
        self.config.read(configfile)
    def __getattr__(self, attr):
        return self.config.get(self.section, attr)

def get_config(section_name, configfile="tests.cfg"):
    "check current working directory & the tests/ subdir for config file"
    cfg_in_current = os.path.join(os.getcwd(), configfile)
    cfg_in_tests = os.path.join(os.path.dirname(__file__), configfile)

    if os.path.isfile(cfg_in_current):
        configfile = cfg_in_current
    elif os.path.isfile(cfg_in_tests):
        configfile = cfg_in_tests
    else:
        raise Exception("cannot find the test config file")
    return ConfigWrapper(configfile, section_name)
