import ConfigParser

class ConfigWrapper(object):
    def __init__(self, configfile, section):
        self.section = section
        self.config = ConfigParser.ConfigParser()
        self.config.read(configfile)
    def __getattr__(self, attr):
        return self.config.get(self.section, attr)

def get_config(section_name, configfile="tests.cfg"):
    return ConfigWrapper(configfile, section_name)
