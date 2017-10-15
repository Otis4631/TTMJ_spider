import configparser

__all__ = ['ConfigParser']

class ConfigParser(object):
    def __init__(self, file_path=None, sections=None):
        self.file_path = file_path
        self.config = configparser.ConfigParser()
        self.sections = sections
        try:
            self.config.read(file_path)
        except TypeError:
            self.config = None
            print("can\'t open config file")

    def get(self, section=None, key=None,):
        if not section:
            return
        section = section.upper()
        if key:
            key = key.upper()
            res = None
            try:
                res = self.config.get(section, key)
            except configparser.NoSectionError:
                print('No section:', section)
            except configparser.NoOptionError:
                print('No option:', key, ' in section: ',section)
            finally:return res
        else:
            return dict(self.config.items(section))
