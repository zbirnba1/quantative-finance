import logging
import configparser
import os
import inspect

class ConfigWrapper():
	def __init__(self,config_file=None):
		if config_file is None or not os.path.exists(config_file):
			logging.error('config_file does not exist:'+str(config_file))
			exit()
		else:
			config=configparser.ConfigParser()
			config.read(config_file)
			logging.info('reading config file:'+config_file)
			self.config=config
		return
	def check_section_exists(self,section):
		if section not in self.get_sections():
			return False
		else:
			return True
	def check_option_exists(self,section,option):
		if self.check_section_exists(section=section) is False:
			return False
		else:
			if option not in self.get_options(section=section):
				return False
			else:
				return True
	def get_string(self,section,option):
		if not self.check_section_exists(section=section) or not self.check_option_exists(section=section,option=option):
			return None
		else:
			return str(((self.config.get(section,option)).split('#'))[0]).strip()
	def get_int(self,section,option):
		if not self.check_section_exists(section=section) or not self.check_option_exists(section=section,option=option):
			return None
		else:
			return int(self.get_string(section,option))
	def get_bool(self,section,option):
		result=self.get_string(section,option)
		if result is None:
			return None
		if result.lower() in ['y','yes','true']:
			return True
		elif result.lower() in ['n','no','false']:
			return False
		else:
			logging.error('unknown bool value:'+str(result))
			return None

	def get_sections(self):
		return [str(x) for x in self.config.sections()]
	def get_options(self,section):
		return [str(x) for x in self.config.options(section=section)]
if __name__ == '__main__':
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	c=ConfigWrapper('finance_cfg.cfg')
	print c.get_string('bob','bob')
