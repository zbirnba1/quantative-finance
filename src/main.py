import logging
import metrics
import pulldata
import inspect
import robinhoodtransfer
import traderobinhood
import recommended_portfolios
import sys
import performance
import os

class StreamToLogger(object): #https://www.electricmonk.nl/log/2011/08/14/redirect-stdout-and-stderr-to-a-logger-in-python/
   """
   Fake file-like stream object that redirects writes to a logger instance.
   """
   def __init__(self, logger, log_level=logging.INFO):
	  self.logger = logger
	  self.log_level = log_level
	  self.linebuf = ''

   def write(self, buf):
	  for line in buf.rstrip().splitlines():
		 self.logger.log(self.log_level, line.rstrip())

def main(config_file=None):
	if config_file is None:
		logging.error('must specify config file as first paramater, exiting')
		exit()
	logging.info('main started')
	logging.info('pulling data started')
	pulldata.main(config_file=config_file)
	logging.info('pulling data finished')
	logging.info('starting metrics')
	metrics.main(config_file=config_file)
	logging.info('finished metrics')
	logging.info('getting recommended portfolio')
	recommended_portfolios.main(config_file=config_file)
	logging.info('finished getting recommended portfolio')
	logging.info('transfering to robinhood')
	robinhoodtransfer.main(config_file=config_file)
	logging.info('finished transfering to robinhood')
	logging.info('buying and selling stocks')
	traderobinhood.main(config_file=config_file)
	logging.info('finished buying and selling')
	logging.info('updating spreadsheets')
	performance.main(config_file=config_file)
	logging.info('finished updating spreadsheets')
	logging.info('main finished')

if __name__ == '__main__':
	if os.path.exists(inspect.stack()[0][1].replace('py','log')):
		os.remove(inspect.stack()[0][1].replace('py','log'))
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	stdout_logger = logging.getLogger('STDOUT')
	sl = StreamToLogger(stdout_logger, logging.INFO)
	sys.stdout = sl
	stderr_logger = logging.getLogger('STDERR')
	sl = StreamToLogger(stderr_logger, logging.ERROR)
	sys.stderr = sl
	if len(sys.argv)==1:
		config_file='finance_cfg.cfg'
	else:
		config_file=sys.argv[1]
	main(config_file=config_file)