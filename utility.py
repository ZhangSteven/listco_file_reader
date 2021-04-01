# coding=utf-8
# 
from os.path import dirname, abspath, join
import logging, configparser
logger = logging.getLogger(__name__)



getCurrentDir = lambda: dirname(abspath(__file__))



def loadConfigFile(file):
	"""
	Read the config file, convert it to a config object.
	"""
	cfg = configparser.ConfigParser()
	cfg.read(join(getCurrentDir(), file))
	return cfg



getDataDirectory = lambda : \
	loadConfigFile('listco.config')['data']['directory']