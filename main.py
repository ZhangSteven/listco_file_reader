# coding=utf-8
#
# Read ListCo NAV files and output their NAV, management fee, fixed
# income&cash to a csv file.
# 
from steven_utils.excel import fileToLines, getRawPositionsFromLines
from steven_utils.utility import mergeDict, writeCsv, dictToValues
from steven_utils.iter import firstOf
from steven_utils.file import getFiles
from listco_file_reader.utility import getDataDirectory
from toolz.itertoolz import groupby as groupbyToolz
from toolz.functoolz import compose
from functools import partial
from itertools import dropwhile, chain
from datetime import datetime
from os.path import join
import logging
logger = logging.getLogger(__name__)



"""

1. read date, positions from file.
2. group positions by account number.
3. per account number, get NAV, management fee, fixed income&cash.

file -> [Iterable] ([Dictionary] position)

Each position:

Date, Account Number, NAV, Management Fee, Fixed Income & Cash

files -> [Iterable] ([Dictioanry]) positions

"""
def readNavFilesFromDirectory(directory):
	"""
	[String] directory => [Iterable] ([Dictionary]) account data
	"""
	return \
	compose(
		chain.from_iterable
	  , partial(map, readNavFile)
	  , lambda directory: getFiles(directory, True)
	)(directory)



def readNavFile(file):
	"""
	[String] file => [Iterable] ([Dictionary]) account data
	"""
	logger.debug('readNavFile(): {0}'.format(file))
	lines = fileToLines(file)
	date = getDateFromLines(lines)
	positions = getPositionsFromLines(lines)
	positions = map(lambda p: mergeDict(p, {'Date': date}), positions)
	positionGroups = groupbyToolz(lambda p: p['Account Number'], positions).values()
	return map(getAccountDataFromPositions, positionGroups)



def getDateFromLines(lines):
	"""
	[Iterable] lines => [String] date (yyyy-mm-dd)
	"""
	def getDate(dtString):
		"""
		[String] dtString => [String] date (yyyy-mm-dd)
		
		dtString: As Of Date: 04-Jan-2021
		"""
		return \
		compose(
			lambda dt: dt.strftime('%Y-%m-%d')
		  , lambda s: datetime.strptime(s, '%d-%b-%Y')
		  , lambda L: L[1].strip()
		  , lambda s: s.split(':')
		)(dtString)


	return \
	compose(
		getDate
	  , lambda line: line[0]
	  , partial( firstOf
	  		   , lambda line: len(line) > 0 and line[0].startswith('As Of Date:')
	  		   )
	)(lines)



def getPositionsFromLines(lines):
	"""
	[Iterable] lines => [Iterable] ([Dictionary]) positions
	"""
	return \
	compose(
		getRawPositionsFromLines
	  , partial( dropwhile
	  		   , lambda line: len(line) > 0 and not line[0].startswith('Account Number')
	  		   )
	)(lines)



def getAccountDataFromPositions(group):
	"""
	[List] group of positions for an account
		=> [Dictionary] account data
	"""
	def getManagementFee(positions):
		"""
		[List] positions => [Float] management fee
		"""
		return \
		compose(
			lambda p: 0 if p is None else p['Cost Local']
		  , partial( firstOf
		  		   , lambda p: p['Security Description 1'].lower().startswith('management fee')
		  		   )
		)(positions)


	def getFixedIncomeCash(positions):
		"""
		[List] positions => [Float] fixed income & cash
		"""
		return \
		compose(
			sum
		  , partial( map
		  		   , lambda x: 0 if isinstance(x, str) and x.strip() == '' \
		  		   				else float(x)
		  		   )
		  , partial(map, lambda p: 0 if p['Account Number'] == 'G 26810' and p['Report Currency Code'] == 'CNY' \
		  							else p['Market Value Local'])
		)(positions)


	return { 'Date': group[0]['Date']
		   , 'Account': group[0]['Account Number']
		   , 'NAV': group[0]['Net Asset Market Value']
		   , 'ManagementFee': getManagementFee(group)
		   , 'FixedIncomeCash': getFixedIncomeCash(group)
		   }



def getHeaders():
	return ('Date', 'Account', 'NAV', 'ManagementFee', 'FixedIncomeCash')




if __name__ == "__main__":
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)

	# import argparse
	# parser = argparse.ArgumentParser(description='Handle ListCo NAV files')
	# parser.add_argument( 'file', metavar='file name', type=str
	# 				   , help="NAV file")

	logger.debug('main(): start')
	compose(
		print
	  , lambda positions: \
			writeCsv( 'output.csv'
					, chain( [getHeaders()]
	  		   			   , map(partial(dictToValues, getHeaders()), positions)
	  		   			   )
	  			    )
	  , readNavFilesFromDirectory
	)(getDataDirectory())
