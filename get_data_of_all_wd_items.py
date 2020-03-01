from pywikiapi import wikipedia
import json
from helpers import chunks

fileNameInput = 'data/petscan_data_all_wikidata_items.txt'
fileOutputRaw = 'data/wikidata_raw_output.txt'
fileOutput = 'data/wikidata_output.txt'

site = wikipedia('www','wikidata')

CHUNK_SIZE = 50

batchCounter = 0

ALL_DATA = {}
BY_LANG = {}

def formatData():
	for entity in ALL_DATA:
		for wiki in ALL_DATA[entity]:
			if wiki in BY_LANG:
				BY_LANG[wiki].append(ALL_DATA[entity][wiki])
			else:
				BY_LANG[wiki] = [ALL_DATA[entity][wiki]]

def oneBatch(wdItems):
	global batchCounter
	batchCounter += 1
	print(batchCounter)

	reqData = site('wbgetentities', props="sitelinks", ids=wdItems,redirects='yes')['entities']
	for entity in reqData:
		entData = reqData[entity]
		currSitelinks = {}

		if 'sitelinks' in entData:
			currSitelinks = {f:entData['sitelinks'][f]['title'] for f in entData['sitelinks'] if f.endswith('wiki') and not f == 'commonswiki'}
		
		ALL_DATA.update({entity: currSitelinks})
	

with open(fileNameInput, 'r', encoding='utf-8') as inputFile:
	wdItemList = json.loads(inputFile.read())

	allChunks = chunks(wdItemList, CHUNK_SIZE)

	for chunk in allChunks:
		oneBatch(chunk)
		
with open(fileOutputRaw, 'w', encoding='utf-8') as fileSave:
	fileSave.write(json.dumps(ALL_DATA, ensure_ascii=False))

formatData()

with open(fileOutput, 'w', encoding='utf-8') as fileSave:
	fileSave.write(json.dumps(BY_LANG, ensure_ascii=False))
