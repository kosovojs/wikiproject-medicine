import pymysql
import json
import time
import os
import sys
import yaml
from collections import defaultdict, OrderedDict
from datetime import datetime

import logging

from helpers import chunks, whereIn, encode_if_necessary

fileNameInput = 'data/wikidata_output.txt'


logging.basicConfig(filename='user-contrib.log', filemode='a+', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)


CHUNK_SIZE = 100

YEAR = '2019'

IS_DEV = True

def runQuery(conn, sql: str, params: tuple = (), maxTries: int = 2) -> dict:
	if maxTries == 0:
		return []

	with conn.cursor() as cursor:
		cursor.execute(sql, params)
		result = cursor.fetchall()
		return result
		
def connect(wiki):
	dbView = '{}_p'.format(wiki)

	try:
		if IS_DEV:
			config = yaml.safe_load(open('db_local.yaml'))
			conn = pymysql.connect(host=config['host'], user=config['user'], password=config['password'], port=config['port'], database=dbView, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
		else:
			conn = pymysql.connect( database=dbView, host=wiki+'wiki.web.db.svc.eqiad.wmflabs', read_default_file=os.path.expanduser("~/replica.my.cnf"), charset='utf8mb4' , cursorclass=pymysql.cursors.DictCursor)
		
		return conn
		
	except pymysql.Error as e:
		print('e:', e)
		exit()

def getPageIds(conn, pageTitles):
	sqlTpl = "select page_id from page where page_namespace=0 and page_title in ({})"

	chunkList = chunks(pageTitles,CHUNK_SIZE)

	pageIDs = []

	for chunk in chunkList:
		formattedTitles = [f.replace(' ','_') for f in chunk]
		
		sqlTemplateFormatted = sqlTpl.format(whereIn(formattedTitles))

		result = runQuery(conn, sqlTemplateFormatted, tuple(formattedTitles))

		pageIDs.extend([f['page_id'] for f in result if 'page_id' in f])

		#print(result)
	
	return pageIDs

def getUserNames(conn, actorIDs):
	sqlTpl = "select actor_id, actor_name from actor_revision where actor_id in ({})"

	actorIDs = list(set(actorIDs))

	chunkList = chunks(actorIDs,CHUNK_SIZE)

	wikiContribs = {}

	for chunk in chunkList:
		sqlTemplateFormatted = sqlTpl.format(whereIn(chunk))

		result = runQuery(conn, sqlTemplateFormatted, tuple(chunk))

		formattedResult = {f['actor_id']:encode_if_necessary(f['actor_name']) for f in result}

		wikiContribs.update(formattedResult)

	return wikiContribs
	
	#return pageIDs

def getContributors(conn, wiki, pageIDs):
	sqlTpl = "select rev_actor, count(*) as COUNT from revision where rev_page IN ({}) and  rev_timestamp like %s group by rev_actor"

	chunkList = chunks(pageIDs,CHUNK_SIZE)

	wikiContribs = {}

	for chunk in chunkList:
		sqlTemplateFormatted = sqlTpl.format(whereIn(chunk))

		sqlParams = chunk + ['{}%'.format(YEAR)]

		result = runQuery(conn, sqlTemplateFormatted, tuple(sqlParams))

		formattedResult = {f['rev_actor']:f['COUNT'] for f in result}

		for userID in formattedResult:
			userID = int(userID)
			if userID in wikiContribs:
				wikiContribs[userID] += formattedResult[userID]
			else:
				wikiContribs.update({userID: formattedResult[userID]})

	userNames = getUserNames(conn, wikiContribs.keys())
	
	retData = []

	for user in wikiContribs:
		userName = userNames[user] if user in userNames else 'UNKNOWN - {}'.format(user)

		retData.append('\t'.join(map(str,[wiki, userName, wikiContribs[user]])))
	
	return '\n'.join(retData)

inputData = json.loads(open(fileNameInput,'r',encoding='utf-8').read())

wikisBySize = sorted(inputData.items(), key=lambda item: len(item[1]))

filesave = open('data/2019-top-editors-raw.txt', 'a', encoding='utf-8')

for wikipedia in wikisBySize:
	wikiLang, articles = wikipedia
	if wikiLang == 'enwiki': continue#special handling

	startTime = time.time()

	logging.info("Started {}".format(wikiLang))

	connection = connect(wikiLang)

	pageIDList = getPageIds(connection, articles)
	contributors = getContributors(connection, wikiLang, pageIDList)

	filesave.write(contributors+'\n')

	endTime = time.time()

	logging.info("Finished {}; time: {}; articles: {}".format(wikiLang, round(endTime - startTime, 4), len(articles)))