import requests
import json
import os.path

USE_CACHE = True

fileNameRAW = 'data/petscan_data_all_enwiki_articles.txt'
fileName = 'data/petscan_data_all_wikidata_items.txt'
fileNameENWIKI = 'data/petscan_data_enwiki_article_list.txt'

#all articles with 'Template:WikiProject Medicine' in their talkpage @enwiki
petscanURL = 'https://petscan.wmflabs.org/?maxlinks=&search_wiki=&langs_labels_no=&regexp_filter=&interface_language=en&ores_type=any&before=&subpage_filter=either&format=json&links_to_no=&manual_list=&wikidata_label_language=&wpiu=any&referrer_url=&negcats=&labels_no=&wikidata_prop_item_use=&show_redirects=both&labels_any=&doit=Do%20it!&templates_yes=WikiProject%20Medicine&sitelinks_yes=&larger=&max_sitelink_count=&ores_prediction=any&wikidata_item=any&langs_labels_any=&source_combination=&ores_prob_from=&sortorder=ascending&sortby=none&depth=0&cb_labels_yes_l=1&labels_yes=&links_to_any=&referrer_name=&outlinks_yes=&smaller=&links_to_all=&ns%5B0%5D=1&after=&sparql=&categories=&edits%5Bbots%5D=both&edits%5Bflagged%5D=both&pagepile=&manual_list_wiki=&output_limit=&ores_prob_to=&langs_labels_yes=&project=wikipedia&search_max_results=500&templates_any=&page_image=any&wikidata_source_sites=&cb_labels_any_l=1&outlinks_any=&sitelinks_no=&max_age=&outlinks_no=&sitelinks_any=&common_wiki=auto&cb_labels_no_l=1&min_sitelink_count=&output_compatability=catscan&min_redlink_count=1&active_tab=tab_output&search_query=&minlinks=&edits%5Banons%5D=both&templates_use_talk_yes=on&combination=subset&language=en&common_wiki_other=&templates_no='

def getData():
	if USE_CACHE:
		reqData = open(fileNameRAW, 'r', encoding='utf-8').read()
	else:
		req = requests.get(petscanURL)
		with open(fileNameRAW, 'w', encoding='utf-8') as fileSave:
			fileSave.write(req.text)
		reqData = req.text

	parsed = json.loads(reqData)
	
	rawData = parsed['*'][0]['a']['*']

	#{"id":1525,"len":120173,"metadata":{"wikidata":"Q18216"},"n":"page","namespace":0,"nstext":"","q":"Q18216","title":"Aspirin","touched":"20200227091815"}
	wdItems = [f['q'] for f in rawData if 'q' in f and f['namespace'] == 0 and f['id']>0]
	enArticles = [f['title'] for f in rawData if f['namespace'] == 0 and f['id']>0]

	with open(fileName, 'w', encoding='utf-8') as fileSave:
		fileSave.write(json.dumps(wdItems))

	with open(fileNameENWIKI, 'w', encoding='utf-8') as fileSave:
		fileSave.write(json.dumps(enArticles))

if USE_CACHE and os.path.isfile(fileName) and os.path.isfile(fileNameENWIKI):
	exit()
else:
	getData()