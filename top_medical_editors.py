import ipaddress
import operator

data1 = 'data/2019-top-editors-raw.txt'
data2 = 'data/2019-top-editors-raw-enwiki.txt'

fileOutput = 'data/2019-top-editors-final.txt'

mas = {}

def isIP(inp):
	try:
		ip = ipaddress.ip_address(inp)
		return True
	except ValueError:
		return False

def sortResults():
	retArr = []

	for user in mas:
		userData = mas[user]
		homeWiki = max(userData.items(), key=operator.itemgetter(1))[0]
		editCount = sum(userData.values())
		retArr.append([user, homeWiki, editCount])

	retArr.sort(key=lambda usr: (-usr[2], user, homeWiki))

	return retArr

def formatData(filename):
	rows = open(filename, 'r', encoding='utf-8').read()
	rows = [f.split('\t') for f in rows.split('\n') if len(f)>2]

	for row in rows:
		wiki, user, count = row

		if isIP(user):
			continue
		
		count = int(count)

		if user in mas:
			mas[user].update({wiki: count})
		else:
			mas[user] = {wiki: count}

formatData(data1)
formatData(data2)

sortedRes = sortResults()

sortedRes = ['\t'.join(map(str,f)) for f in sortedRes if f[2]>99]

with open(fileOutput, 'w', encoding='utf-8') as fileSave:
	fileSave.write('\n'.join(sortedRes))
