def chunks(l, n):
	"""Yield successive n-sized chunks from l."""
	for i in range(0, len(l), n):
		yield l[i:i + n]

def encode_if_necessary(b):
	if type(b) is bytes:
		return b.decode('utf8')
	return b

def whereIn(arguments):
	try:
		return ','.join(['%s']*len(arguments))
	except:
		print('whereIn ex ',arguments)
		exit()
