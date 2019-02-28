import urllib2
import urlparse
import time
import filecmp
import os
import sys
from datetime import datetime, timedelta

sources = []

def createNewDict(name, url, frequency):
	dict = {'name':name,'url':url,'frequency':int(frequency),'lastPolled':datetime(2015,1,1),'lastFilename':""}
	return dict

def loadSourceData():
	print "loading source data..."
	#df = open("weathersource.dat","r")
	df = open("weathertest.dat","r")
	for line in df:
		raw_line = line.strip()
		dList = raw_line.split(",")
		ddict = createNewDict(dList[0],dList[1],dList[2])
		sources.append(ddict)
	df.close()

def getExtension(url):
	path = urlparse.urlparse(url).path
	return os.path.splitext(path)[1]

def getImage(url,filename,lastFilename,name):
	try:
		image = urllib2.urlopen(url)
	except:
		print "{0} Exception {1}:{2}".format(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), url, sys.exc_info()[1])
		return True

	try:
		filepath = name + "/"
		output = open(filepath + filename,"wb")
		output.write(image.read())
		output.close()
		if (lastFilename == ""):return False
		if (filecmp.cmp(filepath + filename,filepath + lastFilename)):
			os.remove(filepath + filename)
			return True
		else:
			return False
	except:
		print "{0} Exception {1}:{2}".format(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), url, sys.exc_info()[1])
		return True

def createDirectories():
	print "Creating directories...."
	try:
		for dict in sources:
			fp = dict['name']
			#d = os.path.dirname(fp)
			if not os.path.exists(fp):
				os.makedirs(dict['name'])
	except:
		print "{0} Exception {1}:{2}".format(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "Creating " + fp, sys.exc_info()[1])
		sys.exit()

def getLastPollTimes():
	print "Getting last poll times...."
	for dict in sources:
		try:
			nom = dict['name']
			dirList = os.listdir(nom)
			dirList.sort(reverse=True)
			if dirList[0] <> "":
				dt = dirList[0][len(nom):dirList[0].find(".")]
				dict['lastPolled'] = datetime.strptime(dt,'%Y%m%dT%H%M%S')
		except:
			print "Startup: Error parsing filename {0}{1}".format(dict['name'],sys.exc_info()[1])

loadSourceData()
createDirectories()
getLastPollTimes()

print "Running main loop"
while True:
	for dict in sources:
		delta = datetime.utcnow() - dict['lastPolled']
		if (delta.total_seconds() / 60 >= dict['frequency']):
			#print "getting: ", dict['name']
			url = dict['url']
			filename = dict['name'] + datetime.utcnow().strftime("%Y%m%dT%H%M%S") + getExtension(url)
			lastFilename = dict['lastFilename']

			if (getImage(url,filename, lastFilename,dict['name'])):
				#if (True):
				#downloaded file matched the last downloaded file so we threw it out
				#now let's modify last polled so it'll check again in a little while
				#last = dict['lastPolled']
				newDelta = timedelta(seconds=((float(dict['frequency'])/float(2)) * 60))
				dict['lastPolled'] = datetime.utcnow() - newDelta
				#next = dict['lastPolled']
				print "{0} duplicate, requeueing {1}".format(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), dict['name'])
			else:
				dict['lastPolled'] = datetime.utcnow()
				dict['lastFilename'] = filename
				print "{0} received: {1}".format(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),dict['name'])

	time.sleep(2)

