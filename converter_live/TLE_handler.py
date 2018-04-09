import logger
import urllib
from threading import Thread
from time import sleep


class TLE_record:

    line1 = None
    line2 = None

    def __init__(self, line1=None, line2=None):
        self.line1 = line1
        self.line2 = line2

    def getLine1(self):
        return self.line1

    def getLine2(self):
        return self.line2

class TLE:

    DB_String_TLE = ""

    def __init__(self, TLE_webFile_path, localFileName, globalTLE, satID, pollingTime):
        print "TLE init...\n"
	logger.WriteLog("TLE init...")
        self.TLE_webFile_path = TLE_webFile_path
        self.localFileTLE = localFileName
        self.getFromWeb(self.TLE_webFile_path)
        self.globalTLE = globalTLE
        self.satelliteID = satID
        self.pollingTime = pollingTime
        self.recordGlobalTLE = []
        self.load()


    def load(self):
        self.recordGlobalTLE = open(self.globalTLE ,'r').readlines()


    def getFromWeb(self, url):
        try:
            urllib.urlretrieve(url, self.localFileTLE)
        except Exception as ex:
            print "ERROR : TLE Downloading FAILED!"
	    logger.WriteLog("ERROR : TLE Downloading FAILED!")

    def getClosest(self):
        print ""

    def tailTLEFile(self):
        f = open(self.localFileTLE, 'r')
        lines = f.readlines()
        firstLine = None
        secondLine = None
        match = False
        for line in lines:
            if match:
                secondLine = line
                match = False
                break
            if self.satelliteID in line:
                firstLine = line
                match = True
        return TLE_record(str(firstLine.strip()),str(secondLine.strip()))

    def recordExist(self, TLE_record):
        f = open(self.globalTLE, 'r')
        lines = f.readlines()
        for line in lines:
            if line.strip() == TLE_record.getLine1():
                return True
        return False

    def appendLines(self, TLE_record):
        flagRecord = self.recordExist(TLE_record)

        if not flagRecord:
            f = open(self.globalTLE,'a+')
            print "Writing on GlobalTLE"
	    logger.WriteLog("Writing on GlobalTLE")
            f.write("\n")
            f.write(TLE_record.line1)
            f.write("\n")
            f.write(TLE_record.line2)
            f.close()

    def polling(self):

        while True:
            print "TLE init...Polling to check TLE from %s \n" % self.TLE_webFile_path
            logger.WriteLog("TLE init...Polling to check TLE from %s \n" % self.TLE_webFile_path)
	    sleep(self.pollingTime)
            record = self.tailTLEFile()
            self.appendLines(record)



if __name__ == '__main__':
    tle = TLE('https://celestrak.com/NORAD/elements/engineering.txt', 'sat26958.txt', 'test.txt', ' 26958U ', 900)
    Thread(target=tle.polling).start()

