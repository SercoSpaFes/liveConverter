from datetime import datetime
import os

class ReportFile:

    def __init__(self, _reportPath, _collectionName, _singleReportDir=None):
        self.reportPath = _reportPath
        if _singleReportDir != None:
            self.singleReportDir = _singleReportDir
            self.collectionFileIngester = os.path.join(_reportPath, "%s_report_INGESTER_%s.csv" % (
            _collectionName, datetime.now().strftime("%Y%m")))
            if not os.path.isdir(self.singleReportDir):
                os.makedirs(self.singleReportDir, 0755)
        else:
            self.singleReportDir = None
            self.collectionFileIngester = None
        self.collectionFile = os.path.join(_reportPath, "%s_report_%s.csv" % (_collectionName,datetime.now().strftime("%Y%m")))

        self.sumConverted = 0
        self.sumProcessed = 0
        self.sumProcessedIngester = 0
        self.sumFailed = 0
        if self.collectionFileIngester != None and os.path.exists(self.collectionFile):
            if not os.path.isfile(self.collectionFile):
                #self.writeHeaderFile(0,0,0)
                f = open(self.collectionFile, 'w')
            if not os.path.isfile(self.collectionFileIngester):
                #self.writeHeaderIngesterStart()
                f = open(self.collectionFileIngester, 'w')


    def writeHeaderIngesterStart(self):
        f = open(self.collectionFileIngester, 'w')
        f.write("### REPORT FOR INGESTER ###\n")
        f.write("\n")
        f.write("Total File Ingested, 0\n")
        f.write("\n")
        f.write("DateTime, InputFile, MD5_in, Result,\n")
        f.close()

    def writeHeaderFile(self, sumProcessed,sumFailed,sumConverted, ingested=None):

        f = open(self.collectionFile, 'a')
        f.write("### REPORT FOR CONVERTER ###\n")
        f.write("\n")
        f.write("Total File Processed, %s \n" % sumProcessed)
        f.write("Total File Converted, %s \n" % sumConverted)
        f.write("Total File Failed, %s \n" % sumFailed)
        f.write("Total File Ingested in LOADS, %s \n\n\n" % ingested)
        f.write("DateTime, InputFile, MD5_in, Result,\n")
        f.close()

    def writeSingleReport(self, inputFile, line):
        singleReport = "%s_REPORT" % os.path.join(self.singleReportDir, inputFile)
        f = open(singleReport,'w').write(line)

    def writeLiveInboxLine(self, message, productType, nameFile):
        f = open(self.collectionFile, 'a')
        f.write("%s,%s,%s,%s,%s\n" % (message,datetime.now(),productType,nameFile,datetime.now().strftime('%s')))
        f.close()


    def writeLine(self, inputFile, result, md5_in):

        self.sumProcessed = self.sumProcessed + 1
        if result == "FAILED":
            self.sumFailed = self.sumFailed + 1
        elif result == "SUCCESS":
            self.sumConverted = self.sumConverted + 1
        f = open(self.collectionFile, 'a')
        currentDateTime = datetime.now().strftime('%s')
        line = "### TASK Process[CONVERSION] PROCESSING TIME : %s^state:%s^source:%s^sourceMd5:%s^at:%s^\n" %(datetime.now(), result, inputFile, md5_in, currentDateTime)
        f.write(line)
        f.close()
        #self.updateHeader()
        inFileName = os.path.basename(inputFile)
        if self.singleReportDir != None:
            self.writeSingleReport(inFileName,line)

    def getStartPosition(self):
        indexValue = 0
        with open(self.collectionFile, 'r') as f:
            current_line = f.readlines()
            if current_line.__contains__("DateTime, InputFile, MD5_in, Result"):
                indexValue = len(f.readlines())

        return indexValue

    def updateHeader(self):

        f = open(self.collectionFile, 'r').readlines()
        f[0] = "### REPORT FOR CONVERTER ###\n"
        f[1] = "\n"
        f[2] = "Total File Processed, %s \n" % self.sumProcessed
        f[3] = "Total File Converted, %s \n" % self.sumConverted
        f[4] = "Total File Failed, %s \n" % self.sumFailed
        f[5] = "Total File Ingested in LOADS, %s \n" % 0
        f[6] = "\n"
        f[7] = "\n"
        f[8] = "DateTime, InputFile, MD5_in, Result,\n"
        g = open(self.collectionFile, 'w').writelines(f)

    def writeLineIngester(self, _filename, _result):
        self.sumProcessedIngester = self.sumProcessedIngester + 1
        f = open(self.collectionFileIngester, 'a')
        currentDateTime = datetime.now().strftime('%s')
        line = "### TASK Process[INGESTION] PROCESSING TIME : %s^state:%s^source:%s^sourceMd5:%s^at:%s^\n" % (datetime.now(), _result, _filename, "N/A", currentDateTime)
        f.write(line)
        f.close()
        #self.updateHeaderIngester()

    def updateHeaderIngester(self):
        f = open(self.collectionFileIngester, 'r').readlines()
        f[0] = "#### REPORT FOR INGESTER ###\n"
        f[1] = "\n"
        f[2] = "Total File Ingested, %s \n" % self.sumProcessedIngester
        f[3] = "\n"
        f[4] = "DateTime, InputFile, MD5_in, Result,\n"
        g = open(self.collectionFileIngester, 'w').writelines(f)

    def getReportPath(self):
        return self.collectionFile

    def getReportPathIngester(self):
        return self.collectionFileIngester
    

class Md5Manifest():

    def __init__(self, absNameProduct, md5Line):
        self.nameProduct = os.path.basename(absNameProduct)
        self.md5 = md5Line
        manifestfileName = "%s.md5-manifest" % absNameProduct[:-4]
        if not os.path.isfile(manifestfileName):
            self.createManifest()

    def createManifest(self):
        manifestFile = open(self.manifestfileName, 'a')
        manifestFile.close()

    def write(self):
        manifestFile = open(self.manifestfileName, 'a')
        manifestFile.write("%s %s\n" % (self.nameProduct,self.md5))
        manifestFile.close()
