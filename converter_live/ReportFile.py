import glob
from audioop import lin2adpcm
from datetime import datetime, time
import os

from converter_live import myConstants
from converter_live.DBLite import DataBaseLite


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

    def findMissingFile(self, resultSet):
        missing = None
        tuple(missing)
        add = 0
        for line in resultSet:
            for i in line[2:6] + line[9:10]:
                add += i
                if not add == 4:
                    missing.append(line)
            add = 0
        return missing

    def createDailyReport(self, resultSetSuccess, resultSetFailed, productType):
        space = "<br>"
        #tupleSuccess = self.findSuccess(resultSetSuccess)
        #tupleFailed = self.findFailed(resultSetFailed)
        reports = self.getReportsByDay("",1)

        tupleSuccess = self.generateReportSuccess(reports)
        tupleFailed = self.generateReportFailed(reports)
        timeLimit =
        name_resultSet = self.db.queryDB("SELECT product_name FROM PROBA_PRODUCTS WHERE (%s - (SELECT insertion_date from PROBA_PRODUCTS)) > %s AND complete='%s' AND check_done='%s'" % (           datetime.now().strftime('%s'), timeLimit, myConstants.SQLITE_FALSE, myConstants.SQLITE_FALSE))
        tupleMissing = self.findMissingFile()

        tableSuccess = self.createTableReport(tupleSuccess, ["PRODUCT", "Date", "Time"])
        tableFailed = self.createTableReport(tupleFailed, ["PRODUCT", "Date", "Time"])

        totalBody = "Dear all,"
        totalBody += "%s" % (space)
        totalBody += "Please find here below the %s Live data conversion daily report for %s:" % (
        productType, datetime.datetime.now().strftime('%d-%m-%Y'))
        totalBody += "%s %s" % (space, space)
        totalBody += "Successful conversions"
        totalBody += "%s %s" % (space, space)
        totalBody += tableSuccess
        totalBody += "%s %s" % (space, space)
        totalBody += "Failed conversions"
        totalBody += "%s %s" % (space, space)
        totalBody += tableFailed
        totalBody += "%s %s" % (space, space)
        totalBody += "Regards,"
        totalBody += "%s" % (space)
        totalBody += "Dissemination Team"
        return totalBody

    def createTableReport(self, tupleIn, arrayColumn):
        style = '''
        table.minimalistBlack {
          border: 3px solid #000000;
          background-color: #EEEEEE;
          text-align: left;
          border-collapse: collapse;
        }
        table.minimalistBlack td, table.minimalistBlack th {
          border: 1px solid #000000;
          padding: 5px 4px;
        }
        table.minimalistBlack tbody td {
          font-size: 13px;
        }
        table.minimalistBlack tr:nth-child(even) {
          background: #D0E4F5;
        }
        table.minimalistBlack thead {
          background: #CFCFCF;
          border-bottom: 2px solid #000000;
        }
        table.minimalistBlack thead th {
          font-size: 15px;
          font-weight: bold;
          color: #000000;
          text-align: left;
          border-left: 1px solid #D0E4F5;
        }
        table.minimalistBlack thead th:first-child {
          border-left: none;
        }
        
        table.minimalistBlack tfoot td {
          font-size: 14px;
        }
            '''

        arrColu = arrayColumn
        head = "<head><meta><style>%s</style></head><body>" % style
        startTable = "<table class=\"minimalistBlack\">"
        headTextTable = "<thead><tr>"
        for i in arrayColumn:
            headTextTable += "<td>%s</td>" % i
        headTextTable += "</tr></thead><tbody>"

        body = head
        body += startTable
        body += headTextTable

        for i in tupleIn:
            tmp = datetime.datetime.fromtimestamp(i[6]).strftime('%d-%m-%Y %H:%M:%S').split(" ")
            productName = i[1]
            date = tmp[0]
            time = tmp[1]
            line = [productName, date, time]

            valuesTextTable = "<tr>"
            for j in line:
                valuesTextTable += "<td>%s</td>" % j
            valuesTextTable += "</tr>"
            body += valuesTextTable
        endTable = "</tbody></table>"

        body += endTable
        body += "</body>"

        return body

    def getReportsByDay(self, absPath, numDay):
        todayTime = time.time()
        pathTotal = os.path.join(absPath, "*.csv")
        listFiles = glob.glob(pathTotal)
        listResult = []

        for line in listFiles:
            creation_time = os.path.getctime(line)

            if line.endswith("csv") and ((float(todayTime) - float(creation_time)) < (86400 * numDay)):

                f = open(line, 'r')
                for j in f.readlines():
                    if ".zip" in j:
                        listResult.append(j)

        return listResult

    def generateReportFailed(self, _reports):
        reports = _reports
        failedList = []
        for report in reports:
            if "N/A" in report:
                totListName = "%s,%s,Error During Convertion" % (report.split(",")[0], report.split(",")[2])
                failedList.append(totListName)

        return failedList

    def generateReportSuccess(self, _reports):
        reports = _reports
        succesList = []
        for report in reports:
            if not "N/A" in report:
                totListName = "%s,%s" % (report.split(",")[0], report.split(",")[2])
                succesList.append(totListName)
        return succesList

    def findSuccess(self, resultSet):
        success = []
        add = 0

        for line in resultSet:
            for i in line[2:6] + line[9:10]:
                add += i
                if add == 4:
                    success.append(line)
            add = 0
        return success

    def findFailed(self, resultSet):
        failed = []
        add = 0

        missing = 0
        for line in resultSet:
            for i in line[2:6] + line[9:10]:
                add += i
                if not add == 4:
                    failed.append(line)
            add = 0
        return failed


    def getLiveInboxFailed(self):
        pass












class Md5Manifest():

    def __init__(self, absNameProduct, md5Line):
        self.nameProduct = os.path.basename(absNameProduct)
        self.md5 = md5Line
        self.manifestfileName = "%s.md5-manifest" % absNameProduct[:-4]
        if not os.path.isfile(self.manifestfileName):
            self.createManifest()

    def createManifest(self):
        manifestFile = open(self.manifestfileName, 'a')
        manifestFile.close()

    def write(self):
        manifestFile = open(self.manifestfileName, 'a')
        manifestFile.write("%s %s\n" % (self.nameProduct,self.md5))
        manifestFile.close()