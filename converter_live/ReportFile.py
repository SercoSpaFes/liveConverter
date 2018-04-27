import glob
from datetime import datetime, timedelta
import time
import os
import myConstants
from DBLite import DataBaseLite
from mail import Mail


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
        self.collectionFile = os.path.join(_reportPath,
                                           "%s_report_%s.csv" % (_collectionName, datetime.now().strftime("%Y%m")))

        self.sumConverted = 0
        self.sumProcessed = 0
        self.sumProcessedIngester = 0
        self.sumFailed = 0
        if self.collectionFileIngester != None and os.path.exists(self.collectionFile):
            if not os.path.isfile(self.collectionFile):
                # self.writeHeaderFile(0,0,0)
                f = open(self.collectionFile, 'w')
            if not os.path.isfile(self.collectionFileIngester):
                # self.writeHeaderIngesterStart()
                f = open(self.collectionFileIngester, 'w')

    def writeHeaderIngesterStart(self):
        f = open(self.collectionFileIngester, 'w')
        f.write("### REPORT FOR INGESTER ###\n")
        f.write("\n")
        f.write("Total File Ingested, 0\n")
        f.write("\n")
        f.write("DateTime, InputFile, MD5_in, Result,\n")
        f.close()

    def writeHeaderFile(self, sumProcessed, sumFailed, sumConverted, ingested=None):

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
        f = open(singleReport, 'w').write(line)

    def writeLiveInboxLine(self, message, productType, nameFile):
        f = open(self.collectionFile, 'a')
        f.write("%s,%s,%s,%s,%s,%s\n" % (message, datetime.now(), productType, nameFile, datetime.now().strftime('%s'), os.path.getsize(nameFile)))
        f.close()

    def writeLine(self, inputFile, result, md5_in):

        self.sumProcessed = self.sumProcessed + 1
        if result == "FAILED":
            self.sumFailed = self.sumFailed + 1
        elif result == "SUCCESS":
            self.sumConverted = self.sumConverted + 1
        f = open(self.collectionFile, 'a')
        currentDateTime = datetime.now().strftime('%s')
        line = "### TASK Process[CONVERSION] PROCESSING TIME : %s^state:%s^source:%s^sourceMd5:%s^at:%s^\n" % (
        datetime.now(), result, inputFile, md5_in, currentDateTime)
        f.write(line)
        f.close()
        # self.updateHeader()
        inFileName = os.path.basename(inputFile)
        if self.singleReportDir != None:
            self.writeSingleReport(inFileName, line)

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
        line = "### TASK Process[INGESTION] PROCESSING TIME : %s^state:%s^source:%s^sourceMd5:%s^at:%s^\n" % (
        datetime.now(), _result, _filename, "N/A", currentDateTime)
        f.write(line)
        f.close()
        # self.updateHeaderIngester()

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


class ReportReader:

    def __init__(self, reportPathFolder, dbPath):
        self.reportPathFolder = reportPathFolder
        self.dbAbsFilePath = dbPath
        self.DB = DataBaseLite(dbPath, None)

    def findMissingFile(self, resultSet):
        missing = []

        tuple(missing)
        for line in resultSet:
            today = time.strftime(time.strftime("%a %b %d %Y, %H:%M:%S", time.localtime(line[1]))).split(",")
            # zip, xml, jpg, bmp
            stringa = "Missing file:"
            if line[2] == 0 and line[0].startswith("CHRIS"):
                stringa += " .zip "
            if line[3] == 0:
                stringa += " .xml "
            if line[4] == 0:
                stringa += " .jpg "
            if line[5] == 0 and not line[0].startswith("CHRIS"):
                stringa += " .bmp "
            missing.append("%s,%s,%s,%s" % (line[0], today[0], today[1], stringa))

        return missing

    def createDailyReport(self, productType, days=1):
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

        space = "<br>"
        now = datetime.now()
        seconds_since_midnight = (now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
        yesterday_00_00 = time.time() - seconds_since_midnight
        reportPath = os.path.join(self.reportPathFolder, "%s/_1/conversionReport" % productType)
        reports = self.getReportsByDay(reportPath, yesterday_00_00, days)
        tupleSuccess = self.generateReportSuccess(reports)
        tupleFailed = self.generateReportFailed(reports)

        lower_range = yesterday_00_00 - (86400 * (days))
        higher_range = yesterday_00_00 - (86400 * (days - 1))

        #        name_resultSet = self.DB.queryDB("SELECT product_name,insertion_date FROM PROBA_PRODUCTS WHERE ((SELECT insertion_date from PROBA_PRODUCTS) BETWEEN %s AND %s) AND complete='%s'" % (lower_range,higher_range,myConstants.SQLITE_FALSE))

        name_resultSet = self.DB.queryDB(
            "SELECT product_name,insertion_date,zip,xml,jpg,bmp FROM PROBA_PRODUCTS WHERE product_type='%s' and insertion_date>%s and insertion_date< %s and complete='%s'" % (
            productType, lower_range, higher_range, myConstants.SQLITE_FALSE))
        tupleMissing = self.findMissingFile(name_resultSet)
        tupleFailed = tupleFailed + tupleMissing
        if len(tupleSuccess) == 0 and len(tupleFailed) == 0:
            tupleSuccess.append("No products received,,")
            tupleFailed.append("No products received,,,")
        elif len(tupleSuccess) != 0 and len(tupleFailed) == 0:
            tupleFailed.append("No products failed,,,")
        elif len(tupleSuccess) == 0 and len(tupleFailed) != 0:
            tupleSuccess.append("No products success,,")
        tableSuccess = self.createTableReport(tupleSuccess, ["PRODUCT", "Reception Date", "Time"])
        tableFailed = self.createTableReport(tupleFailed, ["PRODUCT", "Reception Date", "Time", "Comment"])
        totalBody = "<head><meta><style>%s</style></head><body>" % style
        totalBody += "Dear all,"
        totalBody += "%s %s" % (space, space)
        totalBody += "Please find here below the %s Live data conversion daily report for %s:" % (
        productType, (datetime.now() - timedelta(days)).strftime("%a %b %d %Y"))
        totalBody += "%s %s" % (space, space)
        totalBody += "<b>Successful conversions</b>"
        totalBody += "%s %s" % (space, space)

        # totalBody += tableFailed
        totalBody += tableSuccess
        totalBody += "%s %s" % (space, space)
        totalBody += "<b>Failed conversions</b>"
        totalBody += "%s %s" % (space, space)
        totalBody += tableFailed
        # totalBody += tableSuccess
        totalBody += "%s %s" % (space, space)
        totalBody += "Regards,"
        totalBody += "%s" % (space)
        totalBody += "FES Dissemination Team</body>"

        return totalBody

    def createTableReport(self, tupleIn, arrayColumn):

        #        head = "<head><meta><style>%s</style></head>" % style
        startTable = "<table class=\"minimalistBlack\">"
        headTextTable = "<thead><tr>"
        for i in arrayColumn:
            headTextTable += "<td>%s</td>" % i
        headTextTable += "</tr></thead><tbody>"

        #        body = head
        body = startTable
        body += headTextTable
        tupleIn = filter(None, tupleIn)
        try:
            for i in tupleIn:
                # ['Wed Mar 14 14:24:26 CET 2018','C:\\Convertitori\\DEIMOS\\DE2_PSH_L1C_000000_20160308T115211_20160308T115214_DE2_9313_BB4B_.zip',              'Error During Convertion']
                valuesTextTable = "<tr>"
                line = i.split(",")
                for j in line:
                    valuesTextTable += "<td>%s</td>" % j
                valuesTextTable += "</tr>"
                body += valuesTextTable
        except IndexError:
            print "TupleIn empty"
        endTable = "</tbody></table>"
        body += endTable

        return body

    def getReportsByDay(self, absPath, yesterday_second, numDay=0):
        todayTime = time.time()
        pathTotal = os.path.join(absPath, "*.csv")
        listFiles = glob.glob(pathTotal)
        listResult = []
        lower_range = yesterday_second - (86400 * numDay)
        higher_range = yesterday_second - (86400 * (numDay - 1))
        for line in listFiles:
            creation_time = os.path.getmtime(line)
            if line.endswith("csv") and creation_time > lower_range and creation_time < higher_range:
                # if line.endswith("csv") and creation_time in xrange(int(lower_range),int(higher_range)):
                f = open(line, 'r')
                for j in f.readlines():

                    if ".zip" in j:
                        listResult.append(j)
        return listResult

    def generateReportFailed(self, reports):
        failedList = []
        for report in reports:
            if "N/A" in report:
                time_field = report.split(",")[0]
                time_field_splitted = time_field.split(" ")
                date = "%s %s %s %s" % (
                time_field_splitted[0], time_field_splitted[1], time_field_splitted[2], time_field_splitted[5])
                time = "%s" % (time_field_splitted[3])
                productName = os.path.basename(report.split(",")[2]).replace("_ToCONVERT.zip", "")
                totListName = "%s,%s,%s,Error During Convertion" % (productName, date, time)
                failedList.append(totListName)

        return failedList

    def generateReportSuccess(self, reports):
        succesList = []
        for report in reports:

            if not "N/A" in report:
                time_field = report.split(",")[0]
                time_field_splitted = time_field.split(" ")
                date = "%s %s %s %s" % (
                time_field_splitted[0], time_field_splitted[1], time_field_splitted[2], time_field_splitted[5])
                time = "%s" % (time_field_splitted[3])
                productName = os.path.basename(report.split(",")[2]).replace("_ToCONVERT.zip", "")
                totListName = "%s,%s,%s" % (productName, date, time)
                succesList.append(totListName)
        return succesList

    def generateReportSuccess__(self, reports):
        succesList = []
        failedList = []
        for report in reports:
            time_field = report.split(",")[0]
            time_field_splitted = time_field.split(" ")
            date = "%s %s %s %s" % (
                time_field_splitted[0], time_field_splitted[1], time_field_splitted[2], time_field_splitted[5])
            time = "%s" % (time_field_splitted[3])
            productName = os.path.basename(report.split(",")[2]).replace("_ToCONVERT.zip", "")
            if not "N/A" in report:
                totListName = "%s,%s,%s" % (productName, date, time)
                succesList.append(totListName)
            else:
                totListName = "%s,%s,%s,Error During Convertion" % (productName, date, time)
                failedList.append(totListName)
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

    def getLiveInboxFailed(self, reportPath="/home/datamanager/converter-live/handler", lowRange, highRange):
        inboxFile = glob.glob("%s/LiveInbox_report_%s%s.csv" % (reportPath, datetime.now().year,datetime.strftime('%m')))
        flines = open(inboxFile, 'r').readlines()
        failed = []
        for report in flines:
#Problem: (FirstPhase) File FAILED in InBOX,2018-04-23 22:30:10.334681,PROBA_CHRIS,/nfsdata2/PROBA/Inbox/CHRIS/CHRIS_KA_180413_4762_41.zip,1524515410,12234543(size)
            if report[4] > lowRange and report[4] < highRange:
                size         = report[5]
                epochTime    = report[4]
                absPathFile  = report[3]
                prodType     = report[2]
                humanDate    = report[1]
                messageError = "Corrupted File (bad zip/bad file size)"
                dateSplit = humanDate.split(" ")
                failed.append("%s,%s,%s,%s" % (os.path.basename(absPathFile), dateSplit[0], dateSplit[1],messageError) )
        return failed



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
        manifestFile.write("%s %s\n" % (self.nameProduct, self.md5))
        manifestFile.close()


if __name__ == '__main__':
    rep = ReportReader("/home/datamanager/converter-live/reports/",
                       "/nfsdata/nfsdata02/databases/converter_live/ProbaDB.sqlite")
    daysDelta = 18
    ptypes = ["PROBA_HRC", "PROBA_CHRIS"]
    while daysDelta >= 18:
        for i in ptypes:
            body = rep.createDailyReport(i, daysDelta)
            s = i.replace("PROBA_", "PROBA-1 ")
            ciao = Mail("Dissemination Team <info@apps.eo.esa.int>", "giulio.villani@serco.com",
                        "%s daily report (%s)" % (s, (datetime.now() - timedelta(daysDelta)).strftime("%a %b %d %Y")),
                        hostMail='localhost', bcc=None, cc=None, logFile=None, textMail=body, port=25)
            ciao.sendEmail(True)
            print "--> %s %s <--" % (s, (datetime.now() - timedelta(daysDelta)).strftime("%a %b %d %Y"))
            time.sleep(2)
        daysDelta -= 1
