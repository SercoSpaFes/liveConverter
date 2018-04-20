# -*- coding: utf-8 -*-

import datetime
import glob
import os
import time


from mail import Mail

if __name__ == '__main__':
    a = []
    b = []
    a.append((48, 'CHRIS_OH_050513_53CF_41', 1, 1, 1, 0, 1522328257.23, 'PROBA_CHRIS', 0, 1, 0, 0))
    b.append((108, 'CHRIS_YW_180212_4442_41', 1, 1, 1, 0, 1523006752.38, 'PROBA_CHRIS', 0, 0, 0, 0))


    def createDailyReport(resultSetSuccess, resultSetFailed, productType):
        space = "<br>"
        tupleSuccess = findSuccess(resultSetSuccess)
        tupleFailed = findFailed(resultSetFailed)


        tableSuccess = createTableReport(tupleSuccess, ["PRODUCT", "Date", "Time"], False)
        tableFailed = createTableReport(tupleFailed, ["PRODUCT", "Date", "Time", "Notes"], True)

        totalBody = "Dear all,"
        totalBody += "%s" % (space)
        totalBody +="Please find here below the %s Live data conversion daily report for %s:" % (productType, datetime.datetime.now().strftime('%d-%m-%Y'))
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


    def createTableReport(tupleIn, arrayColumn, flag=False):
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
        head = "<head><meta><style>%s</style></head><body>" % style
        startTable = "<table class=\"minimalistBlack\">"
        headTextTable = "<thead><tr>"
        for i in arrayColumn:
            headTextTable += "<td>%s</td>" % i
        headTextTable += "</tr></thead><tbody>"

        body = head
        body += startTable
        body += headTextTable
        #test
        count = 0

        for i in tupleIn:
            tmp = datetime.datetime.fromtimestamp(i[6]).strftime('%d-%m-%Y %H:%M:%S').split(" ")
            productName = i[1]
            date = tmp[0]
            time = tmp[1]
            count += 1
            if count == 2 and flag:
                line = [productName, date, time, "Corrupted file"]
            elif count !=2 and flag:
                line = [productName, date, time, "Missing jpg File/s"]
            else:
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



    def findSuccess(resultSet):
        success = []
        add = 0

        for line in resultSet:
            for i in line[2:6] + line[9:10]:
                add += i
                if add == 4:
                    success.append(line)
            add = 0
        return success


    def findFailed(resultSet):
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


    def getReportsByDay(absPath, numDay):
        todayTime = time.time()
        pathTotal = os.path.join(absPath,"*.csv")
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

    def generateReportFailed(_reports):
        reports = _reports
        failedList = []
        for report in reports:
            if "N/A" in report:
                totListName = "%s,%s,Error During Convertion" % (report.split(",")[0], report.split(",")[2])
                failedList.append(totListName)

        return failedList


    def generateReportSuccess(_reports):
        reports = _reports
        succesList = []
        for report in reports:
            if not "N/A" in report:
                totListName = "%s,%s" % (report.split(",")[0], report.split(",")[2])
                succesList.append(totListName)
        return succesList


    reports = getReportsByDay('C:\Convertitori\DEIMOS\eportdir_30', 50)
    print generateReportFailed(reports)
    print generateReportSuccess(reports)


    body = createDailyReport(a, b, "PROBA_CHRIS")
    ciao = Mail("giulio@gmail.com", "giulio82.villani@libero.it", "PROBA CHRIS daily report (%s)" % datetime.datetime.now(), hostMail = '192.168.7.73', bbc = "giulio.villani@serco.com", cc = None, logFile = None, textMail = body, port = 25)
    #ciao = Mail("giulio@gmail.com", "alessandro.maltese@serco.com", "PROBA CHRIS daily report (%s)" % datetime.datetime.now(), hostMail='192.168.7.73',             bbc="giulio.villani@serco.com", cc=None, logFile=None, textMail=body, port=25)
    ciao.sendEmail(True)