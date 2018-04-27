from utilities import md5
import logger
import glob
import traceback
import zipfile
import os
import time
from datetime import datetime, timedelta
import sys
import shutil
from DBLite import DataBaseLite
from mail import Mail
from ReportFile import ReportReader
# GL:
import myConstants


class PROBA_product:
    DB_String_PROBA_old = '''CREATE TABLE PROBA_PRODUCTS
                 (ID INTEGER PRIMARY KEY AUTOINCREMENT,
                 Product_NAME              TEXT     UNIQUE NOT NULL,
                 zip                    BOOLEAN     DEFAULT FALSE NOT NULL,
                 xml                    BOOLEAN     DEFAULT FALSE NOT NULL,
                 jpg                    BOOLEAN     DEFAULT FALSE NOT NULL,
                 bmp                    BOOLEAN     DEFAULT FALSE NOT NULL,
                 Insertion_date         INTEGER     NOT NULL,
                 ProductType               TEXT     NOT NULL,
                 Already_Converted      BOOLEAN     DEFAULT FALSE,
                 COMPLETE               BOOLEAN     DEFAULT FALSE,
                 CheckDone              BOOLEAN     DEFAULT FALSE,
                 LTA_Archiving_Status   BOOLEAN     DEFAULT FALSE                    
                 );'''

    DB_String_PROBA = '''CREATE TABLE PROBA_PRODUCTS
                     (ID INTEGER PRIMARY KEY AUTOINCREMENT,
                     product_name              TEXT     UNIQUE NOT NULL,
                     zip                    BOOLEAN     DEFAULT 0 NOT NULL,
                     xml                    BOOLEAN     DEFAULT 0 NOT NULL,
                     jpg                    BOOLEAN     DEFAULT 0 NOT NULL,
                     bmp                    BOOLEAN     DEFAULT 0 NOT NULL,
                     insertion_date         INTEGER     NOT NULL,
                     product_type               TEXT     NOT NULL,
                     already_converted      BOOLEAN     DEFAULT 0 NOT NULL,
                     complete               BOOLEAN     DEFAULT 0 NOT NULL,
                     check_done              BOOLEAN     DEFAULT 0 NOT NULL,
                     lta_archiving_status   BOOLEAN     DEFAULT 0 NOT NULL,
                     CHECK (zip IN (0,1)),
                     CHECK (xml IN (0,1)),
                     CHECK (jpg IN (0,1)),
                     CHECK (bmp IN (0,1)),
                     CHECK (already_converted IN (0,1)),
                     CHECK (complete IN (0,1)),
                     CHECK (check_done IN (0,1)),
                     CHECK (lta_archiving_status IN (0,1))
                     );'''

    pathDB = "/nfsdata/nfsdata02/databases/converter_live"

    def __init__(self, productType, _fileName, indir, LTA_Path, outdir):

        self.indir = indir
        self.outdir = outdir
        self.productType = productType
        self.filename_absolutePath = _fileName
        self.filename_basename = os.path.basename(_fileName)
        self.filename = self.filename_basename[:-4]
        self.extension = _fileName[-3:]

        # create proba db, updated schema for BOOLEAN as integer 0/1. Test integrity
        self.probaDB = DataBaseLite(os.path.join(self.pathDB, "ProbaDB.sqlite"), self.DB_String_PROBA)
        # self.checkDbIntegrity()

        self.xml_Element = "xml"
        self.jpg_Element = "jpg"
        self.LTA_Path = LTA_Path

        if productType == "PROBA_CHRIS":
            self.zip_Element = "zip"
        elif productType == "PROBA_HRC":
            self.bmp_Element = "bmp"

    #
    # test that the insert, select on BOOLEAN in particular, delete works on the DB.
    #
    def checkDbIntegrity(self):
        # create 3 test records
        print "\n\n START DB integrity check\n"
        self.probaDB.insertEntryDB(
            '''INSERT INTO PROBA_PRODUCTS (product_name,zip,xml,jpg,bmp,insertion_date,product_type,complete) VALUES ('test_complete','1','0','0','0','1523002351','TESTING','1')''')
        self.probaDB.insertEntryDB(
            '''INSERT INTO PROBA_PRODUCTS (product_name,zip,xml,jpg,bmp,insertion_date,product_type,complete) VALUES ('test_not_complete1','0','1','0','0','1523002371','TESTING','0')''')
        self.probaDB.insertEntryDB(
            '''INSERT INTO PROBA_PRODUCTS (product_name,zip,xml,jpg,bmp,insertion_date,product_type,complete) VALUES ('test_not_complete2','0','0','1','0','1523002251','TESTING','0')''')

        #
        print " 3 TESTING records added:"
        lines = self.probaDB.queryDB("SELECT * FROM PROBA_PRODUCTS WHERE product_type = 'TESTING'")
        n = 0
        for item in lines:
            print "  %s:%s" % (n, item)
            n += 1
        if len(lines) != 3:
            raise Exception("DB CHECK failed: don't find the 3 TESTING records but:%s" % len(lines))

        #
        print " 1 TESTING records complete queryed:"
        lines = self.probaDB.queryDB("SELECT * FROM PROBA_PRODUCTS WHERE product_type = 'TESTING' and complete=1")
        n = 0
        for item in lines:
            print "  %s:%s" % (n, item)
            n += 1
        if len(lines) != 1:
            raise Exception("DB CHECK failed: don't find the 1 TESTING complete records but:%s" % len(lines))

        #
        print " 2 TESTING records not complete queryed:"
        lines = self.probaDB.queryDB("SELECT * FROM PROBA_PRODUCTS WHERE product_type = 'TESTING' and complete=0")
        n = 0
        for item in lines:
            print "  %s:%s" % (n, item)
            n += 1
        if len(lines) != 2:
            raise Exception("DB CHECK failed: don't find the 2 TESTING not complete records but:%s" % len(lines))

        # delete the 3 test records
        self.probaDB.deleteEntryDB("DELETE FROM PROBA_PRODUCTS WHERE product_type = 'TESTING'")

        print "\n END DB integrity check\n\n"

    def createContainer(self, _fileName, _inDir, _outDir, productType):
        # _fileName = file name without extension
        print "Create ZIP container for %s product type" % self.productType
        logger.WriteLog("Create ZIP container for %s product type" % self.productType)
        _outFile = os.path.join(_outDir, _fileName)
        absFileName = os.path.join(_inDir, _fileName)
        listProba = ["%s.xml" % absFileName, "%s.jpg" % absFileName, "%s.zip" % absFileName]
        try:
            if productType == "PROBA_HRC":
                listProba = ["%s.xml" % absFileName, "%s.jpg" % absFileName, "%s.bmp" % absFileName]
            zip = zipfile.ZipFile("%s_ToCONVERT.zip" % _outFile, "w", zipfile.ZIP_DEFLATED)
            for name in listProba:
                zip.write(name, arcname=os.path.basename(name))
            zip.close()
            return 0
        except Exception, e:
            print "ERROR : Error during container creation"
            logger.WriteLog("ERROR : Error during container creation")
            logger.WriteLog(e)
            print e
            return -1

    def setAlreadyConverted(self):
        self.probaDB.updateEntryDB("UPDATE PROBA_PRODUCTS SET already_converted='%s' WHERE product_name='%s'" % (
            myConstants.SQLITE_TRUE, self.filename))

    def checkAlreadyConverted(self):

        result = self.probaDB.queryDB(
            "SELECT already_converted FROM PROBA_PRODUCTS WHERE product_name ='%s'" % self.filename)
        if result[0][0] == myConstants.SQLITE_TRUE:
            print "INFO: product already converted: %s " % self.filename
            logger.WriteLog("INFO: product already converted: %s " % self.filename)
            return True
        else:
            print "INFO:  product not converted: %s " % self.filename
            logger.WriteLog("INFO:  product not converted: %s " % self.filename)
            return False

    def run(self):

        exitCode = self.recordIteamDB(self.filename, self.extension, self.productType)
        if exitCode == 0:
            exitCode = self.checkAllExtensions(self.filename)
            if exitCode == 0:
                print "INFO: Updating complete field for product %s " % self.filename
                logger.WriteLog("INFO: Updating complete field for product %s " % self.filename)
                updateComplete = "UPDATE PROBA_PRODUCTS SET complete='%s' WHERE product_name='%s'" % (
                    myConstants.SQLITE_TRUE, self.filename)
                exitCode = self.createContainer(self.filename, self.indir, self.outdir, self.productType)
                if exitCode == 0:
                    self.probaDB.updateEntryDB(updateComplete)
                    self.ingestInLTA()
        return exitCode

    def checkZipFile(self, fileName):
        if fileName.endswith("zip"):
            if zipfile.ZipFile(fileName, 'r').testzip() is None:
                print "INFO: Zip Validation Passed"
                logger.WriteLog("INFO: Zip Validation Passed")
                return 0
            else:
                print "ERROR: Zip File Corrupted"
                logger.WriteLog("ERROR: Zip File Corrupted")
                return -1
        else:
            if not fileName.endswith((".xml", ".jpg", ".bmp")):
                logger.WriteLog("ERROR: %s file is not jpg, bmp or xml and cannot be inserted into DB" % fileName)
                return -1
            if not os.path.getsize(fileName) == 0:
                print "INFO: File Validation Passed"
                logger.WriteLog("INFO: File Validation Passed")
                return 0
            else:
                print "ERROR: File Corrupted"
        raise Exception("ERROR: File Corrupted")

    def queryDB(self, filename, extension_element):
        sql = "SELECT %s FROM PROBA_PRODUCTS WHERE product_name ='%s'" % (extension_element, filename)
        print " queryDB sql=%s" % sql
        resultSet = self.probaDB.queryDB(sql)
        print " queryDB resultSet=%s" % resultSet
        if len(resultSet) == 0:
            return 4
        elif len(resultSet[0]) == 1 and resultSet[0][0] == myConstants.SQLITE_FALSE:  # .__contains__("FALSE"):
            # resultSet is list of tuple
            return 2
        elif len(resultSet[0]) == 1 and resultSet[0][0] == myConstants.SQLITE_TRUE:  # .__contains__("TRUE"):
            return 3
        else:
            print " queryDB resultSet[0]=%s; type:%s" % (resultSet[0], type(resultSet[0]))
            return -1

    def checkAllExtensions(self, filename):
        resultSet = self.probaDB.queryDB(
            "SELECT zip,jpg,xml,bmp FROM PROBA_PRODUCTS WHERE product_name ='%s'" % (filename))
        resultSet_2 = self.probaDB.queryDB(
            "SELECT complete FROM PROBA_PRODUCTS WHERE product_name ='%s'" % (filename))
        count = 0
        if len(resultSet) > 0:
            for i in resultSet[0]:
                if i == myConstants.SQLITE_TRUE:
                    count += 1
            print " checkAllExtensions resultSet part count=%s; complete field=%s" % (count, resultSet_2[0][0])
            if count == 3 and resultSet_2[0][0] == myConstants.SQLITE_FALSE:  # .__contains__("FALSE"):
                return 0
        return None

    def recordIteamDB(self, filename, extension_element, prodType):
        logger.WriteLog("Finding product %s on Database..." % filename)
        exitcode = self.queryDB(filename, extension_element)
        if exitcode == 2:
            if self.checkZipFile(self.filename_absolutePath) == 0:
                print "INFO: Updating field %s related to product %s on DB" % (extension_element, filename)
                logger.WriteLog("INFO: Updating field %s related to product %s on DB" % (extension_element, filename))
                self.probaDB.updateEntryDB("UPDATE PROBA_PRODUCTS SET %s='%s' WHERE product_name='%s'" % (
                extension_element, myConstants.SQLITE_TRUE, filename))
            else:
                return -1

        if exitcode == 4:
            if self.checkZipFile(self.filename_absolutePath) == 0:
                print "INFO: Product Insertion on DB: %s" % filename
                logger.WriteLog("INFO: Product Insertion on DB: %s" % filename)
                self.probaDB.insertEntryDB(
                    '''INSERT INTO PROBA_PRODUCTS (product_name,%s,insertion_date,product_type,complete) VALUES ('%s','%s','%s','%s','%s')''' % (
                    extension_element, filename, myConstants.SQLITE_TRUE, time.time(), prodType,
                    myConstants.SQLITE_FALSE))
            else:
                return -1

        if exitcode == 3:
            print "INFO: Product already present on DB: %s" % filename
            logger.WriteLog("WARN: Product already present on DB: %s" % filename)
            return -1
        if exitcode == -1:
            logger.WriteLog("ERROR: Something wrong during execution of query on DB for: %s" % filename)
            print "ERROR: Something wrong during execution of query on DB for: %s" % filename
            return exitcode
        return 0

    def check_lta_archiving_status(self, nameProduct):
        logger.WriteLog("Check LTA Archiving Status...")
        resultSet = self.probaDB.queryDB(
            "SELECT * FROM PROBA_PRODUCTS WHERE product_name='%s' AND complete='%s' AND lta_archiving_status='%s'" % (
                nameProduct, myConstants.SQLITE_TRUE, myConstants.SQLITE_FALSE))
        if len(resultSet) > 0:
            return False
        else:
            return True

    def createManifestMD5(self):
        file_to_search = os.path.join(self.LTA_Path, self.filename)
        logger.WriteLog("creating Manifest File on %s.manifest" % file_to_search)
        list_file = glob.glob("%s*" % file_to_search)
        manifest = open("%s.manifest" % file_to_search, 'w')
        lineManifest = None
        for f in list_file:
            lineManifest = "%s  %s\n" % (f, md5.createMD5(f))
            manifest.write(lineManifest)
        manifest.flush()
        manifest.close()

    def ingestInLTA(self):

        if not self.check_lta_archiving_status(self.filename):
            print "Archiving in LTA..."
            logger.WriteLog("Archiving in LTA")
            exitCode = self.hardLinkLongTermArchive(self.filename_absolutePath, self.LTA_Path)
            if exitCode is None:
                self.setArchiveFlag(self.filename)
                logger.WriteLog("Writing %s.ini" % self.filename)
                self.createManifestMD5()
                bannerLTA = "\n###############################################################################################\n\n" \
                            "                                     Archiving %s in LTA...!\n\n" \
                            "###############################################################################################\n" % self.filename
                print bannerLTA
                logger.WriteLog(" -------------------- Archiving %s in LTA...! --------------------- " % self.filename)
                return 0
            else:
                logger.WriteLog(
                    " -------------------- ERROR of %s during archiving in LTA...! --------------------- " % self.filename)
                return -1
        else:
            print "Product already stored in LTA"
            logger.WriteLog("Product Already stored in LTA")
            return 0

    def setArchiveFlag(self, nameProduct):
        self.probaDB.updateEntryDB(
            "UPDATE PROBA_PRODUCTS SET lta_archiving_status='%s' WHERE product_name='%s'" % (
                myConstants.SQLITE_TRUE, nameProduct))

    def hardLinkLongTermArchive(self, absFileTgt, folderLinkPath):

        listToArchive = glob.glob("%s*" % self.filename_absolutePath[:-4])
        exitCode = None
        for fn in listToArchive:
            print "Hard link in LTA for %s" % fn
            logger.WriteLog("Hard link in LTA for %s" % fn)
            try:
                linkName = os.path.join(folderLinkPath, os.path.basename(fn))
                if os.path.exists(linkName):
                    # raise Exception("Link already exists")
                    print "WARN: Link already exists"
                    logger.WriteLog("WARN: Link already exists")
                else:
                    # exitCode = os.link(fn, linkName)
                    exitCode = shutil.copy(fn, linkName)
                    print "Files  H-Linked in LTA: %s " % linkName
                    logger.WriteLog("Files  H-Linked in LTA: %s " % linkName)
            except OSError:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                traceback.print_exc(file=sys.stdout)
                print "OSERROR: File %s Already Present in LTA" % absFileTgt
                logger.WriteLog("OSERROR: File %s Already Present in LTA" % absFileTgt)
                continue
            except:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                print "Error moving to LTA '%s': %s %s" % (type, absFileTgt, exc_obj)
                logger.WriteLog("ERROR: Error moving to LTA '%s': %s %s" % (type, absFileTgt, exc_obj))
                traceback.print_exc(file=sys.stdout)
                continue
        return exitCode


class DBWatcher:

    def __init__(self, pathDB, reportPath):
        print "Daemon is polling on DB %s \n" % pathDB
        logger.WriteLog("Daemon is polling on DB %s \n" % pathDB)
        self.db = DataBaseLite(pathDB, None)
        self.emailTo = "dissemination@serco.com"
        self.timeLimit = (3600 * 24)
        self.dailyReport = ReportReader(reportPath, pathDB)

    def sendMail(self, productName):

        try:
            text = "ERROR: Product %s is incomplete since %s seconds (more than %s day) " % (
                productName, self.timeLimit, int(self.timeLimit / 86400))
            subject = "PROBA time limit exceeded"
            serverSMTP = "localhost"
            destination = self.emailTo
            destination_CC = ""
            fromRecipient = "Dissemination Team <info@apps.eo.esa.int>"
            Mail(fromRecipient, destination, subject, serverSMTP, "", destination_CC, "mail.log", text, 25).sendEmail()

        except Exception as s:
            logger.WriteLog(s)
            print s

    def checkIncomplete(self, timeLimit):
        name_resultSet = self.db.queryDB(
            "SELECT product_name FROM PROBA_PRODUCTS WHERE (%s - (SELECT insertion_date from PROBA_PRODUCTS)) > %s AND complete='%s' AND check_done='%s'" % (
                datetime.now().strftime('%s'), timeLimit, myConstants.SQLITE_FALSE, myConstants.SQLITE_FALSE))
        for name in name_resultSet:
            print "Sending mail for incomplete product %s" % name
            logger.WriteLog("Sending Mail for incomplete product %s" % name)
            self.sendMail(name)
            self.setCheckDoneFlag(name)
        return name_resultSet

    def setCheckDoneFlag(self, fileName):
        self.db.updateEntryDB(
            "UPDATE PROBA_PRODUCTS SET check_done='%s' WHERE product_name='%s'" % (myConstants.SQLITE_TRUE, fileName))

    def runCheck(self, dailyHourCrontab):

        print "Start Proba DB-Watcher Daemon..."
        pType = ["PROBA_CHRIS", "PROBA_HRC"]
        daysDelta = 1
        while True:

            if datetime.now().hour == dailyHourCrontab and datetime.now().minute == 30:
                for _pType in pType:
                    body = self.dailyReport.createDailyReport(_pType, daysDelta)
		    s = _pType.replace("PROBA_","PROBA-1 ")
		    mail = Mail("Dissemination Team <info@apps.eo.esa.int>", "roberto.biasutti@esa.int,bruno.schmitt@esa.int","%s daily report (%s)" % (s,(datetime.now() - timedelta(daysDelta)).strftime("%a %b %d %Y")), hostMail='localhost', bcc=None, cc="dissemination@serco.com,giulio82.villani@libero.it", logFile=None, textMail=body, port=25)
#                    mail = Mail("Dissemination Team <info@apps.eo.esa.int>", "giulio82.villani@libero.it",                                "%s daily report (%s)" % (                                _pType, (datetime.now() - timedelta(daysDelta)).strftime("%a %b %d %Y")), hostMail='localhost',                                bbc=None, cc="alessandro.maltese@gmail.com", logFile=None, textMail=body, port=25)
                    mail.sendEmail(True)
		    logger.WriteLog("Crontab event: sent daily report at %s:%s" % (dailyHourCrontab,datetime.now().minute))
		print "Crontab Python of %s:%s" % (dailyHourCrontab,datetime.now().minute)
                time.sleep(61)

if __name__ == '__main__':

    dbW = DBWatcher("/nfsdata/nfsdata02/databases/converter_live/ProbaDB.sqlite", "/home/datamanager/converter-live/reports/")
    dbW.runCheck(14)
