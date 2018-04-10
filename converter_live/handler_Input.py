from utilities import md5
import logger
from threading import Thread
import os
import traceback
from zipfile import BadZipfile
import inotify.adapters
import shutil
import str2bool
import hashlib
from os import link
import sys
from PROBA_product import DBWatcher
from PROBA_product import PROBA_product
from TLE_handler import TLE
from service import Service
from datetime import datetime
from ReportFile import ReportFile
from mail import Mail

#
import http_notification_client

GRAPHITE_EVENTS_URL = 'http://172.17.63.32:8083/events'
#

#####################################
#    Properties file parameter      #
#          Description              #
#####################################

CONVERTER_PATH = "CONVERTER_PATH"
CONVERTER_CONF = "CONVERTER_CONF"
SINGLE_REPORT_PATH_FOLDER = "SINGLE_REPORT_PATH_FOLDER"
DB_PATH = "DB_PATH"

IN_DIR_COSMO = "IN_DIR_COSMO"
OUT_DIR_COSMO = "OUT_DIR_COSMO"
LINK_LTA_COSMO = "LINK_LTA_COSMO"
TMP_LOADS_COSMO = "TMP_LOADS_COSMO"

IN_DIR_PROBA = "IN_DIR_PROBA"
OUT_DIR_PROBA = "OUT_DIR_PROBA"
LINK_LTA_PROBA = "LINK_LTA_PROBA"
TMP_LOADS_PROBA = "TMP_LOADS_PROBA"
OUT_REGEX_PROBA = "OUT_REGEX_PROBA"

IN_DIR_PROBA_HRC = "IN_DIR_PROBA_HRC"
OUT_DIR_PROBA_HRC = "OUT_DIR_PROBA_HRC"
LINK_LTA_PROBA_HRC = "LINK_LTA_PROBA_HRC"
TMP_LOADS_PROBA_HRC = "TMP_LOADS_PROBA_HRC"
OUT_REGEX_PROBA_HRC = "OUT_REGEX_PROBA_HRC"

IN_DIR_DEIMOS = "IN_DIR_DEIMOS"
OUT_DIR_DEIMOS = "OUT_DIR_DEIMOS"
LINK_OADS_DEIMOS = "LINK_OADS_DEIMOS"
TMP_LOADS_DEIMOS = "TMP_LOADS_DEIMOS"

IN_DIR_OCEANSAT = "IN_DIR_OCEANSAT"
OUT_DIR_OCEANSAT = "OUT_DIR_OCEANSAT"
LINK_OADS_OCEANSAT = "LINK_OADS_OCEANSAT"
TMP_LOADS_OCEANSAT = "TMP_LOADS_OCEANSAT"

REPORT_PATH_FOLDER = "REPORT_PATH_FOLDER"
OADS_INBASKET = "OADS_INBASKET"
TMP_FOLDER_LOADS = "TMP_FOLDER_LOADS"
EMAIL_TO = "EMAIL_TO"

JAVA_VM = "/nfsdata/nfsdata02/CONVERTERS/JAVA_CONVERTER/converter-COSMO/jre1.8.0_144/bin/java"
ownership = "datamanager:datamanager"

####################################
#    End Properties description    #
####################################
import glob


#
# send graphite events
#
def sendGraphiteEvent(what, tags, data):
    try:
        http_notification_client.sendGraphiteEvent(GRAPHITE_EVENTS_URL, what, tags, data)
    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print " problem using http_notification_client: %s %s" % (exc_type, exc_obj)
        traceback.print_exc(file=sys.stdout)


def find(regex):
    list = []
    for f in glob.iglob(regex):  # generator, search immediate subdirectories
        list.append(f)
    return list


class Handler_JavaConverter:

    def __init__(self, propFilePath):

        self.propPath = propFilePath
        self.serv = Service()
        self.serv.init(self.propPath)
        self.converterPath = self.serv.getProperty(CONVERTER_PATH)
        self.converterConf = self.serv.getProperty(CONVERTER_CONF)
        self.reportDir = self.serv.getProperty(REPORT_PATH_FOLDER)
        self.singleReportDir = self.serv.getProperty(SINGLE_REPORT_PATH_FOLDER)
        self.confValueCOSMO = self.loadConfig("COSMO")
        self.confValuePROBA_CHRIS = self.loadConfig("PROBA_CHRIS")
        self.confValuePROBA_HRC = self.loadConfig("PROBA_HRC")
        self.oadsInBasket = self.serv.getProperty(OADS_INBASKET)
        self.tmpFolderLoads = self.serv.getProperty(TMP_FOLDER_LOADS)
        self.JAVA_VM = self.serv.getProperty(JAVA_VM)
        self.EMAIL_TO = self.serv.getProperty(EMAIL_TO)
        self.generalDBPATH = self.serv.getProperty(DB_PATH)
        self.reportInboxLive = ReportFile(self.reportDir, "LiveInbox")

    def loadConfig(self, collection):
        logger.WriteLog("Loading properties parameters for %s" % collection)
        first = None
        second = None
        third = None
        fourth = None
        fifth = None

        if collection == "COSMO":
            self.in_dir_COSMO = self.serv.getProperty(IN_DIR_COSMO)
            self.LINK_LTA_COSMO = self.serv.getProperty(LINK_LTA_COSMO).strip()
            self.OUTBOX_COSMO = self.serv.getProperty(OUT_DIR_COSMO)
            self.tmpFolderLoads_COSMO = self.serv.getProperty(TMP_LOADS_COSMO)
            first = self.in_dir_COSMO
            second = self.LINK_LTA_COSMO
            third = self.OUTBOX_COSMO
            fourth = self.tmpFolderLoads_COSMO

        if collection == "OCEANSAT":
            self.in_dir_OCEANSAT = self.serv.getProperty(IN_DIR_OCEANSAT)
            self.LINK_OCEANSAT = str2bool.str2bool(self.serv.getProperty(LINK_OADS_OCEANSAT).strip())
            self.OUTBOX_OCEANSAT = self.serv.getProperty(OUT_DIR_OCEANSAT)
            self.tmpFolderLoads_OCEANSAT = self.serv.getProperty(TMP_LOADS_OCEANSAT)
            first = self.in_dir_OCEANSAT
            second = self.LINK_OCEANSAT
            third = self.OUTBOX_OCEANSAT
            fourth = self.tmpFolderLoads_OCEANSAT

        if collection == "DEIMOS":
            self.in_dir_DEIMOS = self.serv.getProperty(IN_DIR_DEIMOS)
            self.LINK_DEIMOS = str2bool.str2bool(self.serv.getProperty(LINK_OADS_DEIMOS).strip())
            self.OUTBOX_DEIMOS = self.serv.getProperty(OUT_DIR_DEIMOS)
            self.tmpFolderLoads_DEIMOS = self.serv.getProperty(TMP_LOADS_DEIMOS)
            first = self.in_dir_DEIMOS
            second = self.LINK_DEIMOS
            third = self.OUTBOX_DEIMOS
            fourth = self.tmpFolderLoads_DEIMOS

        if collection == "PROBA_CHRIS":
            self.in_dir_PROBA = self.serv.getProperty(IN_DIR_PROBA)
            self.LINK_LTA_PROBA = self.serv.getProperty(LINK_LTA_PROBA).strip()
            self.OUTBOX_PROBA = self.serv.getProperty(OUT_DIR_PROBA)
            self.tmpFolderLoads_PROBA = self.serv.getProperty(TMP_LOADS_PROBA)
            self.regex = self.serv.getProperty(OUT_REGEX_PROBA)
            first = self.in_dir_PROBA
            second = self.LINK_LTA_PROBA
            third = self.OUTBOX_PROBA
            fourth = self.tmpFolderLoads_PROBA
            fifth = self.regex

        if collection == "PROBA_HRC":
            self.in_dir_PROBA_HRC = self.serv.getProperty(IN_DIR_PROBA_HRC)
            self.LINK_LTA_PROBA_HRC = self.serv.getProperty(LINK_LTA_PROBA_HRC).strip()
            self.OUTBOX_PROBA_HRC = self.serv.getProperty(OUT_DIR_PROBA_HRC)
            self.tmpFolderLoads_PROBA_HRC = self.serv.getProperty(TMP_LOADS_PROBA_HRC)
            self.regex_HRC = self.serv.getProperty(OUT_REGEX_PROBA_HRC)
            first = self.in_dir_PROBA_HRC
            second = self.LINK_LTA_PROBA_HRC
            third = self.OUTBOX_PROBA_HRC
            fourth = self.tmpFolderLoads_PROBA_HRC
            fifth = self.regex_HRC

        return (first, second, third, fourth, fifth)


    def generateReport(self, reportName, singleReportDir):
        report_ = ReportFile(self.reportDir, reportName, singleReportDir)
        return report_

    def checkInputFile(self, nameFile, productType):

        if (nameFile.endswith('.tgz') or nameFile.endswith('.tar.gz')) and not nameFile.startswith('.'):
            print "Checking input file and Naming Convention"
            print "Untar test running for %s/..." % nameFile
            logger.WriteLog("Checking input file and Naming Convention")
            logger.WriteLog("Untar test running for %s/..." % nameFile)
            if os.system('tar tvzf %s' % os.path.join(self.in_dir_COSMO, nameFile)) == 0:
                print "Untar test succesfully done for CosmoSkyMed"
                logger.WriteLog("Untar test succesfully done for CosmoSkyMed")
                return "COSMO"
            else:
                self.sendMail("COSMO", nameFile)
                self.reportInboxLive.writeLiveInboxLine("ERROR: File FAILED in InBOX", productType, nameFile)
                return False

        if nameFile.endswith('.zip') and nameFile.startswith('DE'):
            print "Checking input file and Naming Convention"
            print "UnZip test running..."
            if os.system('unzip -l %s' % os.path.join(self.in_dir_DEIMOS, nameFile)) == 0:
                print "Uzip test succesfully done for DEIMOS product"
                return "DEIMOS"

        ######## FIRST PHASE PROBA ##########
        if (productType == "PROBA_CHRIS" or productType == "PROBA_HRC") and not nameFile.endswith(
                "_ToCONVERT.zip") and not nameFile.endswith(".SIP.ZIP") and not nameFile.endswith("lock"):
            print "Checking input file and Naming Convention (first phase) %s " % nameFile
            logger.WriteLog("Checking input file and Naming Convention (first phase) %s " % nameFile)
            sendGraphiteEvent('NewInput', ['liveConversion', 'proba', 'newInput'], nameFile)
            listValues = self.loadConfig(productType)
            try:
                product_input = PROBA_product(productType, nameFile, listValues[0], listValues[1], listValues[2])
                exitCode = product_input.run()
                if exitCode == 0 or exitCode == None:
                    product_input.ingestInLTA()
                    # self.hardLinkLongTermArchive(nameFile, listValues[1])
                    return -1
                else:
                    self.reportInboxLive.writeLiveInboxLine("Problem: (FirstPhase) File FAILED in InBOX", productType,
                                                            nameFile)
                    return False
            except BadZipfile as ex:
                print "Problem: Bad Zip File in InBOX"
                logger.WriteLog("Problem: Bad Zip File in InBOX")
                self.sendMail(productType, nameFile, "Bad Zip File")
                self.reportInboxLive.writeLiveInboxLine("Problem: (FirstPhase) Bad Zip File in InBOX", productType,
                                                        nameFile)
                return False
            except Exception as e:
                print "Problem: (FirstPhase) Generic Exception in Inbox"
                print e
                exc_type, exc_obj, exc_tb = sys.exc_info()
                traceback.print_exc(file=sys.stdout)
                logger.WriteLog("Problem: (FirstPhase) Generic Exception in Inbox\n %s" % e)
                self.sendMail(productType, nameFile,
                              "Generic Exception in Inbox: %s %s \n %s " % (exc_type, exc_obj, exc_tb))
                self.reportInboxLive.writeLiveInboxLine("Problem: (FirstPhase) Generic Exception in Inbox", productType,
                                                        nameFile)

        ######### SECOND PHASE PROBA ############
        if (productType == "PROBA_CHRIS" or productType == "PROBA_HRC") and nameFile.endswith("_ToCONVERT.zip"):
            print "Checking input file and Naming Convention (second phase) %s" % nameFile
            logger.WriteLog("Checking input file and Naming Convention (second phase) %s" % nameFile)
            listValues = self.loadConfig(productType)
            product_input = PROBA_product(productType, nameFile.replace("_ToCONVERT", ""), listValues[0], listValues[1],
                                          listValues[2])
            if not product_input.checkAlreadyConverted():
                product_input.setAlreadyConverted()
                return productType
            else:
                return -1
        return -1

    def hardLinkLongTermArchive(self, absFileTgt, folderLinkPath):
        try:
            name = os.path.basename(absFileTgt)
            linkName = os.path.join(folderLinkPath, name)
            exitCode = shutil.copy(absFileTgt, linkName)
            # exitCode = os.link(absFileTgt,linkName)
            print "File copied in LTA: %s " % linkName

            return exitCode
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print "Error moving to LTA '%s': %s %s" % (type, absFileTgt, exc_obj)
            logger.WriteLog("File copied in LTA: %s " % linkName)
            traceback.print_exc(file=sys.stdout)
            raise Exception("Error moving to LTA '%s': %s %s" % (type, absFileTgt, exc_obj))

    def sendMail(self, collectionName, nameFile, message):

        try:
            text = "ERROR during conversion of %s product (%s) - %s -" % (collectionName, nameFile, message)
            subject = "PROBA Conversion Error"
            serverSMTP = "localhost"
            destination = self.serv.getProperty(EMAIL_TO)
            destination_CC = self.serv.getProperty("EMAIL_CC")
            fromRecipient = "Dissemination Team <info@apps.eo.esa.int>"

            m = Mail(fromRecipient, destination, subject, serverSMTP, "",
                     destination_CC, "mail.log", text, 25)
            m.sendEmail()
        except Exception as s:
            logger.WriteLog(s)
            traceback._print(s)
            print s

    def getCollectionList(self):

        listPath = self.serv.getpropertiesByRegeX("IN_DIR_")
        listCollection = []
        for element in listPath:
            listCollection.append(element.split("_")[2])
        return listCollection

    def runIngester(self, outBox, _report, tmpDir, regex):

        print "Moving data from %s on LOADS tmp folder...%s" % (outBox, tmpDir)
        logger.WriteLog("Moving data from %s on LOADS tmp folder...%s" % (outBox, tmpDir))
        exitCode = 0
        listEOSIP = glob.glob("%s/%s" % (outBox, regex))

        for i in listEOSIP:
            exitCode = exitCode + os.system("mv %s %s" % (i, tmpDir))
            print "Moved %s in %s" % (i, tmpDir)
            logger.WriteLog("Moved %s in %s" % (i, tmpDir))
        if exitCode == 0:
            print "Change ownership on file"
            exitCode = os.system("chown -R %s %s" % (ownership, tmpDir))
            logger.WriteLog("Change ownership on file (%s) " % tmpDir)
        if exitCode == 0:
            print "Change permissions on file"
            exitCode = os.system("chmod -R 755 %s" % tmpDir)
            logger.WriteLog("Change permissions on file (%s)" % tmpDir)
        if exitCode == 0 and len(glob.glob(os.path.join(tmpDir, "*"))) != 0:
            print "Moving data on LOADS inbasket folder... %s" % self.oadsInBasket
            logger.WriteLog("Moving data on LOADS inbasket folder... %s" % self.oadsInBasket)
            exitCode = os.system("mv %s %s" % (os.path.join(tmpDir, "*"), self.oadsInBasket))
        return exitCode

    def cyclingOnDir(self, inotifyObj, colletionName, Conversion, Ingest):

        collectionConfigurations = self.loadConfig(colletionName)
        reportFinal = self.generateReport(colletionName, self.singleReportDir)

        for event in inotifyObj.event_gen():

            if str(datetime.now().strftime("%Y%m%d")).endswith("01") and not reportFinal.getReportPath().__contains__(
                    datetime.now().strftime("%Y%m")):
                reportFinal = self.generateReport(colletionName, self.singleReportDir)

            if event is not None:
                (header, type_names, watch_path, filename) = event
                singleFile = os.path.join(watch_path, filename)

                if type_names[0] == 'IN_CLOSE_WRITE' or type_names[0] == 'IN_MOVED_TO':
                    current_collectionName = self.checkInputFile(os.path.join(watch_path, filename), colletionName)

                    exitCodeConvertion = 0
                    exitCodeIngestion = 0

                    if current_collectionName != False and current_collectionName != -1:
                        # run CONVERTION
                        if Conversion == True:
                            exitCodeConvertion = self.runConversion(current_collectionName, watch_path, filename,
                                                                    reportFinal)
                            sendGraphiteEvent('ConversionDone', ['liveConversion', 'proba', 'eosipConversion'],
                                              filename)
                            if exitCodeConvertion == 0:
                                print "\n\n###############################################################################################\n\n" \
                                      "    EOSIP CONVERTION OF %s HAS BEEN DONE!\n\n" \
                                      "###############################################################################################\n\n" % filename
                                logger.WriteLog(
                                    "####################### EOSIP CONVERTION OF %s HAS BEEN DONE! #########################" % filename)
                        # run INGESTER
                        if Ingest == True and exitCodeConvertion == 0:
                            exitCodeIngestion = self.runIngester(collectionConfigurations[2], reportFinal,
                                                                 collectionConfigurations[3],
                                                                 collectionConfigurations[4])
                            sendGraphiteEvent('IngestionDone', ['liveConversion', 'proba', 'ingestion'], filename)
                            if exitCodeConvertion == 0:
                                print "\n\n###############################################################################################\n\n" \
                                      "    LOADS INGESTION OF %s HAS BEEN DONE!\n\n" \
                                      "###############################################################################################\n\n" % filename
                                logger.WriteLog(
                                    "####################### LOADS INGESTION OF %s HAS BEEN DONE! #########################" % filename)

                    if current_collectionName != -1:
                        # UPDATE CONVERTER REPORT
                        if ((exitCodeConvertion != 0 or current_collectionName == False) and Conversion == True):
                            reportFinal.writeLine(singleFile, "FAILED", "N/A")
                        elif Conversion == False:
                            pass
                        else:
                            reportFinal.writeLine(singleFile, "SUCCESS", md5.createMD5(singleFile))
                        # UPDATE INGESTER REPORT

                        if Ingest is False or exitCodeConvertion != 0:
                            pass
                        elif ((exitCodeIngestion != 0) and (Ingest is True) and (exitCodeConvertion == 0)):
                            reportFinal.writeLineIngester(filename, "FAILED")
                        else:
                            reportFinal.writeLineIngester(filename, "INGESTED")

        print "Handler phase complete!\n\n\n"
        logger.WriteLog("Handler phase complete!\n\n")
        pass

    def runConversion(self, _collectionName, _watch_path, _filename, _report):

        print "Preparing to convert %s products" % _filename
        logger.WriteLog("Preparing to convert %s products" % _filename)
        singleFile = os.path.join(_watch_path, _filename)
        if os.path.isfile(singleFile):
            print "%s -jar %s -collectionName %s -s %s -c %s -i 1" % (
            JAVA_VM, self.converterPath, _collectionName, singleFile, self.converterConf)
            logger.WriteLog("%s -jar %s -collectionName %s -s %s -c %s -i 1" % (
            JAVA_VM, self.converterPath, _collectionName, singleFile, self.converterConf))
            exitCode = os.system("%s -jar %s -collectionName %s -s %s -c %s -i 1" % (
                JAVA_VM, self.converterPath, _collectionName, singleFile, self.converterConf))
        else:
            exitCode = -1
        return exitCode

    def run(self):
        sendGraphiteEvent('Starting', ['liveConversion', 'proba', 'starting'], 'Starting live converter')
        i_cosmo = inotify.adapters.InotifyTree(self.in_dir_COSMO)
        i_proba_CHRIS_firstPhase = inotify.adapters.InotifyTree(self.in_dir_PROBA)
        i_proba_CHRIS_secondPhase = inotify.adapters.InotifyTree(self.OUTBOX_PROBA)
        i_proba_HRC_firstPhase = inotify.adapters.InotifyTree(self.in_dir_PROBA_HRC)
        i_proba_HRC_secondPhase = inotify.adapters.InotifyTree(self.OUTBOX_PROBA_HRC)

        Thread(target=self.cyclingOnDir, args=(i_cosmo, "COSMO", True, True,)).start()
        Thread(target=self.cyclingOnDir, args=(i_proba_HRC_firstPhase, "PROBA_HRC", False, False,)).start()
        Thread(target=self.cyclingOnDir, args=(i_proba_HRC_secondPhase, "PROBA_HRC", True, True,)).start()
        Thread(target=self.cyclingOnDir, args=(i_proba_CHRIS_firstPhase, "PROBA_CHRIS", False, False,)).start()
        Thread(target=self.cyclingOnDir, args=(i_proba_CHRIS_secondPhase, "PROBA_CHRIS", True, True,)).start()
        tle = TLE("https://celestrak.com/NORAD/elements/engineering.txt", "sat26958.txt", 'ProbaTLE_sat26958.txt',
                  " 26958U ", 900)
        Thread(target=tle.polling).start()
        Thread(target=DBWatcher(os.path.join(self.generalDBPATH, "ProbaDB.sqlite")).runCheck, args=23)


if __name__ == '__main__':

    try:
        banner = "##################################################################################\n" \
                 "##      _________                                                               ##\n" \
                 "##     /        /\      _      _____    ____          dissemination@serco.com   ##\n" \
                 "##    /  LTC   /  \    | |    |_   _|  |  __|                                   ##\n" \
                 "##   /        /    \   | |__    | |    | |__                                    ##\n" \
                 "##  /________/      \  |____| o |_| o  |____| o                   LTC-project   ##\n" \
                 "##  \        \  LTC /                                                           ##\n" \
                 "##   \        \    /  --------------------------------------------------------  ##\n" \
                 "##    \  LTC   \  /    Live TPM Converter (ver 1.0)        Dissemination Team   ##\n" \
                 "##     \________\/    --------------------------------------------------------  ##\n" \
                 "##                                                                              ##\n" \
                 "##################################################################################\n" \
                 "##                                                                              ##\n" \
                 "##                              Starting Circulator                             ##\n" \
                 "##                                                                              ##\n" \
                 "##################################################################################\n"

        logger.LoggingInit("./logs/", "handler.log", html=False)
        print banner
        logger.WriteLog("Start LOGGER")
        hand = Handler_JavaConverter("handler.properties")
        hand.run()
    except KeyboardInterrupt:

        print "\nbye bye"
        text = "WARNING! The handler has been aborted by a wrong error management!"
        subject = "HANDLER ABORTED"
        serverSMTP = "localhost"
        destination = "giulio82.villani@libero.it"
        fromRecipient = "Dissemination Team <info@apps.eo.esa.int>"
        m = Mail(fromRecipient, destination, subject, serverSMTP, None, None, "mail.log", text, 25)
        m.sendEmail()
