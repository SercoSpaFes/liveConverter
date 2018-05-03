from utilities import md5
import logger
from threading import Thread
import os
import traceback
from zipfile import BadZipfile
import glob
import inotify.adapters
import shutil
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

CONVERTER_PATH = "converter_path"
CONVERTER_CONF = "converter_conf"
CONVERTER_REPORT_DIR = "converter_report_dir"

SINGLE_REPORT_PATH_FOLDER = "single_report_path_folder"
DB_PATH = "db_path"

REPORT_PATH_FOLDER = "report_path_folder"
OADS_INBASKET = "oads_inbasket"
TMP_FOLDER_LOADS = "tpm_folder_loads"
EMAIL_TO = "email_to"

JAVA_VM = "/nfsdata/nfsdata02/CONVERTERS/JAVA_CONVERTER/converter-COSMO/jre1.8.0_144/bin/java"
ownership = "datamanager:datamanager"

IN_DIR = "in_dir"
OUT_DIR = "out_dir"
LINK_LTA = "link_lta"
TMP_LOADS = "tpm_loads"
OUT_REGEX = "out_regex"

JAVA_VM = "/nfsdata/nfsdata02/CONVERTERS/JAVA_CONVERTER/converter-COSMO/jre1.8.0_144/bin/java"
ownership = "oadsrun:oads"

####################################
#    End Properties description    #
####################################



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
        self.confValuesForGlobal = self.loadMapConfig(propFilePath, "GLOBAL")
        self.confPROBA_CHRIS = self.loadMapConfig(propFilePath, "PROBA_CHRIS")
        self.confPROBA_HRC = self.loadMapConfig(propFilePath, "PROBA_HRC")
        self.confCOSMO = self.loadMapConfig(propFilePath, "COSMO")
        self.converterPath = self.confValuesForGlobal.get(CONVERTER_PATH)
        self.converterConf = self.confValuesForGlobal.get(CONVERTER_CONF)
        self.reportDir = self.confValuesForGlobal.get(REPORT_PATH_FOLDER)
        self.singleReportDir = self.confValuesForGlobal.get(SINGLE_REPORT_PATH_FOLDER)
        self.oadsInBasket = self.confValuesForGlobal.get(OADS_INBASKET)
        self.tmpFolderLoads = self.confValuesForGlobal.get(TMP_FOLDER_LOADS)
        self.JAVA_VM = self.confValuesForGlobal.get(JAVA_VM)
        self.EMAIL_TO = self.confValuesForGlobal.get(EMAIL_TO)
        self.generalDBPATH = self.confValuesForGlobal.get(DB_PATH)
        self.reportInboxLive = ReportFile(self.reportDir, "LiveInbox")
        self.reportDirConverter = self.confValuesForGlobal.get(CONVERTER_REPORT_DIR)
        self.reportDirConverter = self.confValuesForGlobal.get(CONVERTER_REPORT_DIR)


    def loadMapConfig(self, pathProp, collection, value=None):
        a = Loader(pathProp)
        propertiesMap = a.getIteamsForSection(collection)
        if value is None:
            return propertiesMap
        else:
            return propertiesMap.get(value)


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

            ######## FIRST PHASE PROBA ##########
        if (productType == "PROBA_CHRIS" or productType == "PROBA_HRC") and not nameFile.endswith(
                "_ToCONVERT.zip") and not nameFile.endswith(".SIP.ZIP") and not nameFile.endswith("lock"):
            print "Checking input file and Naming Convention (first phase) %s " % nameFile
            logger.WriteLog("Checking input file and Naming Convention (first phase) %s " % nameFile)
            sendGraphiteEvent('NewInput', ['liveConversion', 'proba', 'newInput'], nameFile)
            listValues = self.loadMapConfig(self.propPath, productType)
            try:
                product_input = PROBA_product(productType, nameFile, listValues.get("IN_DIR"), listValues.get("LINK_LTA"),
                                              listValues.get("OUT_DIR"))
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
            listValues = self.loadMapConfig(self.propPath, productType)
            product_input = PROBA_product(productType, nameFile.replace("_ToCONVERT", ""), listValues.get("IN_DIR"),
                                          listValues.get("LINK_LTA"), listValues.get("OUT_DIR"))

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

    def startingReportPhase(self, colletionName, reportFinal):
        if str(datetime.now().strftime("%Y%m%d")).endswith("01") and not reportFinal.getReportPath().__contains__(
                datetime.now().strftime("%Y%m")):
            reportFinal = self.generateReport(colletionName, self.singleReportDir)

        return reportFinal


    def writeResultOnReport(self, current_collectionName, exitCodeConversion, Conversion, reportFinal, singleFile,
                            exitCodeIngestion, Ingest, filename):
        # UPDATE CONVERTER REPORT
        if ((exitCodeConversion != 0 or current_collectionName == False) and Conversion == True):
            reportFinal.writeLine(singleFile, "FAILED", "N/A")
        elif Conversion == False:
            pass
        else:
            reportFinal.writeLine(singleFile, "SUCCESS", self.createMD5(singleFile))
        # UPDATE INGESTER REPORT
        if ((
                exitCodeIngestion != 0 or current_collectionName == False) and Ingest == True and exitCodeConversion != 0):
            reportFinal.writeLineIngester(filename, "FAILED")
        else:
            reportFinal.writeLineIngester(filename, "INGESTED")

    def cyclingOnDir(self, inotifyObj, colletionName, Conversion, Ingest):

        collectionConfigurations = self.loadMapConfig(self.propPath, colletionName)
        reportFinal = self.generateReport(colletionName, self.singleReportDir)

        for event in inotifyObj.event_gen():

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
                            exitCodeConversion = self.conversionPhase(current_collectionName, watch_path, filename,
                                                                      reportFinal)

                        # run INGESTER
                        if Ingest == True and exitCodeConversion == 0:
                            exitCodeIngestion = self.ingestPhase(collectionConfigurations, reportFinal, filename)

                    if current_collectionName != -1:
                        self.writeResultOnReport(current_collectionName, exitCodeConversion, Conversion, reportFinal,
                                                 singleFile, exitCodeIngestion, Ingest, filename)

        print "Handler phase complete!\n\n\n"
        logger.WriteLog("Handler phase complete!\n\n\n")
        pass


    def conversionPhase(self, current_collectionName, watch_path, filename,  reportFinal):
        exitCodeConversion = self.runConversion(current_collectionName, watch_path, filename, reportFinal)
        sendGraphiteEvent('ConversionDone', ['liveConversion', 'proba', 'eosipConversion'], filename)
        if exitCodeConversion == 0:
            conversionBanner = "\n\n###############################################################################################\n\n" \
                               "    EOSIP Conversion OF %s HAS BEEN DONE!\n\n" \
                               "###############################################################################################\n\n" % filename
            print conversionBanner
            logger.WriteLog(conversionBanner)

        return exitCodeConversion


    def ingestPhase(self, collectionConfigurations, reportFinal,filename):
        exitCodeIngestion = self.runIngester(collectionConfigurations.get("OUT_DIR"), reportFinal,
                                             collectionConfigurations.get("TMP_LOADS"),
                                             collectionConfigurations.get("OUT_REGEX"))
        sendGraphiteEvent('IngestionDone', ['liveConversion', 'proba', 'ingestion'], filename)
        if exitCodeIngestion == 0:
            ingestionBanner = "\n\n###############################################################################################\n\n" \
                              "    LOADS INGESTION OF %s HAS BEEN DONE!\n\n" \
                              "###############################################################################################\n\n" % filename
            print ingestionBanner
            logger.WriteLog(ingestionBanner)

        return exitCodeIngestion


    def runConversion(self, _collectionName, _watch_path, _filename, _report):
        print "Preparing to convert %s products" % _filename

        singleFile = os.path.join(_watch_path, _filename)
        if os.path.isfile(singleFile):
            print "%s -jar %s -collectionName %s -s %s -c %s -i 1" % (
                JAVA_VM, self.converterPath, _collectionName, singleFile, self.converterConf)
            exitCode = os.system("%s -jar %s -collectionName %s -s %s -c %s -i 1" % (
                JAVA_VM, self.converterPath, _collectionName, singleFile, self.converterConf))
            logger.WriteLog("%s -jar %s -collectionName %s -s %s -c %s -i 1" % (
                JAVA_VM, self.converterPath, _collectionName, singleFile, self.converterConf))
        else:
            exitCode = -1
        return exitCode


    def run(self):
        sendGraphiteEvent('Starting', ['liveConversion', 'proba', 'starting'], 'Starting live converter')
        i_cosmo = inotify.adapters.InotifyTree(self.confCOSMO.get(IN_DIR))
        i_proba_CHRIS_firstPhase = inotify.adapters.InotifyTree(self.confPROBA_CHRIS.get(IN_DIR))
        i_proba_CHRIS_secondPhase = inotify.adapters.InotifyTree(self.confPROBA_CHRIS.get(OUT_DIR))
        i_proba_HRC_firstPhase = inotify.adapters.InotifyTree(self.confPROBA_HRC.get(IN_DIR))
        i_proba_HRC_secondPhase = inotify.adapters.InotifyTree(self.confPROBA_HRC.get(OUT_DIR))

        Thread(target=self.cyclingOnDir, args=(i_cosmo, "COSMO", True, True,)).start()
        Thread(target=self.cyclingOnDir, args=(i_proba_HRC_firstPhase, "PROBA_HRC", False, False,)).start()
        Thread(target=self.cyclingOnDir, args=(i_proba_HRC_secondPhase, "PROBA_HRC", True, True,)).start()
        Thread(target=self.cyclingOnDir, args=(i_proba_CHRIS_firstPhase, "PROBA_CHRIS", False, False,)).start()
        Thread(target=self.cyclingOnDir, args=(i_proba_CHRIS_secondPhase, "PROBA_CHRIS", True, True,)).start()
        tle = TLE("https://celestrak.com/NORAD/elements/engineering.txt", "sat26958.txt", 'ProbaTLE_sat26958.txt',
                  " 26958U ", 900)
        Thread(target=tle.polling).start()
        Thread(target=DBWatcher(os.path.join(self.generalDBPATH, "ProbaDB.sqlite"), self.reportDirConverter).runCheck,
               args=00)


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
