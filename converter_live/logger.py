import logging

fileLogger = logging.getLogger()
fileLogger.setLevel(logging.DEBUG)
consoleLogger = logging.getLogger()
consoleLogger.setLevel(logging.DEBUG)

file_logging_level_switch = {
    'debug':    fileLogger.debug,
    'info':     fileLogger.info,
    'warning':  fileLogger.warning,
    'error':    fileLogger.error,
    'critical': fileLogger.critical
}

console_logging_level_switch = {
    'debug':    consoleLogger.debug,
    'info':     consoleLogger.info,
    'warning':  consoleLogger.warning,
    'error':    consoleLogger.error,
    'critical': consoleLogger.critical
}

def LoggingInit( logPath, logFile, html=True ):

    global fileLogger
    global consoleLogger

    logFormatStr = "[%(asctime)s %(threadName)s, %(levelname)s] %(message)s"
    consoleFormatStr = "[%(threadName)s, %(levelname)s] %(message)s"

    if html:
        logFormatStr = "<p>" + logFormatStr + "</p>"

    # File Handler for log file
    logFormatter = logging.Formatter(logFormatStr)
    fileHandler = logging.FileHandler(
        "{0}{1}.html".format( logPath, logFile ))
    fileHandler.setFormatter( logFormatter )
    fileLogger.addHandler( fileHandler )

    # Stream Handler for stdout, stderr
    consoleFormatter = logging.Formatter(consoleFormatStr)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter( consoleFormatter )
    consoleLogger.addHandler( consoleHandler )

def WriteLog( string, print_screen=True, remove_newlines=True, level='debug' ):
    string = str(string)
    if remove_newlines:
        string = string.replace('\r', '').replace('\n', ' ')

    if print_screen:
        console_logging_level_switch[level](string)
    file_logging_level_switch[level](string)
