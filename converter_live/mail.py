from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
import sys
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class Mail:

    def __init__(self, senderMail, toMail, subjectMail , hostMail='localhost', bcc=None, cc=None, logFile=None, textMail=None,port=25):

        self.FROM = senderMail
        self.TO = toMail
        self.SUBJECT = subjectMail
        self.SMTP_HOST = hostMail
        if logFile is None:
            self.LOG_FILE = "mailPython.log"
        else:
            self.LOG_FILE = logFile
        self._BCC = bcc
        self.CC = cc
        self.TEXT = textMail
        self.port = port
        logging.basicConfig(filename=self.LOG_FILE, level=logging.DEBUG)

    def sendEmail(self, html=False):
        try:
            msg = MIMEMultipart()
            msg['Subject'] = self.SUBJECT
            msg['From'] = self.FROM
            msg['To'] = self.TO
            msg['Bcc'] = self._BCC
            msg['cc'] = self.CC
            _REC = "%s,%s,%s" % (msg['To'],msg['cc'],msg['Bcc'])
            part1 = MIMEText(self.TEXT)
            if html:
                part1 = MIMEText(self.TEXT, 'html')
            msg.attach(part1)
            print("will create smtp obj")
            smtpObj = smtplib.SMTP(self.SMTP_HOST, self.port)
            print("Got SMTP obj")
            smtpObj.set_debuglevel(0)
            for i in _REC.split(','):
                smtpObj.sendmail(self.FROM, i, msg.as_string())
            print("Email SENT")
        except smtplib.SMTPRecipientsRefused:
            logging.debug("Error:All recipients were refused. Nobody got the mail")
        except smtplib.SMTPHeloError:
            logging.debug("The server didn't reply properly to the HELO greeting")
        except smtplib.SMTPSenderRefused:
            logging.debug("The server didn't accept the from_addr")
        except smtplib.SMTPDataError:
            logging.debug("The server replied with an unexpected error code")
        except smtplib.SMTPException:
            logging.debug("Error Unable to send email")
        except:
            print("Error:%s" % sys.exc_info()[0])
            print("Error:%s" % (sys.exc_info(),))

        logging.info("mail sent")

if __name__ == '__main__':

    m = Mail("Dissemination Team <info@apps.eo.esa.int>", "giulio.villani@serco.com", "test", 'localhost', None, None, "/home/datamanager/mail.log","testoMail", 25)
    m.sendEmail()

