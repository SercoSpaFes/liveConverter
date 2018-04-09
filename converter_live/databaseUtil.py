import sqlite3
import os, sys, traceback

from optparse import OptionParser


#
POSSIBLE_ACTIONS=['createDb', 'cleanDb', 'eraseRecord', 'setDoneFlag', 'unsetDoneFlag', 'listDb']

#
DEFAULT_DB_CONNECTION_STRING='/nfsdata/nfsdata02/databases/converter_live/test_ProbaDB.sqlite'

DB_CREATE_PROBA = '''CREATE TABLE PROBA_PRODUCTS
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

class DataBaseLite:

    def __init__(self, nameDB, db_String=None):
        self.conn = None
        self.name = nameDB
        if db_String is not None:
            self.connectionString = db_String
        else:
            self.connectionString = DEFAULT_DB_CONNECTION_STRING

    def createDB(self):
        print " Creating database...";
        self.conn = sqlite3.connect(self.connectionString)
        self.conn.execute(DB_CREATE_PROBA)
        self.conn.commit()
        self.conn.close()

    def queryDB(self, query):
        self.conn = sqlite3.connect(self.connectionString)
        self.conn.text_factory = str
        cursor = self.conn.execute(query).fetchall()
        self.conn.close()
        return cursor

    def insertEntryDB(self, insert):
        self.conn = sqlite3.connect(self.connectionString)
        print " Insert on DB..."
        cursor = self.conn.execute(insert)
        self.conn.commit()

    def updateEntryDB(self, updateCommand):
        self.conn = sqlite3.connect(self.connectionString)
        print " Update on DB..."
        cursor = self.conn.execute(updateCommand)
        self.conn.commit()
        return cursor


    def deleteEntryDB(self, deleteCommand):
        self.conn = sqlite3.connect(self.connectionString)
        print " Delete entry on DB..."
        cursor = self.conn.execute(deleteCommand)
        self.conn.commit()
        return cursor


def syntax():
    print "syntax: python databaseUtil -a action -k key [-s dbstring]. Possible actions are:%s" % POSSIBLE_ACTIONS

if __name__ == '__main__':
    try:


        parser = OptionParser()
        parser.usage = ("syntax: python databaseUtil -a action -k key [-s dbstring]. Possible actions are:%s" % POSSIBLE_ACTIONS)
        parser.add_option("-a", "--action", dest="action", help="type of action; valid are:%s" % POSSIBLE_ACTIONS)
        parser.add_option("-s", "--dbString", dest="dbstring", help="the db connection string")
        parser.add_option("-k", "--key", dest="key", help="the db record key")
        (options, args) = parser.parse_args()


        action=None
        key=None
        dbString=DEFAULT_DB_CONNECTION_STRING
        if options.action is None:
            print " a"
            syntax()
        action=options.action
        if options.action!=POSSIBLE_ACTIONS[0] and options.action!=POSSIBLE_ACTIONS[1] and options.action!=POSSIBLE_ACTIONS[5]:
            if options.key is None:
                print " action %s need to have a key parameter" % options.action
                syntax()
            else:
                key=options.key
                print " doing %s on record:%s using dbString:%s" % (action, key, dbString)
        else:
            print " doing %s on db using dbString:%s" % (action, dbString)

                
        if options.dbstring is not None:
            dbString = options.dbstring


        #
        db = DataBaseLite('default', dbString)

        if action==POSSIBLE_ACTIONS[0]: # createDb
            db.createDB()
            print " DB created"

            # create 3 test records
            print "\n\n START DB integrity check\n"
            db.insertEntryDB(
                '''INSERT INTO PROBA_PRODUCTS (product_name,zip,xml,jpg,bmp,insertion_date,product_type,complete) VALUES ('test_complete','1','0','0','0','1523002351','TESTING','1')''')
            db.insertEntryDB(
                '''INSERT INTO PROBA_PRODUCTS (product_name,zip,xml,jpg,bmp,insertion_date,product_type,complete) VALUES ('test_not_complete1','0','1','0','0','1523002371','TESTING','0')''')
            db.insertEntryDB(
                '''INSERT INTO PROBA_PRODUCTS (product_name,zip,xml,jpg,bmp,insertion_date,product_type,complete) VALUES ('test_not_complete2','0','0','1','0','1523002251','TESTING','0')''')

            #
            print " 3 TESTING records added:"
            lines = db.queryDB("SELECT * FROM PROBA_PRODUCTS WHERE product_type = 'TESTING'" )
            n=0
            for item in lines:
                print "  %s:%s" % (n, item)
                n+=1
            if len(lines)!=3:
                raise Exception("DB CHECK failed: don't find the 3 TESTING records but:%s" % len(lines))

            #
            print " 1 TESTING records complete queryed:"
            lines = db.queryDB("SELECT * FROM PROBA_PRODUCTS WHERE product_type = 'TESTING' and complete=1" )
            n=0
            for item in lines:
                print "  %s:%s" % (n, item)
                n+=1
            if len(lines)!=1:
                raise Exception("DB CHECK failed: don't find the 1 TESTING complete records but:%s" % len(lines))

            #
            print " 2 TESTING records not complete queryed:"
            lines = db.queryDB("SELECT * FROM PROBA_PRODUCTS WHERE product_type = 'TESTING' and complete=0" )
            n=0
            for item in lines:
                print "  %s:%s" % (n, item)
                n+=1
            if len(lines)!=2:
                raise Exception("DB CHECK failed: don't find the 2 TESTING not complete records but:%s" % len(lines))

            # delete the 3 test records
            db.deleteEntryDB("DELETE FROM PROBA_PRODUCTS WHERE product_type = 'TESTING'")

            print "\n END DB integrity check\n\n"
        
        elif action==POSSIBLE_ACTIONS[1]: #cleanDb
            db.deleteEntryDB("DELETE FROM PROBA_PRODUCTS")
        elif action==POSSIBLE_ACTIONS[2]: # eraseRecord
            db.deleteEntryDB("DELETE FROM PROBA_PRODUCTS WHERE product_name = '%s'" % key)
        elif action==POSSIBLE_ACTIONS[3]: # setDoneFlag
            db.updateEntryDB("UPDATE PROBA_PRODUCTS set complete = 'True' WHERE product_name = '%s'" % key)
        elif action==POSSIBLE_ACTIONS[4]: # unsetDoneFlag
            db.updateEntryDB("UPDATE PROBA_PRODUCTS set complete = 'False' WHERE product_name = '%s'" % key)
        elif action==POSSIBLE_ACTIONS[5]: # listDb
            lines = db.queryDB("SELECT * FROM PROBA_PRODUCTS")
            print lines
        else:
            raise Exception(" unknown action:'%s'" % action)


    except sqlite3.IntegrityError as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(" IntegrityError '%s %s'" % (exc_type, exc_obj))
        traceback.print_exc(file=sys.stdout)

    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(" Error '%s %s'" % (exc_type, exc_obj))
        traceback.print_exc(file=sys.stdout)

