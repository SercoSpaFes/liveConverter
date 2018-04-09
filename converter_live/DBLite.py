import sqlite3
import os

class DataBaseLite:

    def __init__(self, nameDB, DB_STRING):
        self.nameDB = nameDB
        self.DB_STRING = DB_STRING
        if not os.path.isfile(nameDB) and not self.DB_STRING is None:
            self.createDB()

    def createDB(self):
        print "Creating database...";
        self.conn = sqlite3.connect(self.nameDB)
        self.conn.execute(self.DB_STRING)
        self.conn.commit()
        self.conn.close()

    def queryDB(self, query):
        self.conn = sqlite3.connect(self.nameDB)
        self.conn.text_factory = str
        cursor = self.conn.execute(query).fetchall()
        self.conn.close()
        return cursor

    def insertEntryDB(self, insert):
        self.conn = sqlite3.connect(self.nameDB)
        print "Insert on DB..."
        self.conn.execute(insert)
        self.conn.commit()
        self.conn.close()

    def updateEntryDB(self, updateCommand):
        self.conn = sqlite3.connect(self.nameDB)
        print "Update on DB..."
        self.conn.execute(updateCommand)
        self.conn.commit()

    def alterTableDB(self, alterCommand):
        self.conn = sqlite3.connect(self.nameDB)
        print "Alter Table on going..."
        self.conn.execute(alterCommand)
        self.conn.commit()
        print "Alter Table committed"

    def deleteEntryDB(self, deleteCommand):
        self.conn = sqlite3.connect(self.nameDB)
        print "Delete entry on DB..."
        self.conn.execute(deleteCommand)
        self.conn.commit()


if __name__ == '__main__':

    try:
        db = DataBaseLite("/home/gilles/shared/dev/FES/giulio_live_converter/test_dbs/ProbaDB_new.sqlite")
        print "all records:"
        for i in db.queryDB("SELECT * FROM PROBA_PRODUCTS"):
            print i
            
        print "complete records:"
        for i in db.queryDB("SELECT * FROM PROBA_PRODUCTS WHERE complete=1"):
            print i

        print "not complete records:"
        for i in db.queryDB("SELECT * FROM PROBA_PRODUCTS WHERE complete=0"):
            print i


    except sqlite3.IntegrityError as e:
        print "prodotto presente"

