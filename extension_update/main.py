# Does require the user of a pgpass file https://www.postgresql.org/docs/current/libpq-pgpass.html

from pg import DB, Query
import sys
import logging
import optparse
import pgpasslib
import time

start_time = time.time()



parser = optparse.OptionParser()
parser.add_option("-d", "--database", dest = "database", action = "store", help = "Specify target database to connect to")
parser.add_option("--host", dest = "host", action = "store", default = 'localhost', help = "Specify the target host")
parser.add_option("--username", dest = "username", action = "store", default = 'postgres', help = "Specify the username for PostgreSQL host")
parser.add_option("--single-database", dest = "singledb", action = "store", default=False, help = "Analyze only the connection database")
parser.add_option("--exclude-databases", dest = "excluded_dbs", action = "store_true", default = ["rdsadmin", "template0", "template1"], help = "Specify any databases to exclude in list format: [\"db1\", \"db2\"]")
parser.add_option("--loglevel", dest = "loglevel", action = "store", default = "INFO", help = "Log level to output.")



options, args = parser.parse_args()

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',level=options.loglevel.upper())

# Getting command line options
if options.database:
    vDatabase = options.database
else:
    logging.error("database not supplied... exiting...")
    sys.exit()

# Get the password from .pgpass
password = pgpasslib.getpass(options.host, 5432, vDatabase, options.username)
if not password:
    raise ValueError('Did not find a password in the .pgpass file')

if options.singledb:
    con = DB(dbname='postgres', host=options.host, user=options.username, passwd=password)
    if vDatabase in con.get_databases():
        vAllDatabases = [vDatabase]
        pass
    else:
        logging.error("Database doesn't exists... exiting")
        sys.exit()
    con.close()
else:
    con = DB(dbname=vDatabase, host=options.host, user=options.username, passwd=password)
    vAllDatabases = con.get_databases()
    con.close()
    
vHost = options.host
vUser = options.username
vPass = password
vSingleDB = options.singledb
vExcludedDBs = options.excluded_dbs

# Run on selected database
def database_run(database):
    upgrade_extensions(database)

# Function to upgrade all extensions within database
def upgrade_extensions(database):
    db = DB(dbname = database, host = vHost, user = vUser, passwd=vPass)
    for ext in get_extensions(database):
        logging.info("Extension {} is currently at version {}".format(ext['extname'], ext['extversion']))
        if "-" in ext['extname']:
            command = "ALTER EXTENSION \"{}\" UPDATE;".format(ext['extname'])
        else:
            command = "ALTER EXTENSION {} UPDATE;".format(ext['extname'])
        result = db.query(db.escape_string(command))
        logging.debug("Query result: {}".format(result))
    for newext in get_extensions(database):
        logging.info("Extension {} has been updated to version {}".format(newext['extname'], newext['extversion']))
    db.close()

# Get all extensions from database connection
def get_extensions(database):
    db = DB(dbname = database, host = vHost, user = vUser, passwd=vPass)
    ext_list = db.query('select extname, extversion from pg_extension')
    db.close()

    return Query.dictresult(ext_list)


if __name__ == '__main__':
    
    if not vSingleDB:
        for badDB in vExcludedDBs:
            while(badDB in vAllDatabases):
                vAllDatabases.remove(badDB)

    logging.info("Found the following databases: {}".format(vAllDatabases))
    
    
    total_dbs = len(vAllDatabases)
    logging.info("Running Extension Updates on {} databases".format(str(total_dbs)))
    done_dbs = 0
    for database in vAllDatabases:
        logging.info("Starting database {}".format(database))
        database_run(database)
        done_dbs += 1
        logging.info("Finished with {} databases out of {}".format(str(done_dbs), str(total_dbs)))

    print("--- %s seconds ---" % (time.time() - start_time))
