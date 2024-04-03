# Does require the user of a pgpass file https://www.postgresql.org/docs/current/libpq-pgpass.html

from multiprocessing import Pool, Value
from pg import DB
import sys
import re
import logging
import optparse
import pgpasslib
import time

start_time = time.time()



parser = optparse.OptionParser()
parser.add_option("-d", "--database", dest = "database", action = "store", help = "Specify target database to connect to")
parser.add_option("-n", "--schema", dest = "schema", action = "store", help = "Specify schema to analyze")
parser.add_option("-p", "--parallel-processes", dest = "parallel", action = "store",default = 1, help = "Specify number of parallel-processes")
parser.add_option("--single-database", dest = "singledb", action = "store", default=False, help = "Analyze only the connection database")
parser.add_option("--host", dest = "host", action = "store", default = 'localhost', help = "Specify the target host")
parser.add_option("--username", dest = "username", action = "store", default = 'postgres', help = "Specify the username for PostgreSQL host")
parser.add_option("--user-tables", dest = "usertables", action = "store_true", help = "Specify if you want to analyze only user table")
parser.add_option("--exclude-databases", dest = "excluded_dbs", action = "store_true", default = ["rdsadmin", "template0", "template1"], help = "Specify any databases to exclude in list format: [\"db1\", \"db2\"]")
parser.add_option("--loglevel", dest = "loglevel", action = "store", default = "INFO", help = "Log level to output.")



options, args = parser.parse_args()

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',level=options.loglevel.upper())

# Getting command line options

if options.database:
    vDatabase = options.database
else:
    logging.error("Database not supplied... Exiting...")
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
    

vProcesses = int(options.parallel)
vHost = options.host
vSchema = options.schema
vUser = options.username
vPass = password
vSingleDB = options.singledb
vExcludedDBs = options.excluded_dbs

total_tables = 0
counter = Value('i', 0) 

# Run on selected database
def database_run(database):
    global counter
    global total_tables
    tabpool = Pool(initializer=init, initargs=(counter, ), processes=vProcesses)
    total_tables = len(get_tables(database))
    logging.info("{}: Running analyze on {} tables".format(database,str(total_tables)))
    logging.debug("{}".format(get_tables(database)))
    tabpool.map(run_analyze, get_tables(database))
    tabpool.close()  # worker processes will terminate when all work already assigned has completed.
    tabpool.join() # to wait for the worker processes to terminate.
    counter = Value('i', 0) # reset the counter

# Get list of tables
def get_tables(database):
    db = DB(dbname = database, host = vHost, user = vUser, passwd=vPass)
    table_list = []
    if options.usertables:
        table_list = db.get_tables()
    else:
        table_list = db.get_tables('system')
    db.close()

    if vSchema:
        tables = []
        regex = "^" + vSchema + "\."
        for table in table_list:
            if re.match(regex, table, re.I):
                tables.append(tables)
    else:
        tables = table_list

    Tables=[]
    for t in tables:
        Tables.append([database, t])
    return Tables

# Run analyze on single table
def run_analyze(params):
    global counter
    database = params[0]
    table = params[1]
    db = DB(dbname = database, host = vHost, user = vUser, passwd=vPass)
    logging.debug("Working on: {} on {}".format(table, vDatabase))
    
    command = 'ANALYZE {};'.format(table)
    logging.debug("Command: {}".format(command))
    db.query(db.escape_string(command))
    with counter.get_lock():
        counter.value += 1
        if counter.value % 100 == 0 and counter.value != 0:
            logging.info("Currently at {} tables analyzed...".format(counter.value))
    logging.debug("Completed table {}".format(table))
    logging.debug("{} tables completed...".format(str(counter.value)))
    db.close()

# For counter
def init(args):
    ''' store the counter for later use '''
    global counter
    counter = args


# Forking new #n processes for run_analyze() Function
if __name__ == '__main__':
    
    if not vSingleDB:
        for badDB in vExcludedDBs:
            while(badDB in vAllDatabases):
                vAllDatabases.remove(badDB)

    logging.info("Found the following databases: {}".format(vAllDatabases))
    
    total_dbs = len(vAllDatabases)
    
    logging.info("Running Analyzer on {} databases".format(str(total_dbs)))
    done_dbs = 0
    
    for database in vAllDatabases:
        logging.info("Starting database {}".format(database))
        database_run(database)
        done_dbs += 1
        logging.info("Finished with {} databases out of {}".format(str(done_dbs), str(total_dbs)))

    print("--- %s seconds ---" % (time.time() - start_time))
