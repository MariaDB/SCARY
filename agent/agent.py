import mariadb
import os, time
import msgpack
import datetime
import signal

from confluent_kafka import Producer

# connection parameters
# PROCESS GRANT REQUIRED.
conn_params= {
    "user" : "root",
    "password" : "",
    "host" : os.environ["MARIADB_HOST"],
    "database" : os.environ["MARIADB_DATABASE"],
}

kafka_params = {
    'bootstrap.servers': os.environ['KAFKA'],
    'batch.size' : 256,
    'linger.ms' : 4,
}

# https://stackoverflow.com/questions/18499497/how-to-process-sigterm-signal-gracefully#31464349
class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, *args):
    self.kill_now = True

def encode_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return {'__datetime__': True, 'as_str': obj.strftime("%Y%m%dT%H:%M:%S.%f")}
    return obj

def send_variables(start_end, test_name):
    test = dict()
    test['testname'] = test_name
    test['startstop'] = start_end
    test['time'] = datetime.datetime.now()
    cursor.execute('SHOW GLOBAL VARIABLES')
    test['vars'] = cursor.fetchall()
    cursor.execute('SHOW GLOBAL STATUS')
    test['status'] = cursor.fetchall()
    producer.produce('scary_test', msgpack.packb(test, default=encode_datetime))

PROCESS_LIST_QUERY = "SELECT QUERY_ID, DB, INFO FROM INFORMATION_SCHEMA.PROCESSLIST WHERE COMMAND='Query'"

if __name__ == '__main__':

    print("initializing")
    # Establish a connection
    connection = mariadb.connect(**conn_params)

    cursor = connection.cursor()

    producer = Producer(**kafka_params)

    test = dict()

    test['name'] = os.environ.get('TEST')
    if test['name'] is None:
        cursor.execute('SELECT CONCAT(@@hostname, DATE_FORMAT(now(), "_%Y%m%d_%H:%i:%s"))')
        (test['name'],) = cursor.fetchone()
        cursor.nextset()

    test_name = test['name']

    send_variables('start', test_name)

    query_stream = 'scary_queries'

    killer = GracefulKiller()

    # We maintain a list of query_ids that we have sent
    # this list is maintained in order and we assume the I_S.PROCESSLIST returns query_id in order
    sentlist = []

    print("beginning collection")
    while not killer.kill_now:
        cursor.execute(PROCESS_LIST_QUERY)
        if sentlist:
            s = iter(sentlist)
            c = next(s)
        else:
            c = None
        nextlist = []
        q = dict()
        q['testname'] =  test_name
        sent = 0
        skipped = 0
        for (query_id, db, info) in cursor:
            if info == PROCESS_LIST_QUERY:
                continue
            # we filter out the query_ids that we have done
            while c is not None and query_id < c:
                try:
                    c = next(s)
                except StopIteration:
                    c = None
            if c == None or query_id != c:
                q['db'] = db
                q['info'] = info
                producer.produce(query_stream, msgpack.packb(q))
                sent = sent + 1
            else:
                skipped = skipped + 1
            nextlist.append(query_id)
        sendlist = nextlist
        print('number sent ' + str(sent) + " skipped " + str(skipped))
        if sent == 0:
            # just a little backoff
            time.sleep(3)

    # Shutdown
    print("shutting down")
    producer.flush()
    send_variables('end', test_name)
    producer.flush()
    print("terminating")
