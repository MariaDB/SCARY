import mariadb
import os, time
import msgpack
from datetime import datetime

from confluent_kafka import Producer

# https://stackoverflow.com/questions/18499497/how-to-process-sigterm-signal-gracefully#31464349
class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, *args):
    self.kill_now = True

def send_variables(start_end):
    test['time'] = datetime.now()
    cursor.execute('SHOW GLOBAL VARIABLES')
    test['vars'] = cursor.fetchall()
    cursor.execute('SHOW GLOBAL STATUS')
    test['status'] = cursor.fetchall()
    producer.produce('scary_test', msgpack.packb(test), start_end)

if __name__ == '__main__':
    # connection parameters
    conn_params= {
        "user" : os.environ["MARIADB_USER"],
        "password" : os.environ["MARIADB_USER"],
        "host" : os.environ["MARIADB_HOST"],
        "database" : os.environ["MARIADB_DATABASE"],
    }
    # Establish a connection
    connection = mariadb.connect(**conn_params)

    cursor = connection.cursor()

    producer = Producer({'bootstrap.servers': os.environ['KAFKA']})

    test = dict()

    test['name'] = os.environ.get('TEST')
    if test.test is None:
        cursor.execute('SELECT CONCAT(@@hostname, DATE_FORMAT(now(), "_%Y%m%d_%H:%i:%s"))')
        (test['name'],) = cursor.fetchone()
        cursor.nextset()

    test_name = test['name']

    send_variables('start')

    query_stream = 'scary_queries'

    killer = GracefulKiller()

    # We maintain a list of query_ids that we have sent
    # this list is maintained in order and we assume the I_S.PROCESSLIST returns query_id in order
    sentlist = []
    while not killer.kill_now:
        cursor.execute("SELECT QUERY_ID, DB, INFO FROM INFORMATION_SCHEMA.PROCESSLIST WHERE COMMAND='Query'")
        s = iter(sentlist)
        nextlist = []
        c = next(s)
        q = dict()
        sent = 0
        for (query_id, db, info) in cursor:
            # we filter out the query_ids that we have done
            while c is not None and query_id < c:
                c = next(s)
            if c == None or query_id != c:
                q['db'] = db
                q['info'] = info
                producer.produce(query_stream, msgpack.packb(q), test_name)
                sent = sent + 1
            nextlist.append(query_id)
        sendlist = nextlist
        if sent == 0:
            # just a little backoff
            time.sleep(3)

    # Shutdown
    send_variables('end')
    producer.flush()
