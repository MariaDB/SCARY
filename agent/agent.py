import mariadb
import os
import msgpack
from datetime import datetime


# https://stackoverflow.com/questions/18499497/how-to-process-sigterm-signal-gracefully#31464349
class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, *args):
    self.kill_now = True

# connection parameters
conn_params= {
    "user" : os.environ["MARIADB_USER"],
    "password" : os.environ["MARIADB_USER"],
    "host" : os.environ["MARIADB_HOST"],
    "database" : os.environ["MARIADB_DATABASE"],
}

# Establish a connection
connection= mariadb.connect(**conn_params)

cursor= connection.cursor()

from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': os.environ['KAFKA']})

test = dict()

test['name'] = os.environ['TEST']
if test.test is None:
    cursor.execute('SELECT CONCAT(@@hostname, DATE_FORMAT(now(), "_%Y%m%d_%H:%i:%s"))')
    (test['name'],) = cursor.fetchone()
    cursor.nextset()

test_name = test['name']

def send_variables(start_end):
    test['time'] = datetime.now()
    cursor.execute('SHOW GLOBAL VARIABLES')
    test['vars'] = cursor.fetchall()
    cursor.execute('SHOW GLOBAL STATUS')
    test['status'] = cursor.fetchall()
    producer.produce('scary_test', msgpack.packb(test), start_end)

send_variables('start')


query_stream = 'scary_queries'

killer = GracefulKiller()
while not killer.kill_now:
    cursor.execute("SELECT QUERY_ID, DB, INFO FROM INFORMATION_SCHEMA.PROCESSLIST WHERE COMMAND='Query'")
    q = dict()
    for (db, info) in cursor:
        q['db'] = db
        q['info'] = info
        producer.produce(query_stream, msgpack.packb(q), test_name)

send_variables('end')
producer.flush()
