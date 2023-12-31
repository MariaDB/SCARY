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

# https://stackoverflow.com/questions/2805231/how-can-i-do-dns-lookups-in-python-including-referring-to-etc-hosts/66000439#66000439
import socket

def get_ip_by_hostname(hostname):
    # see `man getent` `/ hosts `
    # see `man getaddrinfo`

    return list(
        i        # raw socket structure
            [4]  # internet protocol info
            [0]  # address
        for i in
        socket.getaddrinfo(
            hostname,
            0  # port, required
        )
        if i[0] in ( socket.AddressFamily.AF_INET, socket.AddressFamily.AF_INET6)

        # ignore duplicate addresses with other socket types
        and i[1] is socket.SocketKind.SOCK_RAW
    )

def start_end_event(test_name, start_end):
    test = dict()
    test['testname'] = test_name
    test['startstop'] = start_end
    producer.produce('scary_test', msgpack.packb(test))


PROCESS_LIST_QUERY = """
    SELECT QUERY_ID, DB, INFO
    FROM INFORMATION_SCHEMA.PROCESSLIST
    WHERE COMMAND='Query'
        AND ID != CONNECTION_ID()
        AND STATE = 'Sending data'
"""
# TODO 'Sending data' isn't always a SELECT query - e.g. INSERT .. RETURNING

if __name__ == '__main__':

    print("initializing")
    # Establish a connection
    connection = mariadb.connect(**conn_params)

    cursor = connection.cursor()

    producer = Producer(**kafka_params)

    test_name = os.environ.get('TEST')
    if test_name is None:
        cursor.execute('SELECT CONCAT(@@hostname, DATE_FORMAT(now(), "_%Y%m%d_%H:%i:%s"))')
        (test_name,) = cursor.fetchone()
        cursor.nextset()

    start_end_event(test_name, 'start')

    if os.environ.get('PROCESSOR'):
        # Exclude the processor from the agent's collection
        p = os.environ.get('PROCESSOR')
        p_ip = get_ip_by_hostname(p)
        if len(p_ip) > 0:
            print('ignoring PROCESSOR ip ' + p_ip[0])
            PROCESS_LIST_QUERY = PROCESS_LIST_QUERY + " AND NOT HOST LIKE '{}:%'".format(p_ip[0])

    query_stream = 'scary_queries'

    killer = GracefulKiller()

    # We maintain a list of query_ids that we have sent
    # this list is maintained in order and we assume the I_S.PROCESSLIST returns query_id in order
    sentlist = []

    print("beginning collection")
    while not killer.kill_now:
        cursor.execute(PROCESS_LIST_QUERY)
        nextlist = []
        q = dict()
        q['testname'] =  test_name
        sent = 0
        skipped = 0
        for (query_id, db, info) in cursor:
            if query_id not in sentlist and info is not None:
                q['db'] = db
                q['info'] = info
                print('send ' + str(query_id) + " db: " + db + " q: " + info[:30])
                producer.produce(query_stream, msgpack.packb(q))
                sent = sent + 1
            else:
                skipped = skipped + 1
            nextlist.append(query_id)
        sentlist = nextlist.copy()
        print('number sent ' + str(sent) + " skipped " + str(skipped))
        if sent == 0:
            # just a little backoff
            time.sleep(3)

    # Shutdown
    print("shutting down")
    producer.flush()
    start_end_event(test_name, 'stop')
    producer.flush()
    print("terminating")
