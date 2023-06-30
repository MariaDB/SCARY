import mariadb
from mariadb.constants import CURSOR
import os
import msgpack
import json
import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException

# connection parameter
record_params= {
    "user" : os.environ["RECORD_USER"],
    "password" : os.environ["RECORD_PASSWORD"],
    "host" : os.environ["RECORD_HOST"],
    "database" : os.environ["RECORD_DATABASE"],
    "port" : int(os.environ["RECORD_PORT"]),
}

base_params= {
    "user" : os.environ["MARIADB_USER"],
    "password" : os.environ["MARIADB_PASSWORD"],
    "host" : os.environ["MARIADB_HOST"],
}

target_params= {
    "user" : os.environ["MARIADB_USER"],
    "password" : os.environ["MARIADB_PASSWORD"],
    "host" : os.environ["TARGET_MARIADB_HOST"],
    "port" : int(os.environ["TARGET_MARIADB_PORT"]),
}

kafka_params = {
    'bootstrap.servers': os.environ['KAFKA'],
    'group.id' : 'scary'
}

# Record Tables

record_init = [
        """create table if not exists test (
            id int unsigned not null auto_increment primary key,
            testname varchar(100),
            start DATETIME COMMENT "start of of the text",
            stop DATETIME COMMENT "time test concluded",
            unique key(testname))""",
# information_schema.GLOBAL_VARIABLES 
        """create table if not exists globalvars (
            test_id int unsigned not null,
            server enum('base', 'target') not null,
            varname varchar(64) charset latin1 not null,
            startvalue varchar(2048),
            stopvalue varchar(2048),
            primary key(test_id, server, varname))""",
# information_schema.GLOBAL_STATUS
        """create table if not exists globalstatus (
            test_id int unsigned not null,
            server enum('base', 'target') not null,
            varname varchar(64) charset latin1 not null,
            startvalue varchar(2048), stopvalue varchar(2048),
            primary key(test_id, server, varname))""",
# The queries
        """create table if not exists queries (
            id int unsigned not null auto_increment,
            test_id int unsigned not null,
            db varchar(64) comment 'database name',
            info varchar(2048) comment 'query',
            digest varchar(32) charset latin1 comment 'ref: https://mariadb.com/kb/en/performance-schema-digests/',
            qtime double comment 'query time in ms',
            `explain` text,
            server enum('base', 'target'),
            other int unsigned comment 'reference from target to the base query',
            primary key(id),
            index (test_id, other))"""
        ]

WORKERS = 10

def decode_datetime(obj):
    if '__datetime__' in obj:
        obj = datetime.datetime.strptime(obj["as_str"], "%Y%m%dT%H:%M:%S.%f")
    return obj

# Init recording tables if not there
def init():
    with mariadb.connect(**record_params) as recording, recording.cursor() as r:
        for i in record_init:
            r.execute(i)

def scary_test_event(r, v):
    r.begin()
    if v['startstop'] == 'start':
        r.execute('insert into test (testname, start) values (?, ?)', (v['testname'], v['time']))
        r.execute('set @test_id = LAST_INSERT_ID()')
    elif v['startstop'] == 'stop':
        r.execute('update test set stop = ? where testname = ?', (v['time'], v['testname']))
        r.execute('set @test_id = (select id from test where testname = ?)', (v['testname'],))
    r.executemany('insert into globalvars values (@test_id, ?, ?)', v['vars'])
    r.executemany('insert into globalstats values (@test_id, ?, ?)', v['status'])
    r.commit()

def process_events(consumer, recording, base, target):
    with recording.cursor(binary=True) as r, recording.cursor(prepared=True, binary=True) as rbq, recording.cursor(prepared=True,binary=True) as rtq, base.cursor() as b, target.cursor() as t:
        consumer.subscribe(['scary_test', 'scary_queries'])
        while True:
            msg = consumer.poll()
            if msg is None:
                print("poll timeout - continuing")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
                else:
                    raise KafkaException(msg.error())
            else:
                v = msgpack.unpackb(msg.value(), object_hook=decode_datetime)
                if msg.topic() == 'scary_test':
                    scary_test_event(r, v)

                elif msg.topic() == 'scary_queries':
                    b.execute('use ' + v['db'])
                    b.execute('analyze format=json ' + v['info'])
                    baseQP = b.fetchone()[0]
                    r.execute('set @test_id = (select id from test where testname = ?)', (v['testname'],))
                    recording.begin()
                    rbq.execute("INSERT INTO queries (test_id, db, info, server, qtime, `explain`) VALUES (@test_id, ?, ?, ?, ?, ?)", (v['db'], v['info'], 'base', json.loads(baseQP)['query_block']['r_total_time_ms'], baseQP))
                    id = r.lastrowid;
                    t.execute('use ' + v['db'])
                    t.execute('analyze format=json ' + v['info'])
                    targetQP = t.fetchone()[0]
                    rtq.execute("INSERT INTO queries (test_id, db, info, server, qtime, `explain`, other) VALUES (@test_id, ?, ?, ?, ?, ?, ?)", (v['db'], v['info'], 'target', json.loads(targetQP)['query_block']['r_total_time_ms'], targetQP, id))
                    recording.commit()
                else:
                    print("unknown msg topic " + msg.topic())




def waitforstart():
    print('waiting for start')
    consumer = Consumer(**kafka_params)
    consumer.subscribe(['scary_test'])
    while True:
        msg = consumer.poll()
        if msg.error():
            if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                from confluent_kafka.admin import AdminClient, NewTopic
                admin = Consumer(**kafka_params)
                admin.create_topics([NewTopic('scary_test', 1), NewTopic('scary_queries', WORKERS)])
                continue
            else:
                raise KafkaException(msg.error())

        v = msgpack.unpackb(msg.value(), object_hook=decode_datetime)
        if msg.topic() == 'scary_test' and v['startstop'] == 'start':
            with mariadb.connect(**record_params) as recording, recording.cursor(binary=True) as r:
                scary_test_event(r, v)
            break
        else:
            print("ignoring msg topic " + msg.topic())


def work():
    print('worker begin')
    consumer = Consumer(**kafka_params)
    try:
        with mariadb.connect(**record_params) as recording, mariadb.connect(**target_params) as target, mariadb.connect(**base_params) as base:
            print("connections made, begin processing")
            process_events(consumer, recording, base, target)

    finally:
        consumer.close()


#from multiprocessing import Process

if __name__ == '__main__':
    init()
    waitforstart()
    work()
#    p = Process(target=work)
#    p.start()
#    p.join()


