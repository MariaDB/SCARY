import mariadb
import os
import msgpack

from confluent_kafka import Consumer

# connection parameter
record_params= {
    "user" : os.environ["RECORD_USER"],
    "password" : os.environ["RECORD_PASSWORD"],
    "host" : os.environ["RECORD_HOST"],
    "database" : os.environ["RECORD_DATABASE"],
    "port" : os.environ["RECORD_PORT"],
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
    "port" : os.environ["TARGET_MARIADB_PORT"],
}

# Record Tables

record_init = [
        'create table if not exists test (id int unsigned not null auto_increment primary key, testname varchar(100), start DATETIME, stop DATETIME, t_u unique key(testname))',
# information_schema.GLOBAL_VARIABLES 
        "create table if not exists globalvars (test_id int unsigned not null, server enum('base', 'target') not null, varname varchar(64) charset latin1 not null, startvalue varchar(2048), stopvalue varchar(2048), primary key(test_id, server, varname))",
# information_schema.GLOBAL_STATUS
        "create table if not exists globalstatus (test_id int unsigned not null, server enum('base', 'target') not null, varname varchar(64) charset latin1 not null, startvalue varchar(2048), stopvalue varchar(2048), primary key(test_id, server, varname))",
        "create table if not exists queries (id int unsigned not null auto_increment, test_id int unsigned not null, db varchar(64), info varchar(2048), digest varchar(32) charset latin1, qtime double, explain text, server enum('base', target'), other int unsigned, primarykey(id), key_test index(test_id, db, info, digest))",
        ]

# Init recording tables if not there
def init():
    with mariadb.connect(**record_params) as recording, recording.cursor() as r:
        for i in record_init:
            r.execute(i)

def process_events(recording, base, target):
    with recording.cursor() as r, base.cursor() as b, target.cursor() as t:
        consumer.subscribe(['scary_test', 'scary_queries'])
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            return
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            v = msgpack.unpackb(msg.value())
            if msg.topic() == 'scary_test':
                r.begin()
                if msg.key() == 'start':
                    r.execute('insert into test (testname, start) values (?, ?)', [v.testname, v.time])
                    r.execute('set @test_id = LAST_INSERT_ID()')
                elif msg.key() == 'stop':
                    r.execute('update test set stop = ? where testname = ?', v.time, v.testname)
                    r.execute('set @test_id = (select id from test where testname = ?)', v.testname)
                r.executemany('insert into globalvars values (@test_id, ?, ?)', v['vars'])
                r.executemany('insert into globalstats values (@test_id, ?, ?)', v['status'])
                r.commit()

            elif msg.topic() == 'scary_queries':
                b.execute('use ' + v.db)
                b.execute('analyze format=json ' + v.info)
                baseQP = b.fetchone()[0]
                r.execute('set @test_id = (select id from test where testname = ?)', v.testname)
                r.begin()
                r.execute("INSERT INTO queries (test_id, db, info, server, qtime, explain) VALUES (@test_id, ?, ?)", (v.db, v.info, 'base', json.loads(baseQP)['query_block']['r_total_time_ms'], baseQP))
                id = r.lastrowid;
                t.execute('use ' + v.db)
                t.execute('analyze format=json ' + v.info)
                targetQP = t.fetchone()[0]
                r.execute("INSERT INTO queries (test_id, db, info, server, qtime, explain, other) VALUES (@test_id, ?, ?)", (v.db, v.info, 'target', json.loads(targetQP)['query_block']['r_total_time_ms'], targetQP, id))
                r.commit()

def work():
    consumer = Consumer({'bootstrap.servers': os.environ['KAFKA'], 'group.id' : 'scary'})
    try:
        with mariadb.connect(**record_params) as recording, mariadb.connect(**target_params) as target, mariadb.connect(**base_params) as base:
            process_events(recording, base, target)

    finally:
        consumer.close()


from multiprocessing import Process

if __name__ == '__main__':

    p = Process(target=work)
    p.start()
    p.join()


