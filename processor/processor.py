import mariadb
import os
import msgpack

from confluent_kafka import Consumer

# connection parameter
record_params= {
    "user" : os.environ["MARIADB_USER"],
    "password" : os.environ["MARIADB_PASSWORD"],
    "host" : os.environ["MARIADB_HOST"],
    "database" : os.environ["MARIADB_DATABASE"],
}

base_params= {
    "user" : os.environ["MARIADB_USER"],
    "password" : os.environ["MARIADB_PASSWORD"],
    "host" : os.environ["BASE_MARIADB_HOST"],
}

target_params= {
    "user" : os.environ["MARIADB_USER"],
    "password" : os.environ["MARIADB_PASSWORD"],
    "host" : os.environ["TARGET_MARIADB_HOST"],
}

# Establish a connection
base = mariadb.connect(**base_params)
target = mariadb.connect(**target_params)

# Record Tables

record_init = [
        'create table if not exists test (id int unsigned not null auto_increment primary key, testname varchar(100), start DATETIME, stop DATETIME, t_u unique key(testname))',
# information_schema.GLOBAL_VARIABLES 
        "create table if not exists globalvars (test_id int unsigned not null, server enum('base', 'target') not null, varname varchar(64) charset latin1 not null, startvalue varchar(2048), stopvalue varchar(2048), primary key(test_id, server, varname))",
# information_schema.GLOBAL_STATUS
        "create table if not exists globalstatus (test_id int unsigned not null, server enum('base', 'target') not null, varname varchar(64) charset latin1 not null, startvalue varchar(2048), stopvalue varchar(2048), primary key(test_id, server, varname))",
        ]

# Init recording tables if not there
def init():
    with mariadb.connect(**record_params) as recording:
        with r as recording.cursor() as r:
            for i in record_init:
                r.execute(i)


def process_events(recording, base, target):
    with recording.cursor() as r, base.cursor() as b, target.cursor() as t :
        consumer.subscribe(['scary_test', 'scary_queries'])
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))
             elif msg.error():
                raise KafkaException(msg.error())
        else:
            if msg.topic() == 'scary_test':
                v = msgpack.unpackb((msg.value())
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
                r.execute("INSERT INTO my_table VALUES (?, ?)", (msg.topic(), msg.value()))
                conn.commit()

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


