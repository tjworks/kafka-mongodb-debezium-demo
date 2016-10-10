#!/usr/bin/env python
import threading, logging, time, json,pymongo


from kafka import KafkaConsumer, KafkaProducer

from pymongo import MongoClient

mongo_server="mongodb"
mongo_port=27017

kafka_server="kafka"
kafka_port=9092

# temp
#mongo_port=30670
#kafka_port=30235

print "#### Starting kafka-mongodb-debezium-demo ####"

kafka_url=kafka_server+":"+ str(kafka_port)

mongo_client = MongoClient(mongo_server, mongo_port)
db = mongo_client.inventory


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers= kafka_url,
                                 auto_offset_reset='earliest')
        consumer.subscribe(['dbserver1.inventory.customers', 
            'dbserver1.inventory.orders', 'dbserver1.inventory.products'])

        for message in consumer:
            print "\n############### RAW MSG #################\n"
            print message
            topic = message.topic.split(".")
            tname = topic.pop()

            coll = db[tname]
            cdc_coll = db["cdc_"+tname]

            doc={}
            doc["topic"]=message.topic
            doc["timestamp"]=message.timestamp
            doc["offset"] = message.offset
            doc["value"]=json.loads(message.value)
            doc["key"]=json.loads(message.key)

            result = cdc_coll.insert_one(doc)
            if result:
                print "Inserted raw: " , result.inserted_id

            if doc["value"]["payload"] and doc["value"]["payload"]["after"]:
                v = doc["value"]["payload"]["after"]
                if(v["id"]):
                    result = coll.find_one_and_replace({"id": v["id"] }, v, upsert=True)
                if(v["order_number"]):
                    result = coll.find_one_and_replace({"order_number": v["order_number"] }, v, upsert=True)
                if result: 
                    print "Updated record: ",result

            print "Before:  ", (doc["value"]["payload"]["before"])
            print "After: ",  (doc["value"]["payload"]["after"])
            print "\n\n"


def main():
    threads = [
        # Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(3600)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()



"""
{topic=u'dbserver1.inventory.customers', partition=0, offset=4, timestamp=1476063989948, timestamp_type=0, 

key='{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"}],"optional":false,"name":"dbserver1.inventory.customers.Key"},"payload":{"id":1001}}', 

value=
{
    "schema" : {
        "type" : "struct",
        "fields" : [
            {
                "type" : "struct",
                "fields" : [
                    {
                        "type" : "int32",
                        "optional" : false,
                        "field" : "id"
                    },
                    {
                        "type" : "string",
                        "optional" : false,
                        "field" : "first_name"
                    },
                    {
                        "type" : "string",
                        "optional" : false,
                        "field" : "last_name"
                    },
                    {
                        "type" : "string",
                        "optional" : false,
                        "field" : "email"
                    }
                ],
                "optional" : true,
                "name" : "dbserver1.inventory.customers.Value",
                "field" : "before"
            },
            {
                "type" : "struct",
                "fields" : [
                    {
                        "type" : "int32",
                        "optional" : false,
                        "field" : "id"
                    },
                    {
                        "type" : "string",
                        "optional" : false,
                        "field" : "first_name"
                    },
                    {
                        "type" : "string",
                        "optional" : false,
                        "field" : "last_name"
                    },
                    {
                        "type" : "string",
                        "optional" : false,
                        "field" : "email"
                    }
                ],
                "optional" : true,
                "name" : "dbserver1.inventory.customers.Value",
                "field" : "after"
            },
            {
                "type" : "struct",
                "fields" : [
                    {
                        "type" : "string",
                        "optional" : false,
                        "field" : "name"
                    },
                    {
                        "type" : "int64",
                        "optional" : false,
                        "field" : "server_id"
                    },
                    {
                        "type" : "int64",
                        "optional" : false,
                        "field" : "ts_sec"
                    },
                    {
                        "type" : "string",
                        "optional" : true,
                        "field" : "gtid"
                    },
                    {
                        "type" : "string",
                        "optional" : false,
                        "field" : "file"
                    },
                    {
                        "type" : "int64",
                        "optional" : false,
                        "field" : "pos"
                    },
                    {
                        "type" : "int32",
                        "optional" : false,
                        "field" : "row"
                    },
                    {
                        "type" : "boolean",
                        "optional" : true,
                        "field" : "snapshot"
                    }
                ],
                "optional" : false,
                "name" : "io.debezium.connector.mysql.Source",
                "field" : "source"
            },
            {
                "type" : "string",
                "optional" : false,
                "field" : "op"
            },
            {
                "type" : "int64",
                "optional" : true,
                "field" : "ts_ms"
            }
        ],
        "optional" : false,
        "name" : "dbserver1.inventory.customers.Envelope",
        "version" : 1
    },
    "payload" : {
        "before" : {
            "id" : 1001,
            "first_name" : "Sally",
            "last_name" : "Thomas",
            "email" : "sally.thomas@acme.com"
        },
        "after" : {
            "id" : 1001,
            "first_name" : "kion",
            "last_name" : "Thomas",
            "email" : "sally.thomas@acme.com"
        },
        "source" : {
            "name" : "dbserver1",
            "server_id" : 223344,
            "ts_sec" : 1476063,
            "gtid" : null,
            "file" : "mysql-bin.000003",
            "pos" : 364,
            "row" : 0,
            "snapshot" : null
        },
        "op" : "u",
        "ts_ms" : 1476063989566
    }
}
 

checksum=-646692677, serialized_key_size=168, serialized_value_size=1724

"""