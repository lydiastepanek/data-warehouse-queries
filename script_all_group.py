import os
import pymongo
from datetime import datetime
import pdb
import pandas as pd
from bson import json_util, ObjectId
import json
import pytz
import re


# Requires the PyMongo package.
# https://api.mongodb.com/python/current

HOST = os.getenv("MONGODBHOST")
CLIENT = pymongo.MongoClient(HOST)
DB = CLIENT["xform_cloud"]
COLLECTION = DB["cloud_last_ping_serverstatus_doc"]


def read_mongodb():
    """
    Connect to a MongoDB collection through a Mongo URI
    and collect information about the collection.
    """
    tc_info = dict()
    with open("output_all_group.csv", "w+") as f:
        cursor = COLLECTION.aggregate(
            [
                {
                    "$match": {
                        # CLOUD records do not contain sessionCatalogSize, so need to analyze ATLAS only
                        "groupType": "ATLAS",
                        "hostInfo.typeName": "REPLICA_PRIMARY",
                        "last_ping_date": {
                            "$gte": datetime(2021, 3, 28, 0, 0, 0, tzinfo=pytz.utc)
                        },
                        "lastPingData.serverStatus.logicalSessionRecordCache.sessionCatalogSize": {
                            "$ne": None
                        },
                    }
                },
                {
                    "$addFields": {
                        "localTime": {
                            "$toDate": {
                                "$multiply": [
                                    {"$toLong": "$lastPingData.serverStatus.localTime"},
                                    1000,
                                ]
                            }
                        },
                        "region": "$lastPingData.isMaster.tags.region",
                        "hostTypeName": "$hostInfo.typeName",
                        "hostClusterId": "$hostInfo.clusterId",
                        "sessionCatalogSize": "$lastPingData.serverStatus.logicalSessionRecordCache.sessionCatalogSize",
                        "activeSessionsCount": "$lastPingData.serverStatus.logicalSessionRecordCache.activeSessionsCount",
                        "endSessionsCount": "$lastPingData.serverStatus.metrics.commands.endSessions.total_delta",
                        "residentMemory": "$lastPingData.serverStatus.mem.resident",
                    }
                },
                {
                    "$project": {
                        "groupId": 1,
                        "hostId": 1,
                        "groupType": 1,
                        "region": 1,
                        "last_ping_date": 1,
                        "localTime": 1,
                        "hostTypeName": 1,
                        "hostClusterId": 1,
                        "sessionCatalogSize": 1,
                        "activeSessionsCount": 1,
                        "endSessionsCount": 1,
                        "residentMemory": 1,
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "groupId": "$groupId",
                            "hostClusterId": "$hostClusterId",
                            "region": "$region",
                        },
                        "activeSessionsCount": {"$sum": "$activeSessionsCount"},
                    }
                },
                {
                    "$addFields": {
                        "groupIdOut": "$_id.groupId",
                        "hostClusterIdOut": "$_id.hostClusterId",
                        "regionOut": "$_id.region",
                    }
                },
                {
                    "$project": {
                        "groupIdOut": 1,
                        "hostClusterIdOut": 1,
                        "activeSessionsCount": 1,
                        "regionOut": 1,
                        "_id": 0,
                    }
                },
            ],
            cursor={},
            maxTimeMS=1000000,
        )
        df = pd.DataFrame(list(cursor))
        f.write(df.to_csv())


if __name__ == "__main__":
    read_mongodb()
