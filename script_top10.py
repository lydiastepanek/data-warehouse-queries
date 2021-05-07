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
    with open("output_top_10.csv", "w+") as f:
        cursor = COLLECTION.aggregate(
            [
                {
                    "$match": {
                        # CLOUD records do not contain sessionCatalogSize, so need to analyze ATLAS only
                        "groupType": "ATLAS",
                        "hostInfo.typeName": "REPLICA_PRIMARY",
                        "last_ping_date": {
                            "$gte": datetime(2021, 2, 28, 0, 0, 0, tzinfo=pytz.utc)
                        },
                        "hostInfo.clusterId": {
                            "$in": [
                                "5cb5f303d5ec137cbe411bb9",
                                "57fef8d49701997f939b8112",
                                "5adde620df9db15acf42db3b",
                                "5daae1f2a6f239dc245e830b",
                                "5fd0af713d1d80700ead364b",
                                "5e17a3f55538551de8b5d6e3",
                                "5c92d9b9c56c988683fde64e",
                                "5d56d0facf09a2f486e773c6",
                                "5f5ba4cb82c27d423ede543d",
                                "5d5adb8e04e3af3d36dbb025",
                            ]
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
                        "activeSessionsCount": "$lastPingData.serverStatus.logicalSessionRecordCache.activeSessionsCount",
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
                        "activeSessionsCount": 1,
                        "residentMemory": 1,
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
