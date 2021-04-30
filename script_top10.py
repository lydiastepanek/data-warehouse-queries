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
                        "lastPingData.serverStatus.logicalSessionRecordCache.sessionCatalogSize": {
                            "$ne": None
                        },
                        "groupId": {
                            "$in": [
                                "59d3bb384e65815603770555",
                                "5de53858553855d6eb3f9c8c",
                                "5de53858553855d6eb3f9c8c",
                                "57fef6a03b34b9247381e266",
                                "5bbf9328ff7a259dccf23ca6",
                                "5f5ba0ca92cad45747fecaf4",
                                "5d739a4ecf09a28b6f69b570",
                                "5e19a6a2cf09a25b90180503",
                                "5eae6931e312e0471bdd6f7f",
                                "5eadd9920b8b5a2d311ace7c",
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
