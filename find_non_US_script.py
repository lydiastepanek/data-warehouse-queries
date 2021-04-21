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
    with open("find_non_US_output.csv", "w+") as f:
        cursor = COLLECTION.aggregate(
            [
                {
                    "$match": {
                        # CLOUD records do not contain sessionCatalogSize, so need to analyze ATLAS only
                        "groupType": "ATLAS",
                        "hostInfo.typeName": "REPLICA_PRIMARY",
                        "last_ping_date": {
                            "$gte": datetime(2021, 4, 8, 0, 0, 0, tzinfo=pytz.utc)
                        },
                        "groupId": {
                            "$in": [
                                "5e7a4b819d14ee27b5b84d70",
                                "58adfa4996e8212cd2025f0b",
                                "5c926bf479358e640c6c6b1b",
                                "60252c70bfa25039cf7cdd23",
                                "5dcb3edec56c981be3f69c72",
                                "5d7fc0a2f2a30b8e78fde53d",
                                "5beb5e6c9ccf6474bbe64707",
                                "5ea3bbc53e180a6f78ca6968",
                                "5bdd9fb0d5ec1310875e3f8f",
                                "5cbdd1c6ff7a25138c030546",
                                "5ba2a8184e658140ba8f5920",
                                "5ce506eac56c98145d8f3d55",
                                "5c819caacf09a2468792dd88",
                                "5d17aa73d5ec13763e8567ea",
                                "5919e4fa3b34b921732e68ff",
                                "5919e4fa3b34b921732e68ff",
                                "5cbf99c5ff7a25138c0c5bce",
                                "5c5ff0ef79358e09cafefe66",
                                "5e6aaca390352b15051a03e9",
                                "5cf44fa9a6f2396cb94114a6",
                                "5c33422e79358e440f7261c0",
                                "5c33422e79358e440f7261c0",
                                "5bbc4f4b553855d637dfb02a",
                                "5d93ad6aff7a255afce1b0bc",
                                "5d30552d79358ef6db7a0cb9",
                                "5a4630314e6581158504e2e1",
                                "5a4630314e6581158504e2e1",
                                "5c49254ed5ec13632feb879b",
                                "5f04c635362d4b4db1e3b643",
                                "5c47daa5ff7a251eebefa0b8",
                                "59d1e3a44e65814dc1fe25e1",
                                "5a7108b54e65812c92030b3c",
                                "5b441d7cc0c6e333f4486ee9",
                                "5c5af4b4f2a30bfa26bb1c35",
                                "5bebe521cf09a23ce96d87cb",
                                "5b231315c0c6e31276a38044",
                                "5c988760ff7a256b9e18d096",
                                "58ea40e0df9db16ab7584712",
                                "5d8e9d29c56c983a10706117",
                                "5a10306a3b34b93d368f702a",
                                "5f747c864b23f35127d19c9d",
                                "57a6611fdf9db140b7968365",
                                "5b9977c04e65810a92dd95ea",
                                "5e55568dc9a54313652e5f65",
                                "5e42c76aa6f2392e166f711c",
                                "5c421aac79358e19b42c3e6b",
                                "5e132708014b7611ecf9ab45",
                                "5c101c3779358e2c67a22752",
                                "5ccb87c3f2a30be8e7e34ccb",
                                "5965976896e8212020d2cba7",
                                "579b8b05e4b00ea242cad99a",
                                "5d9ae86679358e75ab154003",
                                "5c364a09d5ec13bcb5482fce",
                                "5dee34d3cf09a2bc3d16fdb3",
                                "5a148653c0c6e35052cadc91",
                            ]
                        },
                        "$and": [
                            {
                                "lastPingData.isMaster.tags.region": {
                                    "$not": re.compile(r"^US.*")
                                }
                            },
                            {
                                "lastPingData.isMaster.tags.region": {
                                    "$not": re.compile(r".*US$")
                                }
                            },
                        ],
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
                        "currentTime": {
                            "$toDate": {
                                "$multiply": [
                                    {
                                        "$toLong": "$lastPingData.hostInfo.system.currentTime"
                                    },
                                    1000,
                                ]
                            }
                        },
                        "lastSessionsCollectionJobTimestamp": {
                            "$toDate": {
                                "$multiply": [
                                    {
                                        "$toLong": "$lastPingData.serverStatus.logicalSessionRecordCache.lastSessionsCollectionJobTimestamp",
                                    },
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
                        "groupType": 1,
                        "last_ping_date": 1,
                        "localTime": 1,
                        "currentTime": 1,
                        "lastSessionsCollectionJobTimestamp": 1,
                        "region": 1,
                        "groupId": 1,
                        "hostId": 1,
                        "hostTypeName": 1,
                        "hostClusterId": 1,
                        "sessionCatalogSize": 1,
                        "activeSessionsCount": 1,
                        "endSessionsCount": 1,
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
