import os
import pymongo
from datetime import datetime
import pdb
import pandas as pd
from bson import json_util, ObjectId
import json
import pytz


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
    with open("group_by_output.csv", "w+") as f:
        cursor = COLLECTION.aggregate(
            [
                {
                    "$match": {
                        "hostInfo.typeName": "REPLICA_PRIMARY",
                        "last_ping_date": {
                            "$gte": datetime(2021, 4, 3, 0, 0, 0, tzinfo=pytz.utc)
                        },
                    }
                },
                {
                    "$addFields": {
                        "hostTypeName": "$hostInfo.typeName",
                        "hostClusterId": "$hostInfo.clusterId",
                        "sessionCatalogSize": "$lastPingData.serverStatus.logicalSessionRecordCache.sessionCatalogSize",
                        "activeSessionsCount": "$lastPingData.serverStatus.logicalSessionRecordCache.activeSessionsCount",
                        "lastSessionsCollectionJobEntriesRefreshed": "$lastPingData.serverStatus.logicalSessionRecordCache.lastSessionsCollectionJobEntriesRefreshed",
                        "lastSessionsCollectionJobEntriesEnded": "$lastPingData.serverStatus.logicalSessionRecordCache.lastSessionsCollectionJobEntriesEnded",
                        "lastSessionsCollectionJobCursorsClosed": "$lastPingData.serverStatus.logicalSessionRecordCache.lastSessionsCollectionJobCursorsClosed",
                        "startSessionCount": "$lastPingData.serverStatus.metrics.commands.startSession.total_delta",
                        "refreshSessionsCount": "$lastPingData.serverStatus.metrics.commands.refreshSessions.total_delta",
                        "endSessionsCount": "$lastPingData.serverStatus.metrics.commands.endSessions.total_delta",
                        "killSessionsCount": "$lastPingData.serverStatus.metrics.commands.killSessions.total_delta",
                        "killAllSessionsCount": "$lastPingData.serverStatus.metrics.commands.killAllSessions.total_delta",
                    }
                },
                {
                    "$project": {
                        "last_ping_time": 1,
                        "groupId": 1,
                        "hostId": 1,
                        "hostTypeName": 1,
                        "hostClusterId": 1,
                        "sessionCatalogSize": 1,
                        "activeSessionsCount": 1, 
                        "lastSessionsCollectionJobEntriesRefreshed": 1, 
                        "lastSessionsCollectionJobEntriesEnded": 1, 
                        "lastSessionsCollectionJobCursorsClosed": 1, 
                        "startSessionCount": 1,
                        "refreshSessionsCount": 1,
                        "endSessionsCount": 1,
                        "killSessionsCount": 1,
                        "killAllSessionsCount": 1,
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "groupId": "$groupId",
                            "hostClusterId": "$hostClusterId",
                        },
                        "sessionCatalogSizeSum": {"$sum": "$sessionCatalogSize"},
                        "activeSessionsCountSum": {"$sum": "$activeSessionsCount"},
                        "lastSessionsCollectionJobEntriesRefreshedSum": {"$sum": "$lastSessionsCollectionJobEntriesRefreshed"},
                        "lastSessionsCollectionJobEntriesEndedSum": {"$sum": "$lastSessionsCollectionJobEntriesEnded"},
                        "lastSessionsCollectionJobCursorsClosedSum": {"$sum": "$lastSessionsCollectionJobCursorsClosed"},
                        "startSessionCountSum": {"$sum": "$startSessionCount"},
                        "refreshSessionsCountSum": {"$sum": "$refreshSessionsCount"},
                        "endSessionsCountSum": {"$sum": "$endSessionsCount"},
                        "killSessionsCountSum": {"$sum": "$killSessionsCount"},
                        "killAllSessionsCountSum": {"$sum": "$killAllSessionsCount"},
                    }
                },
                {
                    "$addFields": {
                        "groupIdOut": "$_id.groupId",
                        "hostClusterIdOut": "$_id.hostClusterId",
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "groupIdOut": 1,
                        "hostClusterIdOut": 1,
                        "sessionCatalogSizeSum": 1,
                        "activeSessionsCountSum": 1, 
                        "lastSessionsCollectionJobEntriesRefreshedSum": 1, 
                        "lastSessionsCollectionJobEntriesEndedSum": 1, 
                        "lastSessionsCollectionJobCursorsClosedSum": 1, 
                        "startSessionCountSum": 1,
                        "refreshSessionsCountSum": 1,
                        "endSessionsCountSum": 1,
                        "killSessionsCountSum": 1,
                        "killAllSessionsCountSum": 1,
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
