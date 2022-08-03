import os
import traceback
from datetime import timedelta

import numpy as np
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster, QueryOptions
from couchbase.exceptions import CouchbaseException
from couchbase.management.collections import CollectionManager
from couchbase.options import ClusterOptions, ClusterTimeoutOptions

import pandas as pd

endpoint = "cb.bwnakm3x9ibhzylh.cloud.couchbase.com"
username = "user"
password = "User12345!"
bucket_name = "travel-sample"


def connect():
    auth = PasswordAuthenticator(username, password)

    timeout_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=10))

    cluster = Cluster('couchbases://{}'.format(endpoint),
                      ClusterOptions(auth, timeout_options=timeout_opts))

    cluster.wait_until_ready(timedelta(seconds=5))

    return cluster


def main():
    cluster = connect()
    manager = CollectionManager(
        connection=cluster.connection,
        bucket_name=bucket_name
    )

    scopes = manager.get_all_scopes()

    for scope in scopes:
        collections = scope.collections
        for collection in collections:
            collection_name = collection.name
            scope_name = collection.scope_name
            try:
                query_result = cluster.query(
                    f"SELECT * FROM `travel-sample`.{scope_name}.{collection_name}",
                    QueryOptions(metrics=True)
                )
                data = []
                for row in query_result.rows():
                    data.append(row[collection_name])
                file_name = f"{collection_name}.csv"
                if os.path.exists(file_name):
                    df_new = pd.DataFrame(data)
                    df_old = pd.read_csv(file_name)
                    for key in df_new.keys():
                        df_new["testColumn"] = np.where(
                            df_new[key] == df_old[key],
                            "No changes",
                            df_old[key]
                        )
                    df_old.merge(df_new, on="id")
                    df_old.to_csv(file_name)
                else:
                    df = pd.DataFrame(data)
                    df["testColumn"] = "No changes"
                    df.to_csv(file_name)
            except CouchbaseException as ex:
                traceback.print_exc()


if __name__ == "__main__":
    main()
