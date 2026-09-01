"""
Python 3 API wrapper for Garmin Connect to get your statistics.
Copy most code from https://github.com/cyberjunky/python-garminconnect
"""

import argparse
import asyncio
import os
from pathlib import Path


from config import FIT_FOLDER, GPX_FOLDER, JSON_FILE, SQL_FILE
from garmin_connector import (
    GarminActivityScope,
    GarminConnector,
    GarminDomain,
    GarminFileType,
)
from garmin_sync import get_downloaded_ids
from garmin_sync import download_new_activities
from utils import make_activities_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "cn_token_store",
        help="path to the Garmin China token file",
    )
    parser.add_argument(
        "global_token_store",
        help="path to the Garmin Global token file",
    )
    parser.add_argument(
        "--only-run",
        dest="only_run",
        action="store_true",
        help="if is only for running",
    )

    options = parser.parse_args()
    cn_token_store = Path(options.cn_token_store)
    global_token_store = Path(options.global_token_store)
    activity_scope = (
        GarminActivityScope.RUNNING if options.only_run else GarminActivityScope.ALL
    )

    # Step 1:
    # Sync all activities from Garmin CN to Garmin Global in FIT format
    # If the activity is manually imported with a GPX, the GPX file will be synced

    # load synced activity list
    downloaded_fit = get_downloaded_ids(FIT_FOLDER)
    downloaded_gpx = get_downloaded_ids(GPX_FOLDER)
    downloaded_activity = list(set(downloaded_fit + downloaded_gpx))

    folder = FIT_FOLDER
    # make gpx or tcx dir
    if not os.path.exists(folder):
        os.mkdir(folder)

    new_ids, id2title = asyncio.run(
        download_new_activities(
            cn_token_store,
            GarminDomain.CHINA,
            downloaded_activity,
            activity_scope,
            GarminFileType.FIT,
        )
    )

    to_upload_files = []
    for i in new_ids:
        if os.path.exists(os.path.join(FIT_FOLDER, f"{i}.fit")):
            # upload fit files
            to_upload_files.append(os.path.join(FIT_FOLDER, f"{i}.fit"))
        elif os.path.exists(os.path.join(GPX_FOLDER, f"{i}.gpx")):
            # upload gpx files which are manually uploaded to garmin connect
            to_upload_files.append(os.path.join(GPX_FOLDER, f"{i}.gpx"))

    print("Files to sync:" + " ".join(to_upload_files))
    # FIXME is com ok here?
    garmin_global_client = GarminConnector(
        global_token_store,
        GarminDomain.GLOBAL,
        activity_scope,
    )
    asyncio.run(garmin_global_client.upload_activities_files(to_upload_files))

    # Step 2:
    # Generate track from fit/gpx file
    make_activities_file(
        SQL_FILE, GPX_FOLDER, JSON_FILE, file_suffix="gpx", activity_title_dict=id2title
    )
    make_activities_file(
        SQL_FILE, FIT_FOLDER, JSON_FILE, file_suffix="fit", activity_title_dict=id2title
    )
