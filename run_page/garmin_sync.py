"""Download Garmin Connect activities and build running-page data files."""

import argparse
import asyncio
import datetime as dt
import time
import zipfile
from collections.abc import Iterable, Mapping
from pathlib import Path

from lxml import etree

import aiofiles
from config import FOLDER_DICT, JSON_FILE, SQL_FILE
from garmin_connector import (
    GarminActivitySummary,
    GarminActivityScope,
    GarminConnector,
    GarminDomain,
    GarminFileType,
)
from utils import make_activities_file

type GarminSummaryInfo = dict[str, object]
type GarminSummaryIndex = dict[str, GarminSummaryInfo]

SUMMARY_FIELDS: tuple[str, ...] = (
    "distance",
    "average_hr",
    "average_speed",
    "start_time",
    "end_time",
    "moving_time",
    "elapsed_time",
)


class GarminActivityDataError(RuntimeError):
    """Raised when Garmin activity data cannot be transformed safely."""


def get_info_text_value(
    summary_infos: Mapping[str, object],
    key_name: str,
) -> str:
    value = summary_infos.get(key_name)
    if value is None:
        return ""
    return str(value)


def create_element(
    parent: etree._Element,
    tag: str,
    text: str,
) -> etree._Element:
    elem = etree.SubElement(parent, tag)
    elem.text = text
    elem.tail = "\n"
    return elem


def add_summary_info(
    file_data: bytes,
    summary_infos: GarminSummaryInfo | None,
    fields: tuple[str, ...],
) -> bytes:
    if summary_infos is None:
        return file_data
    try:
        root = etree.fromstring(file_data)
    except etree.XMLSyntaxError as error:
        raise GarminActivityDataError(
            f"Downloaded GPX is not valid XML: error={error}"
        ) from error

    extensions_node = etree.Element("extensions")
    extensions_node.text = "\n"
    extensions_node.tail = "\n"
    for field in fields:
        create_element(
            extensions_node,
            field,
            get_info_text_value(summary_infos, field),
        )
    root.insert(0, extensions_node)
    return etree.tostring(root, encoding="utf-8", pretty_print=True)


async def download_garmin_data(
    client: GarminConnector,
    activity_id: str,
    file_type: GarminFileType,
    summary_infos: GarminSummaryIndex,
) -> None:
    folder = Path(FOLDER_DICT.get(file_type.value, "gpx"))
    file_data = await client.download_activity(activity_id, file_type)
    if file_type is GarminFileType.GPX:
        file_data = add_summary_info(
            file_data,
            summary_infos.get(activity_id),
            SUMMARY_FIELDS,
        )

    file_path = folder / f"{activity_id}.{file_type.value}"
    if file_type is GarminFileType.FIT:
        file_path = folder / f"{activity_id}.zip"

    async with aiofiles.open(file_path, "wb") as activity_file:
        await activity_file.write(file_data)

    if file_type is not GarminFileType.FIT:
        return

    extracted_file_count = 0
    with zipfile.ZipFile(file_path, "r") as zip_file:
        for file_info in zip_file.infolist():
            if file_info.filename.endswith(".fit"):
                destination = folder / f"{activity_id}.fit"
            elif file_info.filename.endswith(".gpx"):
                destination = Path(FOLDER_DICT["gpx"]) / f"{activity_id}.gpx"
            else:
                continue
            destination.parent.mkdir(exist_ok=True)
            destination.write_bytes(zip_file.read(file_info))
            extracted_file_count += 1
    if extracted_file_count == 0:
        raise GarminActivityDataError(
            f"Garmin original activity archive contains no FIT or GPX file: "
            f"activity_id={activity_id}"
        )
    file_path.unlink()


async def get_activity_id_list(
    client: GarminConnector,
    start: int,
) -> list[str]:
    activities = await client.get_activities(start, 100)
    if len(activities) > 0:
        ids = [str(activity.get("activityId", "")) for activity in activities]
        if any(not activity_id for activity_id in ids):
            raise GarminActivityDataError(
                f"Garmin returned an activity without an activityId: start={start}"
            )
        print("Syncing Activity IDs")
        return ids + await get_activity_id_list(client, start + 100)
    return []


def get_downloaded_ids(folder: str | Path) -> list[str]:
    return [
        file.name.split(".")[0]
        for file in Path(folder).iterdir()
        if not file.name.startswith(".")
    ]


def get_garmin_summary_infos(
    activity_summary: GarminActivitySummary,
    activity_id: str,
) -> GarminSummaryInfo:
    summary_value = activity_summary.get("summaryDTO")
    if not isinstance(summary_value, dict) or not all(
        isinstance(key, str) for key in summary_value
    ):
        raise GarminActivityDataError(
            f"Garmin activity summary is missing summaryDTO: activity_id={activity_id}"
        )
    summary_dto = summary_value

    start_time_value = summary_dto.get("startTimeGMT")
    duration_value = summary_dto.get("duration")
    if not isinstance(start_time_value, str):
        raise GarminActivityDataError(
            f"Garmin activity summary has an invalid startTimeGMT: "
            f"activity_id={activity_id}; value_type={type(start_time_value).__name__}"
        )
    if isinstance(duration_value, bool) or not isinstance(duration_value, (int, float)):
        raise GarminActivityDataError(
            f"Garmin activity summary has an invalid duration: "
            f"activity_id={activity_id}; value_type={type(duration_value).__name__}"
        )

    normalized_start_time = (
        f"{start_time_value[:-1]}+00:00"
        if start_time_value.endswith("Z")
        else start_time_value
    )
    try:
        start_time = dt.datetime.fromisoformat(normalized_start_time)
    except ValueError as error:
        raise GarminActivityDataError(
            f"Garmin activity summary has an invalid startTimeGMT: "
            f"activity_id={activity_id}; value={start_time_value}"
        ) from error

    end_time = start_time + dt.timedelta(seconds=duration_value)
    return {
        "distance": summary_dto.get("distance"),
        "average_hr": summary_dto.get("averageHR"),
        "average_speed": summary_dto.get("averageSpeed"),
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "moving_time": summary_dto.get("movingDuration"),
        "elapsed_time": summary_dto.get("elapsedDuration"),
    }


async def download_new_activities(
    token_store: Path,
    domain: GarminDomain,
    downloaded_ids: Iterable[str],
    activity_scope: GarminActivityScope,
    file_type: GarminFileType,
) -> tuple[list[str], dict[str, str]]:
    client = GarminConnector(token_store, domain, activity_scope)
    activity_ids = await get_activity_id_list(client, 0)
    downloaded_id_set = set(downloaded_ids)
    to_generate_garmin_ids = [
        activity_id
        for activity_id in activity_ids
        if activity_id not in downloaded_id_set
    ]
    print(f"{len(to_generate_garmin_ids)} new activities to be downloaded")

    to_generate_garmin_id2title: dict[str, str] = {}
    garmin_summary_infos_dict: GarminSummaryIndex = {}
    for activity_id in to_generate_garmin_ids:
        activity_summary = await client.get_activity_summary(activity_id)
        activity_title_value = activity_summary.get("activityName")
        activity_title = (
            activity_title_value if isinstance(activity_title_value, str) else ""
        )
        to_generate_garmin_id2title[activity_id] = activity_title
        garmin_summary_infos_dict[activity_id] = get_garmin_summary_infos(
            activity_summary,
            activity_id,
        )

    start_time = time.time()
    for activity_id in to_generate_garmin_ids:
        await download_garmin_data(
            client,
            activity_id,
            file_type,
            garmin_summary_infos_dict,
        )
    print(f"Download finished. Elapsed {time.time() - start_time} seconds")

    return to_generate_garmin_ids, to_generate_garmin_id2title


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "token_store",
        help="path to the token file generated by get_garmin_secret.py",
    )
    parser.add_argument(
        "--is-cn",
        dest="is_cn",
        action="store_true",
        help="if garmin account is cn",
    )
    parser.add_argument(
        "--only-run",
        dest="only_run",
        action="store_true",
        help="if is only for running",
    )
    parser.add_argument(
        "--tcx",
        dest="download_file_type",
        action="store_const",
        const="tcx",
        default="gpx",
        help="to download personal documents or ebook",
    )
    parser.add_argument(
        "--fit",
        dest="download_file_type",
        action="store_const",
        const="fit",
        default="gpx",
        help="to download personal documents or ebook",
    )
    options = parser.parse_args()
    token_store = Path(options.token_store)
    domain = GarminDomain.CHINA if options.is_cn else GarminDomain.GLOBAL
    file_type = GarminFileType(options.download_file_type)
    activity_scope = (
        GarminActivityScope.RUNNING if options.only_run else GarminActivityScope.ALL
    )
    folder = Path(FOLDER_DICT.get(file_type.value, "gpx"))
    folder.mkdir(exist_ok=True)
    downloaded_ids = get_downloaded_ids(folder)

    if file_type is GarminFileType.FIT:
        gpx_folder = Path(FOLDER_DICT["gpx"])
        gpx_folder.mkdir(exist_ok=True)
        downloaded_gpx_ids = get_downloaded_ids(gpx_folder)
        downloaded_ids = list(set(downloaded_ids + downloaded_gpx_ids))

    new_ids, id2title = asyncio.run(
        download_new_activities(
            token_store,
            domain,
            downloaded_ids,
            activity_scope,
            file_type,
        )
    )
    if file_type is GarminFileType.FIT:
        make_activities_file(
            SQL_FILE,
            FOLDER_DICT["gpx"],
            JSON_FILE,
            file_suffix="gpx",
            activity_title_dict=id2title,
        )
    make_activities_file(
        SQL_FILE,
        str(folder),
        JSON_FILE,
        file_suffix=file_type.value,
        activity_title_dict=id2title,
    )
