import logging
from dataflows import Flow, printer, load, update_resource
from avid_covider_pipelines.utils import dump_to_path
from dataflows.base.schema_validator import ignore
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from collections import defaultdict
from googleapiclient.http import MediaIoBaseDownload


def get_client():
    """Initializes the connection to Google Drive."""
    creds = service_account.Credentials.from_service_account_file(
        os.environ["GOOGLE_SERVICE_ACCOUNT_FILE"],
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    return build('drive', 'v3', credentials=creds, cache_discovery=False)


def list_files(client, folder_id: str, extra_q=None, prefix=None):
    """Returns an iterable of tuples (id, name, version) of files in the given folder
    ID. Recurses into sub-folders."""
    page_token = None
    q = f'"{folder_id}" in parents and trashed = false'
    if extra_q is not None:
        q += ' and ' + extra_q
    while True:
        response = client.files().list(
            q=q,
            spaces='drive',
            fields='nextPageToken, files(name, id, mimeType, version)',
            pageToken=page_token).execute()
        for file in response.get('files', []):
            if prefix:
                file['name'] = os.path.join(prefix, file['name'])
            if file.get('mimeType') == 'application/vnd.google-apps.folder':
                yield from list_files(file['id'], extra_q=extra_q, prefix=file['name'])
            else:
                yield file['id'], file['name'], file['version']
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break


def get_file(client, file_id: str, out_path: str):
    """Downloads a general data file from Drive."""
    request = client.files().get_media(fileId=file_id)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            # print("Download %d%%." % int(status.progress() * 100))


def flow(parameters, *_):
    files_dump_to_path = parameters['files_dump_to_path']
    data_dump_to_path = parameters.get('data_dump_to_path')

    def _download_gdrive_data():
        stats = defaultdict(int)
        file_sources = parameters['file_sources']
        folder_id = parameters['google_drive_csv_folder_id']
        files_dir = os.path.join(files_dump_to_path, "files")
        os.makedirs(files_dir, exist_ok=True)
        client = get_client()
        existing_files = {}
        if os.path.exists(os.path.join(files_dump_to_path, "datapackage.json")):
            for row in Flow(load(os.path.join(files_dump_to_path, "datapackage.json"))).results()[0][0]:
                existing_files[row["name"]] = row
        for id, name, version in list_files(client, folder_id):
            source = file_sources.get(name)
            if source:
                assert name.endswith(".csv"), "only csv file sources are supported"
                stats['relevant_source_files'] += 1
                row = {"id": id, "name": name, "version": version, "source": source, "resource_name": "%s__%s" % (source, stats['relevant_source_files'])}
                yield row
                if (
                        os.path.exists(os.path.join(files_dump_to_path, "files", name))
                        and name in existing_files and existing_files[name]["id"] == id and existing_files[name]["version"] == version
                ):
                    logging.info("existing file, will not redownload: %s" % name)
                else:
                    logging.info("downloading file: %s" % name)
                    get_file(client, id, os.path.join(files_dump_to_path, "files", name))
        if stats['relevant_source_files'] != len(file_sources):
            raise Exception("source files mismatch")

    files_flow = Flow(
        _download_gdrive_data(),
        update_resource(-1, name="gdrive_data_files", path="gdrive_data_files.csv", **{"dpp:streaming": True}),
        dump_to_path(files_dump_to_path),
        printer()
    )
    data_flow_args = []
    for file_row in files_flow.results()[0][0]:
        data_flow_args += [
            load(os.path.join(files_dump_to_path, "files", file_row["name"]),
                 strip=False, infer_strategy=load.INFER_STRINGS, deduplicate_headers=True,
                 cast_strategy=load.CAST_TO_STRINGS, on_error=ignore, limit_rows=parameters.get("limit_rows"),
                 encoding="utf-8"),
            update_resource(-1, name=file_row["resource_name"], path=file_row["name"], **{"dpp:streaming": True})
        ]
    if data_dump_to_path:
        data_flow_args += [
            dump_to_path(data_dump_to_path)
        ]
    return Flow(*data_flow_args)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    flow({
        "limit_rows": 200,
        "files_dump_to_path": "data/download_gdrive_files",
        "data_dump_to_path": "data/download_gdrive_files/data",
        # COVID19 WS > Data > Analysis Raw Data
        "google_drive_csv_folder_id": "1pzAyk-uXy__bt1tCX4rpTiPZNmrehTOz",
        "file_sources": {
            "COVID-19-English.csv": "google",
            "COVID-19-Russian.csv": "google",
            "COVID-19-Hebrew.csv": "hebrew_google",
            "maccabi-2020-04-22.csv": "maccabi",
        }
    }).process()
