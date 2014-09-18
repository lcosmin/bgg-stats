import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger()

from apiclient.http import MediaFileUpload
from apiclient.discovery import build

from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage

import httplib2
import time
import subprocess
import argparse
import sys


def create_oauth2_flow(secrets_json):
    return flow_from_clientsecrets(filename=secrets_json,
                                   scope="https://www.googleapis.com/auth/drive",
                                   redirect_uri="urn:ietf:wg:oauth:2.0:oob")


def authorize(flow):
    url = flow.step1_get_authorize_url()

    if sys.platform == "darwin":
        subprocess.check_call(["open", url])
    else:
        subprocess.check_call(["xdg-open", url])


def confirm(flow, code, storage):

    credentials = flow.step2_exchange(code)
    storage.put(credentials)
    return credentials


def get_credentials(flow, storage):

    credentials = storage.get()

    if not credentials or credentials.invalid:
        authorize(flow)
        code = raw_input("enter OAUTH2 verification code: ").strip()
        credentials = confirm(flow, code, storage)

    return credentials


def main():
    p = argparse.ArgumentParser()

    g = p.add_argument_group("OAUTH2 Functions")
    g.add_argument("--authorize", help="Authorizes the application", action="store_true")
    g.add_argument("--confirm", help="Confirm authorization code")
    g.add_argument("--secrets", help="Path to secrets JSON file", default="secrets.json")

    p.add_argument("--storage", help="Path to storage file", default="credentials.storage")

    g = p.add_argument_group("Google Drive")
    g.add_argument("--list", help="List files", action="store_true")
    g.add_argument("--upload", help="Upload file")

    args = p.parse_args()

    flow = create_oauth2_flow(args.secrets)
    storage = Storage(args.storage)

    if args.authorize:
        authorize(flow)

    if args.confirm:
        confirm(flow, args.confirm, storage)

    credentials = get_credentials(flow, storage)

    http = credentials.authorize(httplib2.Http())

    log.info("dir: {}".format(dir(http)))

    r = build("drive", "v2", http=http)

    if args.list:
        res = r.files().list().execute()
        log.info("result: {}".format(res))

    if args.upload:
        mf = MediaFileUpload(filename=args.upload,
                             mimetype="application/octet-stream",
                             resumable=True)

        body = {
            'title': 'upload test.bin',
            'description': 'A test document',
            'mimeType': 'application/octet-stream'
        }

        start_time = time.time()
        file = r.files().insert(body=body, media_body=mf).execute()
        log.info("upload took: {}, upload result: {}".format(time.time() - start_time, file))


if __name__ == "__main__":
    main()
