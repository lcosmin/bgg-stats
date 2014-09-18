import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger()

from apiclient.http import MediaFileUpload
from apiclient.discovery import build

from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage

import httplib2

import subprocess
import argparse
import sys


def create_oauth2_flow(secrets_json):
    return flow_from_clientsecrets(filename=secrets_json,
                                   scope="https://www.googleapis.com/auth/fusiontables",
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

    g = p.add_argument_group("Google Fusion Tables")
    g.add_argument("--list", help="List fusion tables", action="store_true")
    g.add_argument("--update", help="Update fusion table")
    g.add_argument("--file", help="file containing the data to update with")

    args = p.parse_args()

    flow = create_oauth2_flow(args.secrets)
    storage = Storage(args.storage)

    if args.authorize:
        authorize(flow)

    if args.confirm:
        confirm(flow, args.confirm, storage)

    credentials = get_credentials(flow, storage)

    http = credentials.authorize(httplib2.Http())

    r = build("fusiontables", "v1", http=http)

    if args.list:
        table = r.table()

        res = table.list().execute()

        for r in res["items"]:
            log.info("-" * 10)
            log.info("name: {}".format(r["name"]))
            log.info("id  : {}".format(r["tableId"]))
            log.info("cols: {}".format([x["name"] for x in r["columns"]]))

    if args.update:
        if not args.file:
            p.error("missing --file argument")

        try:
            log.warning("deleting all rows from fusion table: {}".format(args.update))
            r.query().sql(sql="DELETE FROM {}".format(args.update)).execute()
        except Exception as e:
            log.error("error deleting rows: {}".format(e))
            #sys.exit(1)

        try:
            mf = MediaFileUpload(filename=args.file,
                                 mimetype="application/octet-stream",
                                 resumable=True)

            log.info("inserting rows from {}".format(args.file))
            res = r.table().importRows(tableId=args.update,
                                       media_body=mf,
                                       encoding="auto-detect",
                                       delimiter=",",
                                       startLine=1).execute()
            log.info("result: {}".format(res))
        except Exception as e:
            log.error("error inserting rows: {}".format(e))
            sys.exit(1)


if __name__ == "__main__":
    main()
