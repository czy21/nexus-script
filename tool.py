#!/usr/bin/env python3
import argparse
import pathlib
import requests
import urllib3
from requests.auth import HTTPBasicAuth

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

common_parser = argparse.ArgumentParser(add_help=False)
common_parser.add_argument("--host", required=True)
common_parser.add_argument('-u', "--username", required=True)
common_parser.add_argument('-p', "--password", required=True)


def cmd_clean(args):
    auth = HTTPBasicAuth(args.username, args.password)

    for r in args.repository:
        continuation_token = None

        while True:
            params = {"repository": r}
            if continuation_token:
                params["continuationToken"] = continuation_token

            get_res = requests.get(f"{args.host}/service/rest/v1/assets", params=params, auth=auth, verify=False)

            get_res.raise_for_status()
            data = get_res.json()

            items = data.get("items", [])

            if not items: break

            for t in items:
                requests.delete(f"{args.host}/service/rest/v1/assets/{t.get('id')}", auth=auth, verify=False)

            continuation_token = data.get("continuationToken")

            if not continuation_token:
                break
        print(f"cleaned {r}")


def cmd_script_run(args):
    auth = HTTPBasicAuth(args.username, args.password)
    res = requests.post(f"{args.host}/service/rest/v1/script/{args.name}/run", headers={"Content-Type": "text/plain"}, auth=auth, verify=False)
    print(res.json())


def cmd_script_update(args):
    auth = HTTPBasicAuth(args.username, args.password)
    res = requests.get(f"{args.host}/service/rest/v1/script", auth=auth, verify=False)
    data = res.json()
    for t in data:
        requests.delete(f"{args.host}/service/rest/v1/script/{t.get('name')}", auth=auth, verify=False)
    for s in pathlib.Path(__file__).parent.joinpath('src/main/groovy').glob("*"):
        requests.post(f"{args.host}/service/rest/v1/script",
                      headers={"Content-Type": "application/json"},
                      json={'name': s.stem, 'content': s.read_text(), 'type': 'groovy'},
                      auth=auth, verify=False)
    res = requests.get(f"{args.host}/service/rest/v1/script", auth=auth, verify=False)
    print([t.get('name') for t in res.json()])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_script = subparsers.add_parser("script", help="")
    script_subparsers = p_script.add_subparsers(dest="script_cmd")
    script_subparsers.required = True

    # script run
    p_run = script_subparsers.add_parser("run", parents=[common_parser])
    p_run.add_argument("--name", required=True)
    p_run.add_argument("--repo")
    p_run.set_defaults(func=cmd_script_run)

    # script update
    p_update = script_subparsers.add_parser("update", parents=[common_parser])
    p_update.add_argument("--name")
    p_update.set_defaults(func=cmd_script_update)

    p_clean = subparsers.add_parser("clean", parents=[common_parser], help="")
    p_clean.add_argument('-r', '--repository', nargs="+", default=[])
    p_clean.set_defaults(func=cmd_clean)

    args = parser.parse_args()
    args.func(args)
