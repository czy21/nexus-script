#!/usr/bin/env python3
import json,os

import argparse
import pathlib
import requests
import urllib3
from future.moves import configparser
from requests.auth import HTTPBasicAuth

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

common_parser = argparse.ArgumentParser(add_help=False)
common_parser.add_argument('--print')


def cmd_clean(args):
    for r in args.repository:
        continuation_token = None

        while True:
            params = {"repository": r}
            if continuation_token:
                params["continuationToken"] = continuation_token

            get_res = requests.get(f"{args.host}/service/rest/v1/assets", params=params, auth=args.auth, verify=False)

            get_res.raise_for_status()
            data = get_res.json()

            items = data.get("items", [])

            if not items: break

            for t in items:
                requests.delete(f"{args.host}/service/rest/v1/assets/{t.get('id')}", auth=args.auth, verify=False)

            continuation_token = data.get("continuationToken")

            if not continuation_token:
                break
        print(f"cleaned {r}")


def cmd_script_run(args):
    res = requests.post(f"{args.host}/service/rest/v1/script/{args.name}/run", headers={"Content-Type": "text/plain"}, auth=args.auth, verify=False)
    print(res.json())


def cmd_script_update(args):
    res = requests.get(f"{args.host}/service/rest/v1/script", auth=args.auth, verify=False)
    data = res.json()
    for t in data:
        requests.delete(f"{args.host}/service/rest/v1/script/{t.get('name')}", auth=args.auth, verify=False)
    for s in pathlib.Path(__file__).parent.joinpath('src/main/groovy').glob("*"):
        requests.post(f"{args.host}/service/rest/v1/script",
                      headers={"Content-Type": "application/json"},
                      json={'name': s.stem, 'content': s.read_text(), 'type': 'groovy'},
                      auth=args.auth, verify=False)
    res = requests.get(f"{args.host}/service/rest/v1/script", auth=args.auth, verify=False)
    print([t.get('name') for t in res.json()])


def cmd_repository_restore(args):
    repository_file = pathlib.Path(__file__).parent.joinpath('repository.json')
    if not repository_file.exists():
        raise FileNotFoundError(f"repository.json not found at {repository_file}")
    res = requests.get(f"{args.host}/service/rest/v1/repositories", auth=args.auth, verify=False)
    for t in res.json():
        requests.delete(f"{args.host}/service/rest/v1/repositories/{t.get('name')}", auth=args.auth, verify=False)
    repositories = json.loads(repository_file.read_text())
    repositories.sort(key=lambda x: x.get('type') == 'group')
    print(f"Restoring {len(repositories)} repositories")
    for t in repositories:
        fmt = t.get('format')
        fmt = 'maven' if fmt == 'maven2' else fmt
        res = requests.post(f"{args.host}/service/rest/v1/repositories/{fmt}/{t.get('type')}", json=t, auth=args.auth, verify=False)
        if res.status_code != 201:
            print(f"{t.get('name')} {res.text}")
    res = requests.get(f"{args.host}/service/rest/v1/repositories", auth=args.auth, verify=False)
    print(f"Restored {len(res.json())} repositories")


def cmd_repository_recreate(args):
    res = requests.get(f"{args.host}/service/rest/v1/repositorySettings", auth=args.auth, verify=False)
    data = res.json()
    repositories_other = [t for t in data if t.get('type') != 'group']
    repositories_group = [t for t in data if t.get('type') == 'group']

    repositories_other = [t for t in repositories_other if t.get('type') == args.type] if args.type else repositories_other

    repositories = repositories_other + repositories_group

    for t in repositories:
        fmt = t.get('format')
        fmt = 'maven' if fmt == 'maven2' else fmt
        res = requests.delete(f"{args.host}/service/rest/v1/repositories/{t.get('name')}", auth=args.auth, verify=False)
        res.raise_for_status()
        res = requests.post(f"{args.host}/service/rest/v1/repositories/{fmt}/{t.get('type')}", json=t, auth=args.auth, verify=False)
        res.raise_for_status()


def cmd_repository_backup(args):
    res = requests.get(f"{args.host}/service/rest/v1/repositorySettings", auth=args.auth, verify=False)
    print(f"repositorySettings {res.json().__len__()}")
    pathlib.Path(__file__).parent.joinpath('repository.json').write_text(res.text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Environment variables:
  NEXUS_HOST       Nexus Repository server URL.
  NEXUS_USERNAME   Nexus Repository username.
  NEXUS_PASSWORD   Nexus Repository password.""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        metavar=""
    )

    # repository
    p_repository = subparsers.add_parser(
        "repository",
        help="manage repositories",
    )
    repository_subparsers = p_repository.add_subparsers(
        dest="repository_cmd",
        required=True,
        metavar="",
    )

    p_repository_recreate = repository_subparsers.add_parser(
        "recreate",
        parents=[common_parser],
        help="recreate a repository",
    )
    p_repository_recreate.add_argument("--name")
    p_repository_recreate.add_argument("--type")
    p_repository_recreate.add_argument("--format")
    p_repository_recreate.set_defaults(func=cmd_repository_recreate)

    p_repository_restore = repository_subparsers.add_parser(
        "restore",
        parents=[common_parser],
        help="restore repositories",
    )
    p_repository_restore.set_defaults(func=cmd_repository_restore)

    p_repository_backup = repository_subparsers.add_parser(
        "backup",
        parents=[common_parser],
        help="back up repositories",
    )
    p_repository_backup.set_defaults(func=cmd_repository_backup)

    # script
    p_script = subparsers.add_parser(
        "script",
        help="manage scripts",
    )
    script_subparsers = p_script.add_subparsers(
        dest="script_cmd",
        required=True,
        metavar="",
    )

    p_run = script_subparsers.add_parser(
        "run",
        parents=[common_parser],
        help="run a script",
    )
    p_run.add_argument("--name", required=True)
    p_run.set_defaults(func=cmd_script_run)

    p_update = script_subparsers.add_parser(
        "update",
        parents=[common_parser],
        help="update scripts",
    )
    p_update.set_defaults(func=cmd_script_update)

    # clean
    p_clean = subparsers.add_parser(
        "clean",
        parents=[common_parser],
        help="clean resources",
    )
    p_clean.add_argument("-r", "--repository", nargs="+", default=[])
    p_clean.set_defaults(func=cmd_clean)

    args = parser.parse_args()
    args.host = os.getenv("NEXUS_HOST")
    args.auth = HTTPBasicAuth(
        os.getenv("NEXUS_USERNAME"),
        os.getenv("NEXUS_PASSWORD"),
    )
    args.func(args)
