import argparse
import base64
import getpass
import sys

from garmin_connector import GarminDomain, generate_token_json


def prompt_mfa_code() -> str:
    print("Enter the Garmin MFA code:", file=sys.stderr)
    code = input().strip()
    if not code:
        raise ValueError("Garmin MFA code cannot be empty")
    return code


def encode_token_json(token_json: str) -> str:
    return base64.b64encode(token_json.encode("utf-8")).decode("ascii")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("email", help="Garmin account email")
    parser.add_argument(
        "--is-cn",
        dest="is_cn",
        action="store_true",
        help="use Garmin China",
    )
    options = parser.parse_args()

    password = getpass.getpass("Garmin password: ")
    domain = GarminDomain.CHINA if options.is_cn else GarminDomain.GLOBAL
    token_json = generate_token_json(
        options.email,
        password,
        domain,
        prompt_mfa_code,
    )
    print(encode_token_json(token_json))
