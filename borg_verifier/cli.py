import argparse
import borg_verifier
import logging
import sys

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('pushgateway')
    parser.add_argument('repos', nargs='+')
    parser.add_argument('--auth_username', default=None)
    parser.add_argument('--auth_password', default=None)
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--quiet', action='store_true')
    args = parser.parse_args()

    log_level = logging.INFO
    if args.debug:
        log_level = logging.DEBUG
    elif args.quiet:
        log_level = logging.WARNING
    logging.basicConfig(level=log_level)

    credentials = None
    if args.auth_username or args.auth_password:
        credentials = (args.auth_username, args.auth_password)
        if any(x is None for x in credentials):
            print("Both of auth username and password must be specified",
                  file=sys.stderr)
            return 1

    borg_verifier.run(args.pushgateway, args.repos,
                      auth_credentials=credentials)


if __name__ == '__main__':
    sys.exit(main())
