import argparse


def get_argparser():
    parser = argparse.ArgumentParser(description='Zendesk consumer')
    parser.add_argument('-a', '--account', type=str, required=True, help='API account username')
    parser.add_argument('-t', '--token', type=str, required=True, help='API token')
    parser.add_argument('-b', '--bucket', type=str, required=True, help='S3 Bucket Name')

    args = parser.parse_args()
    return args.account, args.token, args.bucket
