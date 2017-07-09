from argparse import ArgumentParser
from elasticsearch import Elasticsearch
import os


def init_es(args):
    es_args = {}
    if args.es_use_ssl:
        import certifi
        es_args = {"use_ssl": True, "verify_certs": True, "ca_certs": certifi.where()}

    return Elasticsearch([{"host": args.es_host, "port": args.es_port, "http_auth": args.es_auth}], **es_args)


def get_arg_parser():
    parser = ArgumentParser()
    parser.add_argument("-i", "--index")
    parser.add_argument("-d", "--doc-type")
    parser.add_argument("-r", "--routing", type=int)
    parser.add_argument("--es-host")
    parser.add_argument("--es-port", type=int)
    parser.add_argument("--es-auth")
    parser.add_argument("--es-use-ssl", type=bool)
    return parser


def parse_args(parser):
    args = parser.parse_args()
    print(args)

    args.index = args.index or os.environ.get("INDEX")
    args.doc_type = args.doc_type or os.environ.get("DOC_TYPE")
    args.routing = args.routing or int(os.environ.get("ROUTING", 0))

    args.es_host = args.es_host or os.environ.get("ES_HOST")
    args.es_port = args.es_port or int(os.environ.get("ES_PORT", 9200))
    args.es_auth = args.es_auth or os.environ.get("ES_AUTH")
    args.es_use_ssl = args.es_use_ssl or bool(os.environ.get("ES_USE_SSL", False))
    print(args)
    return args
