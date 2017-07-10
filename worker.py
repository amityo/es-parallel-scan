import json

from app.utils import parse_args, get_arg_parser, init_es
from app.shards import scan_shard


def get_routing_by_shard_config():
    with open('shards.json', 'r') as f:
        shards_to_routing = json.load(f)
        return shards_to_routing[args.shard]

if __name__ == '__main__':
    parser = get_arg_parser()
    args = parse_args(parser)

    es = init_es(args)

    routing = get_routing_by_shard_config()

    query = {"query": {"match_all": {}}}

    # Example - count documents
    docs = []

    scan_shard(es, args.index, args.doc_type, query, routing, lambda doc: docs.append(doc))

    print(len(docs))
