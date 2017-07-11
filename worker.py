from app.utils import parse_args, get_arg_parser, init_es
from app.shards import scan_shard, get_shards_info

if __name__ == '__main__':
    parser = get_arg_parser()
    args = parse_args(parser)

    es = init_es(args)

    shards_to_routing = get_shards_info(es, args.index, args.doc_type)
    shard_info = shards_to_routing[args.shard]

    query = {"query": {"match_all": {}}}

    # Example - count documents
    docs = []

    if args.es_direct_node:
        args.es_host = shard_info.address
        es = init_es(args)

    print(f"Scanning Shard: #{args.shard} with Routing: {shard_info.routing}, at Node: {args.es_host}")

    scan_shard(es, args.index, args.doc_type, query, shard_info.routing, lambda doc: docs.append(doc))

    print(len(docs))
