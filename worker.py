from app.utils import parse_args, get_arg_parser, init_es
from app.shards import scan_shard, get_shards_to_routing

if __name__ == '__main__':
    parser = get_arg_parser()
    args = parse_args(parser)

    es = init_es(args)

    shards_to_routing = get_shards_to_routing(es, args.index, args.doc_type)
    routing = shards_to_routing[args.shard]

    query = {"query": {"match_all": {}}}

    # Example - count documents
    docs = []

    scan_shard(es, args.index, args.doc_type, query, routing, lambda doc: docs.append(doc))

    print(len(docs))
