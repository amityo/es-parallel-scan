from app.utils import parse_args, get_arg_parser, init_es
from app.shards import scan_shard


# shards_to_routing = get_shards_to_routing(es, args.index, args.doc_type)
# print(shards_to_routing)

if __name__ == '__main__':
    parser = get_arg_parser()
    args = parse_args(parser)

    es = init_es(args)

    query = {"query": {"match_all": {}}}
    scan_shard(es, args.index, args.doc_type, query, args.routing, lambda doc: print(doc))