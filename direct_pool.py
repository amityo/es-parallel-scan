from copy import deepcopy

from app.utils import parse_args, get_arg_parser, init_es
from app.shards import scan_shard, get_shards_info
from multiprocessing import Pool


def worker(args, routing):
    _es = init_es(args)
    query = {"query": {"match_all": {}}}
    scan_shard(_es, args.index, args.doc_type, query, routing, lambda doc: doc)


if __name__ == '__main__':
    parser = get_arg_parser()
    args = parse_args(parser)

    es = init_es(args)
    shards_info = get_shards_info(es, args.index, args.doc_type)
    print(shards_info)

    pool = Pool()
    jobs = []
    for shard, info in shards_info.items():
        _args = deepcopy(args)
        _args.es_host = info.address
        _args.es_port = 9200
        print(f"Scanning Shard: #{shard} with Routing: {info.routing}, at Node: {_args.es_host}:{_args.es_port}")
        p = pool.apply_async(worker, args=(_args, info.routing,))

    pool.close()
    pool.join()
