from app.utils import parse_args, get_arg_parser, init_es
from app.shards import scan_shard, get_shards_to_routing
from multiprocessing import Pool


def worker(args, routing):
    _es = init_es(args)
    query = {"query": {"match_all": {}}}
    scan_shard(_es, args.index, args.doc_type, query, routing, lambda doc: doc)


if __name__ == '__main__':
    parser = get_arg_parser()
    args = parse_args(parser)

    es = init_es(args)
    shards_to_routing = get_shards_to_routing(es, args.index, args.doc_type)
    print(shards_to_routing)

    pool = Pool()
    jobs = []
    for shard, routing in shards_to_routing.items():
        print(f"shard: {shard}, routing: {routing}")
        p = pool.apply_async(worker, args=(args, routing,))

    pool.close()
    pool.join()
