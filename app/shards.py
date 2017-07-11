from collections import namedtuple

from elasticsearch.helpers import scan

ShardInfo = namedtuple("ShardInfo", "routing address")


def get_shards_to_routing(client, index, doc_type):
    shards_info = client.search_shards(index, doc_type)
    number_of_shards = len(shards_info['shards'])

    shards = {}
    i = 0

    while len(shards.keys()) < number_of_shards:
        r = client.search_shards(index, doc_type, routing=i)
        shard_number = r['shards'][0][0]['shard']
        if shard_number not in shards:
            shards[shard_number] = i
        i += 1

    return shards


def get_shards_info(client, index, doc_type):
    """
    Returns a mapping between shard number to the shards' information - routing and address.
    """
    shards_info = client.search_shards(index, doc_type)
    number_of_shards = len(shards_info['shards'])
    nodes = _get_nodes_to_address(shards_info)

    shards_info = {}
    i = 0

    while len(shards_info.keys()) < number_of_shards:
        result = client.search_shards(index, doc_type, routing=i)
        shard = _get_primary_shard(result)

        shard_number = shard['shard']
        node = shard['node']
        address = nodes[node]

        if shard_number not in shards_info:
            shards_info[shard_number] = ShardInfo(routing=i, address=address)
        i += 1

    return shards_info


def scan_shard(client, index, doc_type, query, routing, func):
    """
    Scans a specific shard by routing number.
    
    :param client: es client 
    :param index: the index to scan
    :param doc_type: the doc_type to scan
    :param query: the query to run
    :param routing: routing number to specific shard
    :param func: func to run on every document
    """
    scroller = scan(client, query, index=index, doc_type=doc_type, routing=routing)
    for doc in scroller:
        func(doc)


def _get_primary_shard(search_shards_result):
    shards = search_shards_result['shards'][0]
    return [shard for shard in shards if shard['primary'] is True][0]


def _get_nodes_to_address(shards_info):
    return dict([(k, v['transport_address'].split(':')[0]) for k, v in shards_info['nodes'].items()])
