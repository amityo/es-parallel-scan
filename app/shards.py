from elasticsearch.helpers import scan


def get_shards_to_routing(client, index, doc_type):
    shards_info = client.search_shards(index, doc_type)
    number_of_shards = len(shards_info['shards'])

    shards = {}
    i = 0

    while len(shards.keys()) < number_of_shards:
        r = client.search_shards(index, doc_type, routing=i)
        shard_number = r['shards'][0][0]['shard']
        if shard_number not in shards:
            print(shard_number, i)
            shards[shard_number] = i
        i += 1

    print(shards)
    return shards


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




