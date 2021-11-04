import asyncio
from aiofile import async_open

from functools import reduce
from hashlib import sha1
from random import choice

from network import MAINTENANCE_FREQUENCY
from node import Node, RemoteNode

from typing import Tuple
from network import CHORD_PORT as _CHORD_PORT
from node import Node as _Node, RemoteNode as _RemoteNode
from abcchord import INode


async def join_network(
    my_ip: str = "0.0.0.0", my_port: int = _CHORD_PORT, bootstrap_node: Tuple = None
) -> INode:
    me = _Node(my_ip, my_port)
    await me._start_server()
    if bootstrap_node:
        entry_point = _RemoteNode(bootstrap_node[0], bootstrap_node[1])
    else:
        entry_point = None
    await me.network.join(entry_point)
    return me




loop = asyncio.new_event_loop() 

node = None         # node resolving a lookup
remote_nodes = []   
IP = "127.0.0.1"
RPORT = 5555


def _get_expected_node(id, nodes):
    candidate_nodes = list(filter(lambda n: n.id > id, nodes)) or nodes
    expected_node = reduce(lambda n1, n2: n1 if n1.id < n2.id else n2, candidate_nodes)
    return expected_node


async def stop_nodes():
    """Stop all nodes"""
    global node, remote_nodes 
    
    await node.leave() 
    node = None 
    
    for local_node, remote_node in remote_nodes:
        await local_node.leave() 
    remote_nodes.clear() 


async def init_nodes(n: int):
    """Initialize n nodes"""
    global node, remote_nodes, IP, RPORT 

    node = Node(IP)
    await node._start_server()

    for i in range(n):
        remote_node_local = Node(IP, RPORT + i) 
        remote_node_remote = RemoteNode(IP, RPORT + i)
        await remote_node_local._start_server()

        remote_nodes.append((remote_node_local, remote_node_remote))
    

async def join_nodes(n: int):
    """Join on n nodes"""
    global remote_nodes 

    await init_nodes(n)

    first_remote_node_local, first_remote_node_remote = (
        remote_nodes[0][0],
        remote_nodes[0][1],
    )
    remote_network = first_remote_node_local.network
    await remote_network.join()

    for finger in remote_network.finger_table:
        assert(finger.node == first_remote_node_local)
    
    for local_node, _ in remote_nodes[1:]:
        await local_node.network.join(first_remote_node_remote)

    await asyncio.sleep(MAINTENANCE_FREQUENCY)

    remote_nodes_remote = list(zip(*remote_nodes))[0]

    for local_node, _ in remote_nodes:
        remote_network = local_node.network
        for finger in remote_network.finger_table:
            expected_node = _get_expected_node(finger.start, remote_nodes_remote)
            assert(finger.node == expected_node)
    
    await stop_nodes()


async def store_nodes(n: int):
    """Store on n nodes"""
    global remote_nodes

    await init_nodes(n)

    first_remote_node_local, first_remote_node_remote = (
        remote_nodes[0][0],
        remote_nodes[0][1],
    )
    remote_network = first_remote_node_local.network
    await remote_network.join()

    for local_node, _ in remote_nodes[1:]:
        await local_node.network.join(first_remote_node_remote)

    await asyncio.sleep(MAINTENANCE_FREQUENCY)

    remote_nodes_remote = list(zip(*remote_nodes))[0]

    content = None 
    data_file_path = "./SampleImage_50kb.jpg"
    async with async_open(data_file_path, "rb") as afp:
        content = await afp.read() 
    
    # content = data.load_file50k()
    hash = sha1()
    hash.update(content)
    key = int.from_bytes(hash.digest(), "big")
    # Local storage
    while _get_expected_node(key, remote_nodes_remote) != first_remote_node_local:
        content += b"a"
        hash = sha1()
        hash.update(content)
        key = int.from_bytes(hash.digest(), "big")

    retrieved_key = await first_remote_node_local.store(content)
    assert(key == retrieved_key)

    retrieved_content = await first_remote_node_local.get(key)
    assert(content == retrieved_content)

    # Remote storage for every N nodes
    for remote_node in remote_nodes_remote[1:]:
        while _get_expected_node(key, remote_nodes_remote) != remote_node:
            content += b"a"
            hash = sha1()
            hash.update(content)
            key = int.from_bytes(hash.digest(), "big")

        retrieved_key = await first_remote_node_local.store(content)
        assert(key == retrieved_key)

        retrieved_content = await first_remote_node_local.get(key)
        assert(content == retrieved_content)

    await stop_nodes()

def test_join_nodes(n: int):
    """Run join node tests"""
    assert(n > 0)
    loop.run_until_complete(join_nodes(n))

def test_store_nodes(n: int):
    """Run store node tests"""
    assert(n > 0)
    loop.run_until_complete(store_nodes(n))


def test_chord():
    """Main handler"""
    n_val = str(input("Specify number of nodes to test with"))
    if len(n_val) == 0:
        print("number of nodes must be positive integer (cannot be 0)")
        return 
    if n_val[0] == "0":
        print("number of nodes must be positive integer (cannot be 0)")
        return 
    for c in n_val:
        if c in ("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"):
            continue 
        print("number of nodes must be positive integer (cannot be 0)")
        return 
    
    # run tests 
    test_join_nodes(int(n_val))
    test_store_nodes(int(n_val))


async def main():

    host = "127.0.0.1"
    port = 5678
    boostrap_node = ("127.0.0.1", 7777)

    network = await join_network(host, port, boostrap_node)
    
    file = b"file"
    file_key = await network.store(file)
    # assert network.get(file_key) == file
    r_file = await network.get(file_key)
    print(f"file: {file};   r_file: {r_file}  ")
    assert(r_file == file)
    
    # ...

    await network.leave()


if __name__ == "__main__":
    # test_chord()
    loop.run_until_complete(main())

    