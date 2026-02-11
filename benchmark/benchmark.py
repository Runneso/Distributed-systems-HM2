import asyncio
import dataclasses
import json
import os
import time
import uuid
import math
import csv
import collections
import random
from typing import (
    Union,
    List,
    Dict,
    Set,
    Tuple,
    Any
)

DELAY = 10
FILENAMES = [
    'resultsA.csv',
    'resultsB.csv'
]
FIELDNAMES = [
    'replicationMode',
    'rf',
    'k',
    'threads',
    'putRatio',
    'totalOps',
    'throughputOpsSec',
    'avgMs',
    'p50Ms',
    'p75Ms',
    'p95Ms',
    'p99Ms'
]
ENCODING = 'utf-8'

SOCKET_COUNT = 10
REQUESTS_COUNT = 1000


@dataclasses.dataclass
class Node:
    node_id: str
    host: str
    port: int

    def to_dict(self):
        return {
            'node_id': self.node_id,
            'hostname': self.host,
            'port': self.port,
        }


LEADER_ID = 'A'
NODES = [
    Node(
        node_id='A',
        host='127.0.0.1',
        port=8081
    ),
    Node(
        node_id='B',
        host='127.0.0.1',
        port=8082
    ),
    Node(
        node_id='C',
        host='127.0.0.1',
        port=8083
    ),
]


def percentile(sorted_vals: List[float], p: float) -> float:
    if not sorted_vals:
        return float('nan')

    if len(sorted_vals) == 1:
        return float(sorted_vals[0])

    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)

    if f == c:
        return float(sorted_vals[int(k)])

    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)

    return float(d0 + d1)


def generate_put(request_id: str) -> Dict[str, Union[int, str]]:
    return {
        'type': 'CLIENT_PUT_REQUEST',
        'request_id': request_id,
        'client_id': str(uuid.uuid4()),
        'key': str(uuid.uuid4()),
        'value': '<value>',
    }


def generate_get(request_id: str) -> Dict[str, Union[int, str]]:
    return {
        'type': 'CLIENT_GET_REQUEST',
        'request_id': request_id,
        'client_id': str(uuid.uuid4()),
        'key': str(uuid.uuid4()),
    }


def generate_cluster_update(
        replication_mode: str,
        replication_factor: int,
        semi_sync_acks: int
) -> Dict[str, Union[int, str]]:
    return {
        'type': 'CLUSTER_UPDATE_REQUEST',
        'request_id': str(uuid.uuid4()),
        'nodes': NODES,
        'leader_id': LEADER_ID,
        'replication_mode': replication_mode,
        'replication_factor': replication_factor,
        'semi_sync_acks': semi_sync_acks,
        'min_delay_ms': 0,
        'max_delay_ms': 0,
    }


def generate_row(
        replication_mode: str,
        replication_factor: int,
        semi_sync_acks: int,
        put_ratio: float,
        total_ops: int,
        throughput_ops_sec: float,
        avg_ms: float,
        p50_ms: float,
        p75_ms: float,
        p95_ms: float,
        p99_ms: float
) -> Dict[str, Union[int, str, float]]:
    return {
        'replicationMode': replication_mode,
        'rf': replication_factor,
        'k': semi_sync_acks if replication_mode == 'semi-sync' else -1,
        'threads': 16,
        'putRatio': put_ratio,
        'totalOps': total_ops,
        'throughputOpsSec': round(throughput_ops_sec, 3),
        'avgMs': round(avg_ms, 3),
        'p50Ms': round(p50_ms, 3),
        'p75Ms': round(p75_ms, 3),
        'p95Ms': round(p95_ms, 3),
        'p99Ms': round(p99_ms, 3),
    }


async def reader_loop(
        reader: asyncio.StreamReader,
        processed: Set[str],
        finish_time_by_request_id: collections.defaultdict[str, float]
) -> None:
    while True:
        line = await reader.readline()
        response = json.loads(line.decode())
        if response['type'] in ('CLIENT_GET_RESPONSE', 'CLIENT_PUT_RESPONSE'):
            processed.add(response['request_id'])
            finish_time_by_request_id[response['request_id']] = time.perf_counter()


def encode(obj):
    if isinstance(obj, Node):
        return obj.__dict__
    raise TypeError(f'{type(obj)} is not JSON serializable')


async def send(
        writer: asyncio.StreamWriter,
        payload: Dict[str, Union[str, int]],
        start_time_by_request_id: collections.defaultdict[str, float]
) -> None:
    start_time_by_request_id[payload['request_id']] = time.perf_counter()
    msg = json.dumps(payload, ensure_ascii=False, default=encode) + '\n'
    writer.write(msg.encode(ENCODING))
    await writer.drain()


async def open_connection(
        node_id: str,
        host: str,
        port: int
) -> Tuple[str, Any]:
    return node_id, *await asyncio.open_connection(host, port)


async def benchmark(
        put_ratio: float,
        replication_mode: str,
        replication_factor: int,
        semi_sync_acks: int,
        filename: str
) -> None:
    processed = set()
    start_time_by_request_id = collections.defaultdict(float)
    finish_time_by_request_id = collections.defaultdict(float)
    writers_by_node_id = collections.defaultdict(list)
    counter_by_node_id = collections.defaultdict(int)
    readers = list()

    open_connections_tasks = list()

    for index in range(SOCKET_COUNT):
        node = NODES[index % len(NODES)]
        open_connections_tasks.append(open_connection(node.node_id, node.host, node.port))

    results = await asyncio.gather(*open_connections_tasks)

    for node_id, reader, writer in results:
        readers.append(
            asyncio.create_task(
                reader_loop(
                    reader=reader,
                    processed=processed,
                    finish_time_by_request_id=finish_time_by_request_id,
                )
            )
        )
        writers_by_node_id[node_id].append(writer)

    set_configuration_tasks = list()

    for node in NODES:
        set_configuration_tasks.append(
            send(
                writers_by_node_id[node.node_id][0],
                generate_cluster_update(
                    replication_mode,
                    replication_factor,
                    semi_sync_acks
                ),
                start_time_by_request_id
            )
        )

    await asyncio.gather(*set_configuration_tasks)

    requests_tasks = list()

    for index in range(REQUESTS_COUNT):
        node = NODES[index % len(NODES)]
        node_id = node.node_id
        request_id = str(uuid.uuid4())
        start_time_by_request_id[request_id] = -1  # чтобы мапа заранее разрослась
        finish_time_by_request_id[request_id] = -1  # чтобы мапа заранее разрослась

        if random.random() <= put_ratio:
            payload = generate_put(request_id)
            node_id = LEADER_ID
        else:
            payload = generate_get(request_id)

        requests_tasks.append(
            send(
                writers_by_node_id[node_id][counter_by_node_id[node_id]],
                payload,
                start_time_by_request_id
            )
        )
        counter_by_node_id[node_id] = (counter_by_node_id[node_id] + 1) % len(writers_by_node_id[node_id])

    await asyncio.gather(*requests_tasks)

    waiting = True

    while waiting:
        if len(processed) == REQUESTS_COUNT:
            waiting = False
        await asyncio.sleep(0.1)

    for task in readers:
        task.cancel()

    latencies_ms = list()
    starts = list()
    finishes = list()

    for request_id in processed:
        start_time = start_time_by_request_id[request_id]
        finish_time = finish_time_by_request_id[request_id]
        if start_time <= 0 or finish_time <= 0 or finish_time < start_time:
            continue
        starts.append(start_time)
        finishes.append(finish_time)
        delta = finish_time - start_time
        latencies_ms.append(delta * 1000.0)

    latencies_ms.sort()

    total_ops = len(latencies_ms)
    t_start = min(starts)
    t_end = max(finishes)
    throughput_ops_sec = total_ops / (t_end - t_start)

    avg_ms = sum(latencies_ms) / total_ops
    p50_ms = percentile(latencies_ms, 50)
    p75_ms = percentile(latencies_ms, 75)
    p95_ms = percentile(latencies_ms, 95)
    p99_ms = percentile(latencies_ms, 99)
    need_header = (not os.path.exists(filename)) or (os.path.getsize(filename) == 0)

    with open(filename, 'a', newline=str(), encoding=ENCODING) as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES)

        if need_header:
            writer.writeheader()

        writer.writerow(
            generate_row(
                replication_mode=replication_mode,
                replication_factor=replication_factor,
                semi_sync_acks=semi_sync_acks,
                total_ops=total_ops,
                throughput_ops_sec=throughput_ops_sec,
                put_ratio=put_ratio,
                avg_ms=avg_ms,
                p50_ms=p50_ms,
                p75_ms=p75_ms,
                p95_ms=p95_ms,
                p99_ms=p99_ms
            )
        )


async def main():
    for replication_mode in ('sync', 'async'):
        for put_ratio in (0.8, 0.2):
            for replication_factor in (1, 2, 3):
                await benchmark(
                    put_ratio=put_ratio,
                    replication_mode=replication_mode,
                    replication_factor=replication_factor,
                    semi_sync_acks=-1,
                    filename=FILENAMES[0]
                )
                await asyncio.sleep(DELAY)

    for replication_mode in ('async', 'semi-sync', 'sync'):
        for put_ratio in (0.8, 0.2):
            await benchmark(
                put_ratio=put_ratio,
                replication_mode=replication_mode,
                replication_factor=3,
                semi_sync_acks=1,
                filename=FILENAMES[1]
            )
            await asyncio.sleep(DELAY)


if __name__ == '__main__':
    asyncio.run(main())
