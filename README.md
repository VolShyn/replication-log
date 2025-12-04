# How to start

If you have troubles with `uvicorn`, it's probably because It works only on unix systems;

1. `pip install -r requirements.txt`
2. from the root, run `docker compose up -d --build`, then, you can curl localhost:8000, e.g.:
```
curl -X POST -H "Content-Type: application/json" http://localhost:8000/messages -d '{"content": "msg1"}'
curl http://localhost:8000/messages 
curl http://localhost:8001/messages
curl http://localhost:8002/messages
```
3. go to the `127.0.0.1:8000/docs` to open "swagger" for the master, and `127.0.0.1:800x/docs` for the secondaries.

As for me, swagger is more fun! =)


# Self note

> Replication - copying or reproducing something - is the primary way in which we can fight latency. Replication improves performance by making additional computing power and bandwidth applicable to a new copy of the data. Replication improves availability by creating additional copies of the data, increasing the number of nodes that need to fail before availability is sacrificed. Replication is about providing extra bandwidth, and caching where it counts. It is also about maintaining consistency in some way according to some consistency model. Replication allows us to achieve scalability, performance and fault tolerance. Afraid of loss of availability or reduced performance? Replicate the data to”. Replication allows us to achieve scalability, performance and fault tolerance. Afraid of loss of availability or reduced performance? Replicate the data to avoid a bottleneck or single point of failure. Slow computation? Replicate the computation on multiple systems. Slow I/O? Replicate the data to a local cache to reduce latency or onto multiple machines to increase throughput. Replication is also the source of many of the problems, since there are now independent copies of the data that has to be kept in sync on multiple machines - this means ensuring that the replication follows a consistency model.

- Excerpt from `Distributed systems for fun and profit`, Mikito Takada

**Write concern** (w) - number of ACK needed before considering a write operation successful and responding to the client. W = 1 - master needs ACK only from itself. It's all about `latency / durability` trade-off. From here **Semi-synchronicity** - is when we dont wait for all the secondaries to send ACK.
