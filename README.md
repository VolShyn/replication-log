# Replicated log

Data is stored and appended in an ordered set of records called ‘log‘. Data is replicated from the "Master" to the "Secondaries";

We would use Pydantic to validate the data;

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
3. go to the `127.0.0.1:8000/docs` to open "swagger" for master, and `127.0.0.1:8001/docs` for one of the secondaries.

As for me, swagger is more fun;
