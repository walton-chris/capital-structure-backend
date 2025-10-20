web: uvicorn app:app --host 0.0.0.0 --port $PORT
worker: PYTHONPATH=. rq worker extract --with-scheduler --log-level=info --url ${REDIS_URL}
