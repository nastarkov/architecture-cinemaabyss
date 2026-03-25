import os
import random

import requests
from flask import Flask, Response, jsonify, request

app = Flask(__name__)
session = requests.Session()

MONOLITH_URL = os.getenv("MONOLITH_URL", "http://localhost:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://localhost:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://localhost:8082")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
MOVIES_MIGRATION_PERCENT = max(0, min(100, int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))))


def choose_movies_target() -> str:
    if not GRADUAL_MIGRATION:
        return MOVIES_SERVICE_URL
    if random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT:
        return MOVIES_SERVICE_URL
    return MONOLITH_URL


def forward(base_url: str) -> Response:
    target_url = f"{base_url}{request.path}"

    headers = {}
    for key, value in request.headers.items():
        if key.lower() not in {"host", "content-length", "connection"}:
            headers[key] = value

    upstream = session.request(
        method=request.method,
        url=target_url,
        params=request.args,
        data=request.get_data(),
        headers=headers,
        cookies=request.cookies,
        allow_redirects=False,
        timeout=15,
    )

    excluded = {"content-encoding", "content-length", "transfer-encoding", "connection"}
    response_headers = [(k, v) for k, v in upstream.headers.items() if k.lower() not in excluded]
    return Response(upstream.content, upstream.status_code, response_headers)


@app.get("/health")
def health() -> Response:
    return jsonify({"status": True})


@app.route("/api/movies", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
@app.route("/api/movies/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
def movies(path: str = "") -> Response:
    _ = path
    return forward(choose_movies_target())


@app.route("/api/users", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
@app.route("/api/users/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
def users(path: str = "") -> Response:
    _ = path
    return forward(MONOLITH_URL)


@app.route("/api/payments", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
@app.route("/api/payments/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
def payments(path: str = "") -> Response:
    _ = path
    return forward(MONOLITH_URL)


@app.route("/api/subscriptions", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
@app.route("/api/subscriptions/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
def subscriptions(path: str = "") -> Response:
    _ = path
    return forward(MONOLITH_URL)


@app.route("/api/events", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
@app.route("/api/events/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
def events(path: str = "") -> Response:
    _ = path
    return forward(EVENTS_SERVICE_URL)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
