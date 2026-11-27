"""Run every shipped store paragraph through Zeppelin; no Maven or test-stack changes."""
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen

BASE = os.environ.get("ZEPPELIN_URL", "http://localhost:8090") + "/api"


def api(path, data=None):
    request = Request(BASE + path, data=None if data is None else json.dumps(data).encode(),
                      headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=120) as response:
        result = json.load(response)
    if result["status"] != "OK":
        raise RuntimeError(result)
    return result["body"]


shipped = json.loads(Path(__file__).with_name("zeppelin").joinpath("store.json").read_text())
name = shipped["name"]
note_id = next(n["id"] for n in api("/notebook") if n.get("path", n.get("name", "")).lstrip("/") == name)
note = api("/notebook/" + note_id)
assert {p["title"] for p in shipped["paragraphs"]} <= {p["title"] for p in note["paragraphs"]}
for paragraph in note["paragraphs"]:
    result = api(f"/notebook/run/{note_id}/{paragraph['id']}", {})
    print(paragraph["title"], json.dumps(result), flush=True)
    if result["code"] != "SUCCESS" or not any(
            m["type"] == "TABLE" and len(m["data"].strip().splitlines()) > 1 for m in result["msg"]):
        raise RuntimeError("Failed paragraph: " + paragraph["title"])
    if paragraph["title"] in ("Masked customer join", "Transformed customer notes"):
        for message in result["msg"]:
            if message["type"] == "TABLE":
                data = message["data"]
                assert "@example.test" not in data and "Example Street" not in data and "demo-private-token" not in data
                assert "192.0.2.15" not in data and "+1-555" not in data
                if paragraph["title"] == "Transformed customer notes":
                    assert "[email removed]" in data
