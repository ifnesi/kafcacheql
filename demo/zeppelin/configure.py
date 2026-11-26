"""Preconfigure Zeppelin through its public REST API, leaving existing notes alone."""
import json
from pathlib import Path
from urllib.request import Request, urlopen

BASE = "http://zeppelin:8080/api"


def api(path, data=None, method=None):
    request = Request(BASE + path, data=None if data is None else json.dumps(data).encode(),
                      headers={"Content-Type": "application/json"}, method=method)
    with urlopen(request, timeout=60) as response:
        result = json.load(response)
    if result["status"] != "OK":
        raise RuntimeError(result)
    return result.get("body")


settings = json.loads(Path("/demo/interpreter.json").read_text())
existing = next((s for s in api("/interpreter/setting") if s["name"] == settings["name"]), None)
if existing:
    api("/interpreter/setting/" + existing["id"], settings, "PUT")
else:
    api("/interpreter/setting", settings)
note = json.loads(Path("/demo/store.json").read_text())
existing_note = next((n for n in api("/notebook")
                      if n.get("path", "").lstrip("/") == note["name"]), None)
note_id = existing_note["id"] if existing_note else api("/notebook/import", note)
print("Configured store note", note_id, flush=True)
print("JDBC interpreter store configured; shaded JAR mounted at /driver", flush=True)
