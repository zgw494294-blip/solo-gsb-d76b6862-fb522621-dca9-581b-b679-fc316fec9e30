"""端到端检查：需要一个已启动的 uvicorn（见下方 BASE）。"""
import base64
import gzip
import io
import json
import os
import sqlite3
import threading
import time
import uuid

import httpx

BASE = os.environ.get("BASE", "http://127.0.0.1:8011")
DB_PATH = os.environ.get("CHECK_DB_PATH", "/tmp/ndjson_check/batches.db")

passed = 0
failed = []


def check(name, cond, extra=""):
    global passed
    if cond:
        passed += 1
        print(f"  PASS {name}")
    else:
        failed.append(name)
        print(f"  FAIL {name} {extra}")


def gz(data: bytes) -> bytes:
    return gzip.compress(data, mtime=0)


def ndjson(records, trailing_newline=True, raw_last: bytes | None = None) -> bytes:
    buf = b"".join(
        (json.dumps(r, ensure_ascii=False).encode() + b"\n") for r in records
    )
    if not trailing_newline:
        buf = buf[:-1]
    if raw_last is not None:
        buf += raw_last
    return buf


def list_batch_ids():
    r = httpx.get(f"{BASE}/batches", params={"limit": 1000})
    r.raise_for_status()
    return {b["batch_id"] for b in r.json()["items"]}


def main():
    c = httpx.Client(base_url=BASE, timeout=30)
    # 每次运行独立的检索标记，避免与历史运行留在库里的数据互相干扰
    tok = uuid.uuid4().hex[:10]
    mark = f"slowmark{uuid.uuid4().hex[:10]}"

    print("== 正常路径 ==")
    before = list_batch_ids()
    recs = [
        {"id": "a", "message": "hello"},
        {"id": "b", "message": "世界"},
        {"id": "c", "message": "x" * 70000},
    ]
    # 第三行正文超过 64KiB？message 70000 字节的 ASCII，整行 > 64KiB，应 413
    r = c.post("/batches", content=gz(ndjson(recs)))
    check("line >64KiB -> 413", r.status_code == 413, r.text)
    check("failed upload not listed", list_batch_ids() == before)

    recs = [
        {"id": "a", "message": "hello"},
        {"id": "b", "message": "世界"},
        {"id": "c", "message": "ok"},
    ]
    r = c.post("/batches", content=gz(ndjson(recs, trailing_newline=False)))
    check("normal upload no trailing newline -> 201", r.status_code == 201, r.text)
    bid = r.json()["batch_id"]
    check("line_count", r.json()["line_count"] == 3)

    r = c.get(f"{BASE}/batches/{bid}/lines")
    check("read lines 200", r.status_code == 200)
    items = r.json()["items"]
    check(
        "order & content preserved",
        [(i["seq"], i["id"], i["message"]) for i in items]
        == [(1, "a", "hello"), (2, "b", "世界"), (3, "c", "ok")],
        items,
    )
    r = c.get(f"{BASE}/batches/{bid}/lines", params={"limit": 2, "offset": 1})
    check("pagination", [i["id"] for i in r.json()["items"]] == ["b", "c"])
    check("pagination total", r.json()["line_count"] == 3)

    r = c.get(f"{BASE}/batches/deadbeef/lines")
    check("unknown id -> 404", r.status_code == 404, r.status_code)

    r = c.get(f"{BASE}/batches")
    body = r.json()
    check("list has new batch", bid in {b["batch_id"] for b in body["items"]})
    check("list fields", all({"batch_id", "line_count", "created_at"} <= set(b)
                             for b in body["items"]))
    r = c.get(f"{BASE}/batches", params={"limit": 1})
    check("list pagination", len(r.json()["items"]) == 1)

    print("== 422 类 ==")
    def expect422(name, payload, headers=None):
        try:
            r = c.post("/batches", content=payload, headers=headers or {})
            check(name, r.status_code == 422, f"got {r.status_code} {r.text[:200]}")
        except httpx.HTTPError as e:
            check(name, False, f"client error {e}")

    expect422("empty body -> 422", b"")
    expect422("gzip of empty -> 422", gz(b""))
    expect422("corrupt gzip -> 422", b"\x1f\x8b\x08not really gzip")
    good = gz(ndjson([{"id": "a", "message": "m"}]))
    expect422("trailing data -> 422", good + b"X")
    expect422("truncated gzip -> 422", good[:-8])
    # 故意改坏 CRC（gzip 尾部倒数第 8 字节起）
    bad_crc = bytearray(good)
    bad_crc[-8] ^= 0xFF
    expect422("bad CRC -> 422", bytes(bad_crc))
    expect422("empty line -> 422", gz(b'{"id":"a","message":"m"}\n\n'))
    expect422("leading empty line -> 422", gz(b'\n{"id":"a","message":"m"}\n'))
    expect422("invalid JSON -> 422", gz(b'{not json\n'))
    expect422("invalid UTF-8 -> 422", gz(b'{"id":"a","message":"\xff"}\n'))
    expect422("missing key -> 422", gz(b'{"id":"a"}\n'))
    expect422("extra key -> 422", gz(b'{"id":"a","message":"m","x":1}\n'))
    expect422("empty id -> 422", gz(b'{"id":"","message":"m"}\n'))
    expect422("empty message -> 422", gz(b'{"id":"a","message":""}\n'))
    expect422("non-string id -> 422", gz(b'{"id":1,"message":"m"}\n'))
    expect422("null message -> 422", gz(b'{"id":"a","message":null}\n'))
    expect422("duplicate id in batch -> 422",
              gz(b'{"id":"a","message":"1"}\n{"id":"a","message":"2"}\n'))
    expect422("json number top-level -> 422", gz(b'123\n'))
    expect422("duplicate json key -> 422",
              gz(b'{"id":"a","id":"b","message":"m"}\n'))
    # 第二条记录坏：第一条不得变可见
    expect422("bad 2nd record -> 422",
              gz(b'{"id":"z1","message":"m"}\n{"id":"z2","message":}\n'))
    check("no zombie batch from failures", list_batch_ids() == before | {bid})

    print("== 413 类 ==")
    # 解压后 >8MiB（高压缩比，压缩体远小于 2MiB）；id 必须唯一
    big_lines = b"".join(
        b'{"id":"%07d","message":"aaaaaaaa"}\n' % i for i in range(290_000)
    )
    check("fixture decompressed size > 8MiB",
          len(big_lines) > 8 * 1024 * 1024, str(len(big_lines)))
    check("fixture compressed size < 2MiB",
          len(gz(big_lines)) <= 2 * 1024 * 1024, str(len(gz(big_lines))))
    r = c.post("/batches", content=gz(big_lines))
    check("decompressed >8MiB -> 413", r.status_code == 413, r.text[:120])

    # 边界：单行恰好 64KiB（整行字节）合法；用紧凑分隔符精确构造
    skeleton = json.dumps(
        {"id": "boundary", "message": ""}, separators=(",", ":")).encode()
    msg = "y" * (64 * 1024 - len(skeleton))
    raw = json.dumps(
        {"id": "boundary", "message": msg}, separators=(",", ":")).encode()
    check("fixture line exactly 64KiB", len(raw) == 64 * 1024, str(len(raw)))
    r = c.post("/batches", content=gz(raw))
    check("line exactly 64KiB -> 201", r.status_code == 201, r.text[:150])
    if r.status_code == 201:
        boundary_bid = r.json()["batch_id"]
    else:
        boundary_bid = None
    committed_so_far = before | {bid}
    if boundary_bid:
        committed_so_far |= {boundary_bid}

    # 单行 >64KiB（非 JSON 也先判字节）
    r = c.post("/batches", content=gz(b"x" * 70000 + b"\n"))
    check("single line >64KiB -> 413", r.status_code == 413, r.text[:120])

    # 压缩体 >2MiB：合法 gzip 封装的不可压缩数据（约 3MiB），服务端中途即停
    over = gz(os.urandom(3 * 1024 * 1024))
    try:
        r = c.post("/batches", content=over)
        check("compressed >2MiB -> 413",
              r.status_code == 413, f"got {r.status_code}")
    except httpx.HTTPError:
        # 服务端已立即回 413 并关连接，客户端写剩余 body 时可能感知到断连
        check("compressed >2MiB -> 413 (connection closed)", True)

    check("still no zombie batches", list_batch_ids() == committed_so_far)

    print("== 并发：慢上传不阻塞其它请求，且完成前不可见 ==")
    # 每条记录带不同的不可压缩内容：压缩后仍足够大，上传可持续数秒；
    # 总解压量约 500*1280B ≈ 640KB，接收中就会多次刷入独立暂存文件
    slow_payload = gz(b"".join(
        json.dumps({"id": f"s{i:06d}",
                    "message": os.urandom(600).hex() + " " + mark})
            .encode() + b"\n"
        for i in range(500)
    ))
    chunks = [slow_payload[i:i + 1000]
              for i in range(0, len(slow_payload), 1000)]
    total_seconds = len(chunks) * 0.05
    check("slow upload lasts several seconds", total_seconds > 5,
          f"{total_seconds:.1f}s, {len(slow_payload)} compressed bytes")

    def gen():
        for ch in chunks:
            time.sleep(0.05)
            yield ch

    result = {}
    def slow_post():
        try:
            with httpx.Client(base_url=BASE, timeout=60) as cc:
                result["r"] = cc.post("/batches", content=gen())
        except Exception as e:
            result["e"] = e

    t = threading.Thread(target=slow_post)
    before_slow = list_batch_ids()
    t.start()
    time.sleep(1.5)  # 慢上传进行中
    ids_mid = list_batch_ids()
    check("no new batch visible mid-flight", ids_mid == before_slow,
          f"extra={ids_mid - before_slow}")
    # 每批次独占一个暂存 SQLite 文件；接收中应已创建并有内容落盘，
    # 但主库里绝不可见
    import glob as _glob
    stage_files = [p for p in _glob.glob(
        os.path.join(os.path.dirname(DB_PATH), "staging", "stage_*.db"))]
    stage_size = sum(os.path.getsize(p) for p in stage_files)
    check("dedicated staging file present mid-flight",
          len(stage_files) == 1 and stage_size > 0,
          f"files={stage_files} size={stage_size}")
    t0 = time.time()
    r1 = c.get("/batches")
    read_ms = (time.time() - t0) * 1000
    t0 = time.time()
    r2 = c.post("/batches",
                content=gz(ndjson([{"id": "fast1", "message": "go"}])))
    post_ms = (time.time() - t0) * 1000
    fast_bid = r2.json()["batch_id"]
    check("read during slow upload fast", read_ms < 3000, f"{read_ms:.0f}ms")
    check("another submit during slow upload fast -> 201",
          r2.status_code == 201 and post_ms < 3000,
          f"{r2.status_code} {post_ms:.0f}ms")
    ids_after_fast = list_batch_ids()
    check("fast batch visible, slow still invisible",
          ids_after_fast == before_slow | {fast_bid},
          f"extra={ids_after_fast - before_slow - {fast_bid}}")
    # 暂存中的批次不参与检索：慢上传的标记此刻应查不到任何行
    r = c.get("/logs/search", params={"q": mark, "page_size": 100})
    check("staging rows not searchable mid-flight",
          r.status_code == 200 and r.json()["items"] == []
          and r.json()["next_cursor"] is None, r.text[:150])
    t.join()
    check("slow upload took its time", "r" in result
          and result["r"].status_code == 201,
          str(result.get("e", "")))
    slow_bid = result["r"].json()["batch_id"]
    check("slow batch visible after completion",
          slow_bid in list_batch_ids() and slow_bid != fast_bid)
    check("slow batch line count", result["r"].json()["line_count"] == 500)
    r = c.get(f"{BASE}/batches/{slow_bid}/lines", params={"limit": 3})
    check("slow batch lines ordered",
          [(i["seq"], i["id"]) for i in r.json()["items"]]
          == [(1, "s000000"), (2, "s000001"), (3, "s000002")])
    # 提交后整批可被检索：用游标翻页数满 500 行
    total, cursor = 0, None
    while True:
        params = {"page_size": 100}
        params["q" if cursor is None else "cursor"] = mark if cursor is None else cursor
        body = c.get("/logs/search", params=params).json()
        total += len(body["items"])
        cursor = body["next_cursor"]
        if cursor is None:
            break
    check("slow batch searchable after commit (500 hits)", total == 500, str(total))

    print("== 日志检索 ==")
    s1 = [
        {"id": "dup", "message": f"{tok} Error disk_full"},
        {"id": "m2", "message": f"{tok} error diskXfull"},
        {"id": "m3", "message": f"{tok} plain"},
        {"id": "m4", "message": f"{tok}_score"},
        {"id": "m5", "message": f"{tok}Xscore"},
        {"id": "m6", "message": f"{tok}%"},
    ]
    r = c.post("/batches", content=gz(ndjson(s1)))
    check("search fixture batch1 -> 201", r.status_code == 201, r.text)
    sb1 = r.json()["batch_id"]

    def search(params):
        return c.get("/logs/search", params=params)

    r = search({"q": f"{tok} Error", "page_size": 10})
    check("search 200", r.status_code == 200, r.text[:200])
    body = r.json()
    check("case sensitive: Error", [i["id"] for i in body["items"]] == ["dup"], body)
    check("single page -> null cursor", body["next_cursor"] is None, body)
    check("item fields",
          all(set(i) == {"batch_id", "seq", "id", "message"} for i in body["items"]))
    check("item batch_id & seq",
          (body["items"][0]["batch_id"], body["items"][0]["seq"]) == (sb1, 1))

    r = search({"q": f"{tok} error", "page_size": 10})
    check("case sensitive: error", [i["id"] for i in r.json()["items"]] == ["m2"])

    r = search({"q": f"{tok}%", "page_size": 10})
    check("% is literal", [i["id"] for i in r.json()["items"]] == ["m6"],
          r.text[:200])

    r = search({"q": f"{tok}_score", "page_size": 10})
    check("_ is literal", [i["id"] for i in r.json()["items"]] == ["m4"],
          r.text[:200])

    r = search({"q": f" {tok}%", "page_size": 10})
    check("whitespace preserved (not stripped)", r.json()["items"] == [])

    r = search({"q": f"{tok}-no-such", "page_size": 10})
    body = r.json()
    check("no match -> empty items + null cursor",
          body["items"] == [] and body["next_cursor"] is None, body)

    # 分页 + 快照：翻页之间提交的新批次不混入旧查询，但新查询可见
    r = search({"q": tok, "page_size": 2})
    body = r.json()
    check("page1", [(i["batch_id"], i["seq"], i["id"]) for i in body["items"]]
          == [(sb1, 1, "dup"), (sb1, 2, "m2")], body)
    check("page1 has cursor", bool(body["next_cursor"]))
    cur1 = body["next_cursor"]

    s2 = [{"id": "dup", "message": f"{tok} second batch"}]
    r = c.post("/batches", content=gz(ndjson(s2)))
    check("search fixture batch2 -> 201", r.status_code == 201, r.text)
    sb2 = r.json()["batch_id"]

    r = search({"cursor": cur1, "page_size": 2})
    body = r.json()
    check("page2 keeps snapshot (new batch not mixed in)",
          [i["id"] for i in body["items"]] == ["m3", "m4"], body)
    cur2 = body["next_cursor"]
    check("page2 has cursor", bool(cur2))

    r = search({"cursor": cur2, "page_size": 2})
    body = r.json()
    check("page3 (last page) -> null cursor",
          [i["id"] for i in body["items"]] == ["m5", "m6"]
          and body["next_cursor"] is None, body)

    r = search({"q": tok, "page_size": 100})
    body = r.json()
    ids = [i["id"] for i in body["items"]]
    check("new query sees new batch",
          ids == ["dup", "m2", "m3", "m4", "m5", "m6", "dup"], ids)
    check("same id across batches kept", ids.count("dup") == 2)
    check("ordered by commit order then seq",
          [i["batch_id"] for i in body["items"]] == [sb1] * 6 + [sb2]
          and [i["seq"] for i in body["items"]] == [1, 2, 3, 4, 5, 6, 1])

    print("== 检索参数校验 ==")
    check("no q no cursor -> 422",
          search({"page_size": 10}).status_code == 422)
    check("q and cursor -> 422",
          search({"q": tok, "cursor": cur1, "page_size": 10}).status_code == 422)
    check("empty q -> 422", search({"q": "", "page_size": 10}).status_code == 422)
    check("q 201 chars -> 422",
          search({"q": "x" * 201, "page_size": 10}).status_code == 422)
    check("q 200 chars ok",
          search({"q": "x" * 200, "page_size": 10}).status_code == 200)
    check("missing page_size -> 422", search({"q": tok}).status_code == 422)
    check("page_size 0 -> 422",
          search({"q": tok, "page_size": 0}).status_code == 422)
    check("page_size 101 -> 422",
          search({"q": tok, "page_size": 101}).status_code == 422)
    check("page_size 100 ok",
          search({"q": tok, "page_size": 100}).status_code == 200)
    check("page_size non-int -> 422",
          search({"q": tok, "page_size": "abc"}).status_code == 422)
    check("garbage cursor -> 422", search({"cursor": "!!!"}).status_code == 422)
    not_json = base64.urlsafe_b64encode(b"not json").decode().rstrip("=")
    check("base64 non-json cursor -> 422",
          search({"cursor": not_json}).status_code == 422)

    def make_cursor(obj):
        raw = json.dumps(obj).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    check("cursor bad version -> 422",
          search({"cursor": make_cursor(
              {"v": 2, "q": "x", "bound": 1, "last": [1, 1]})}).status_code == 422)
    check("cursor empty q -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "", "bound": 1, "last": [1, 1]})}).status_code == 422)
    check("cursor q too long -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x" * 201, "bound": 1, "last": [1, 1]})}).status_code == 422)
    check("cursor negative bound -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": -1, "last": [1, 1]})}).status_code == 422)
    check("cursor bad last -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1, "last": [1]})}).status_code == 422)
    check("cursor bool in last -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1, "last": [1, True]})}).status_code == 422)
    check("cursor missing field -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1})}).status_code == 422)
    check("truncated valid cursor -> 422",
          search({"cursor": cur1[:-3]}).status_code == 422)

    print("== 游标字段严格校验 ==")
    INT64_MAX = 9223372036854775807
    # v 只能是非布尔整数 1：布尔、浮点、字符串一律非法
    check("cursor v=true -> 422",
          search({"cursor": make_cursor(
              {"v": True, "q": "x", "bound": 1, "last": [1, 1]})}).status_code == 422)
    check("cursor v=1.0 -> 422",
          search({"cursor": make_cursor(
              {"v": 1.0, "q": "x", "bound": 1, "last": [1, 1]})}).status_code == 422)
    check("cursor v='1' -> 422",
          search({"cursor": make_cursor(
              {"v": "1", "q": "x", "bound": 1, "last": [1, 1]})}).status_code == 422)
    # bound 超出 SQLite int64 上限：不得 500（参数绑定溢出），须 422
    check("cursor bound=2^63 -> 422 (no 500)",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": INT64_MAX + 1,
               "last": [0, 0]})}).status_code == 422)
    check("cursor bound huge -> 422 (no 500)",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 10 ** 30,
               "last": [0, 0]})}).status_code == 422)
    check("cursor bound=true -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": True, "last": [1, 1]})}).status_code == 422)
    check("cursor bound=1.0 -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1.0, "last": [1, 1]})}).status_code == 422)
    # last 两项同样只允许 0..2^63-1 的非布尔整数
    check("cursor last[0]=2^63 -> 422 (no 500)",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1,
               "last": [INT64_MAX + 1, 0]})}).status_code == 422)
    check("cursor last[1]=2^63 -> 422 (no 500)",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1,
               "last": [0, INT64_MAX + 1]})}).status_code == 422)
    check("cursor last negative -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1, "last": [-1, 0]})}).status_code == 422)
    check("cursor last float -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1, "last": [0, 1.0]})}).status_code == 422)
    # last[0] > bound：不得返回正常空页，须 422
    check("cursor last[0]>bound -> 422 (not empty page)",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1, "last": [2, 0]})}).status_code == 422)

    print("== 合法续页回归 ==")
    # 手工构造的合法游标（与服务端同格式）必须照常可用：
    # bound 取 int64 上限的边界值合法，last=[0,0] 表示从头翻
    r = search({"cursor": make_cursor(
        {"v": 1, "q": tok, "bound": INT64_MAX, "last": [0, 0]}),
        "page_size": 100})
    check("hand-built cursor bound=2^63-1 -> 200", r.status_code == 200,
          f"got {r.status_code} {r.text[:150]}")
    body = r.json()
    check("hand-built cursor full results",
          [i["id"] for i in body["items"]]
          == ["dup", "m2", "m3", "m4", "m5", "m6", "dup"]
          and body["next_cursor"] is None, body)
    # last[0]==bound 边界合法：从快照末尾批次继续翻
    r = search({"cursor": make_cursor(
        {"v": 1, "q": tok, "bound": INT64_MAX, "last": [INT64_MAX, 0]}),
        "page_size": 100})
    check("hand-built cursor last[0]==bound -> 200 empty",
          r.status_code == 200 and r.json()["items"] == []
          and r.json()["next_cursor"] is None,
          f"got {r.status_code} {r.text[:150]}")
    # 解码服务端签发的 cur1，按原样重编码后结果必须与游标直接续页一致
    raw = json.loads(base64.urlsafe_b64decode(cur1 + "=" * (-len(cur1) % 4)))
    check("server cursor format", set(raw) == {"v", "q", "bound", "last"}
          and raw["v"] == 1 and raw["q"] == tok, raw)
    rebuilt = make_cursor(raw)
    r = search({"cursor": rebuilt, "page_size": 2})
    check("re-encoded cursor same page2",
          r.status_code == 200
          and [i["id"] for i in r.json()["items"]] == ["m3", "m4"],
          f"got {r.status_code} {r.text[:200]}")
    # 续页页大小可变：同一游标用不同 page_size 不报错且从同一位置开始
    r = search({"cursor": rebuilt, "page_size": 3})
    check("resumed page_size variable",
          [i["id"] for i in r.json()["items"]] == ["m3", "m4", "m5"],
          r.text[:200])

    print("== 重复消息统计 ==")
    # 批次 A：alpha×2（同批不同 id）、Alpha（大小写不同）、beta（只一次）、
    # "gamma "（尾随空格）、"gamma"（无空格，只一次）、café（组合字符 é，一次）
    da = [
        {"id": "d1", "message": f"{tok} alpha"},
        {"id": "d2", "message": f"{tok} alpha"},
        {"id": "d3", "message": f"{tok} Alpha"},
        {"id": "d4", "message": f"{tok} beta"},
        {"id": "d5", "message": f"{tok} gamma "},
        {"id": "d6", "message": f"{tok} gamma"},
        {"id": "u1", "message": f"{tok} caf\u00e9"},
    ]
    r = c.post("/batches", content=gz(ndjson(da)))
    check("dup fixture A -> 201", r.status_code == 201, r.text)
    ba = r.json()["batch_id"]

    # 批次 B：alpha（id 与 A.d1 相同，跨批不去重）、Alpha、"gamma "、δ×2、
    # café（分解形式 e+U+0301，与组合形式是不同码点序列）
    dbf = [
        {"id": "d1", "message": f"{tok} alpha"},
        {"id": "x1", "message": f"{tok} Alpha"},
        {"id": "x2", "message": f"{tok} gamma "},
        {"id": "x3", "message": f"{tok} δ"},
        {"id": "x4", "message": f"{tok} δ"},
        {"id": "u2", "message": f"{tok} cafe\u0301"},
    ]
    r = c.post("/batches", content=gz(ndjson(dbf)))
    check("dup fixture B -> 201", r.status_code == 201, r.text)
    bb = r.json()["batch_id"]

    # 批次 C：只有一条唯一消息
    r = c.post("/batches", content=gz(ndjson([{"id": "c1", "message": f"{tok} solo"}])))
    check("dup fixture C -> 201", r.status_code == 201, r.text)
    bc = r.json()["batch_id"]

    def dups(**params):
        return c.get("/logs/duplicates", params=params)

    # occurrences 降序，同数按 message BINARY 升序：'A' < 'g' < 'δ'(U+03B4)
    expected_items = [
        {"message": f"{tok} alpha", "occurrences": 3, "batch_count": 2},
        {"message": f"{tok} Alpha", "occurrences": 2, "batch_count": 2},
        {"message": f"{tok} gamma ", "occurrences": 2, "batch_count": 2},
        {"message": f"{tok} δ", "occurrences": 2, "batch_count": 1},
    ]
    r = dups(from_batch=ba, to_batch=bc, limit=100, offset=0)
    check("dups 200", r.status_code == 200, r.text[:200])
    body = r.json()
    check("dups total groups before pagination", body["total"] == 4, body)
    check("dups items ordered & counted (no id dedup, case/space/codepoint "
          "sensitive)", body["items"] == expected_items, body["items"])
    check("dups echo params",
          body["from_batch"] == ba and body["to_batch"] == bc
          and body["limit"] == 100 and body["offset"] == 0, body)

    # 两端相同：只看单个批次
    r = dups(from_batch=ba, to_batch=ba)
    body = r.json()
    check("single-batch interval", body["total"] == 1 and body["items"] == [
        {"message": f"{tok} alpha", "occurrences": 2, "batch_count": 1}], body)
    r = dups(from_batch=bc, to_batch=bc)
    body = r.json()
    check("no duplicates -> empty items + total 0",
          body["total"] == 0 and body["items"] == [], body)

    # 分页：越界页为空但 total 仍是分组总数
    r = dups(from_batch=ba, to_batch=bc, limit=2, offset=0)
    body = r.json()
    check("dups page1", body["items"] == expected_items[:2]
          and body["total"] == 4, body)
    r = dups(from_batch=ba, to_batch=bc, limit=2, offset=2)
    body = r.json()
    check("dups page2", body["items"] == expected_items[2:]
          and body["total"] == 4, body)
    r = dups(from_batch=ba, to_batch=bc, limit=2, offset=4)
    body = r.json()
    check("dups out-of-range page -> empty, total kept",
          body["items"] == [] and body["total"] == 4, body)

    # 区间由批次 ID 固定：之后上传的新批次不改变相同参数的结果
    snap = dups(from_batch=ba, to_batch=bc, limit=100, offset=0).json()
    dd = [{"id": f"n{i}", "message": f"{tok} alpha"} for i in range(3)]
    r = c.post("/batches", content=gz(ndjson(dd)))
    check("dup fixture D -> 201", r.status_code == 201, r.text)
    bd = r.json()["batch_id"]
    body = dups(from_batch=ba, to_batch=bc, limit=100, offset=0).json()
    check("later upload does not change closed-interval result",
          body == snap, body)
    body = dups(from_batch=ba, to_batch=bd, limit=100, offset=0).json()
    check("extending interval includes new batch",
          body["items"][0]["occurrences"] == 6
          and body["items"][0]["batch_count"] == 3, body["items"][:1])

    # 404 / 422
    check("dups unknown from -> 404",
          dups(from_batch="does-not-exist", to_batch=bc).status_code == 404)
    check("dups unknown to -> 404",
          dups(from_batch=ba, to_batch="does-not-exist").status_code == 404)
    check("dups from after to -> 422",
          dups(from_batch=bc, to_batch=ba).status_code == 422)
    check("dups limit 0 -> 422",
          dups(from_batch=ba, to_batch=bc, limit=0).status_code == 422)
    check("dups limit 101 -> 422",
          dups(from_batch=ba, to_batch=bc, limit=101).status_code == 422)
    check("dups limit 100 ok",
          dups(from_batch=ba, to_batch=bc, limit=100).status_code == 200)
    check("dups negative offset -> 422",
          dups(from_batch=ba, to_batch=bc, offset=-1).status_code == 422)
    check("dups non-int limit -> 422",
          dups(from_batch=ba, to_batch=bc, limit="abc").status_code == 422)
    check("dups missing from_batch -> 422", dups(to_batch=bc).status_code == 422)
    check("dups missing to_batch -> 422", dups(from_batch=ba).status_code == 422)
    check("dups default limit/offset ok",
          dups(from_batch=ba, to_batch=bc).status_code == 200)

    print("== 暂存清理 ==")
    import glob as _glob
    leftovers = _glob.glob(
        os.path.join(os.path.dirname(DB_PATH), "staging", "stage_*.db*"))
    check("no staging files left", leftovers == [], str(leftovers))
    conn = sqlite3.connect(DB_PATH)
    orphan_lines = conn.execute(
        "SELECT count(*) FROM lines WHERE batch_id NOT IN (SELECT id FROM batches)"
    ).fetchone()[0]
    check("no orphan lines", orphan_lines == 0, str(orphan_lines))
    conn.close()

    print(f"\n{passed} passed, {len(failed)} failed: {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
