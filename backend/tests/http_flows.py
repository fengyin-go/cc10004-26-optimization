"""HTTP 端到端演练：沿 router -> service -> store 调用链验证承载与再平衡。

运行（在 backend 目录，先起服务）：
    .venv/bin/python -I tests/http_flows.py
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE = "http://127.0.0.1:8011/api"


def call(method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def check(label: str, condition: bool, detail: str = "") -> None:
    print(f"[{'PASS' if condition else 'FAIL'}] {label} {detail}")
    if not condition:
        raise SystemExit(f"断言失败：{label} {detail}")


# 1. 跨模块关联：让分片规则3的目标数据集在数据集管理中存在并生效（全程走真实 HTTP 链路）
seg_status, seg = call("GET", "/segment/3")
check("读取分片规则3", seg_status == 200)
dataset_name = seg["目标数据集"]
status, created = call("POST", "/datastore", {"values": {
    "数据集编号": "DATA-LINK-3",
    "数据集名称": dataset_name,
    "来源表名": "tbl_link_3",
}})
check("登记关联数据集", status == 200 and created["ok"], str(created))
new_id = created["entry"]["id"]
call("POST", f"/datastore/{new_id}/actions", {"values": {"action": "提交审核"}})
status, body = call("POST", f"/datastore/{new_id}/actions", {"values": {"action": "生效发布"}})
check("关联数据集生效发布", status == 200 and body["entry"]["status"] == "已生效", str(body))
# 规则3 的分片字段对齐种子里的 city、桶数对齐后续配额（通过服务所在进程的接口无可写入口，
# 故种子字段沿用规则声明，诊断与候选均以规则当前声明为准）

# 2. 灌种子：分片字段沿用规则3声明的字段名，含一条缺列样本（未知值不补零）
shard_field = seg["分片字段"]
bucket_count = seg["分片数量"]
samples = [{"样本编号": f"e{i}", shard_field: f"city-{i % 3}"} for i in range(7)]
samples.append({"样本编号": "e-bad", shard_field: None})
status, body = call("POST", "/placement/rules/3/seed", {"values": {"samples": samples}})
check("灌入样本种子", status == 200 and body["entry"]["loaded"] == 8, str(status))

# 3. 诊断面板：桶要素、隔离未知值
status, panel = call("GET", "/placement/rules/3/diagnosis")
check("诊断面板返回", status == 200)
check("每桶含样本数/哈希区间/配额",
      all({"sample_count", "quota", "hash_range"} <= set(b) for b in panel["buckets"]))
check("缺列样本隔离且不补零",
      any(q["sample_id"] == "e-bad" and q["shard_value"] == "待确认" for q in panel["quarantine"]))

# 4. 建立候选（基于代次 0，给足配额避免布局拒收；隔离区未知值待确认）
status, body = call("POST", "/placement/candidates",
                    {"values": {"rule_id": 3, "quotas": [50] * bucket_count,
                                "fragment_size": 2, "operator": "alice"}})
check("建立候选", status == 200, str(body))
cid = body["entry"]["candidate_id"]
check("候选基于代次0", body["entry"]["base_generation"] == 0)
check("候选无布局拒收（仅隔离告警）", not body["entry"]["blocked"])

# 5. 过期代次建候选 -> 409（此时目录尚未推进，先制造已提交候选）
status, body = call("POST", "/placement/candidates/{}/resume".format(cid),
                    {"values": {"operator": "alice"}})
check("续做校验完成", status == 200 and body["entry"]["done"], str(body))
# 隔离区有待确认样本时，未显式确认不能接管
status, body = call("POST", f"/placement/candidates/{cid}/commit",
                    {"values": {"operator": "alice"}})
check("未确认隔离区拒绝接管409", status == 409 and "待确认" in body["detail"], str(body))
# 显式确认承接未知值清单后接管，目录推进到代次1
status, body = call("POST", f"/placement/candidates/{cid}/commit",
                    {"values": {"operator": "alice", "confirm_quarantine": True}})
check("首个候选接管，目录推进代次1", status == 200 and body["entry"]["generation"] == 1, str(body))
status, catalog = call("GET", f"/placement/catalog?dataset={urllib.parse.quote(dataset_name)}")
check("待确认清单随目录落定且未补零",
      any(q["shard_value"] == "待确认" for q in catalog["active"]["quarantine"]))

status, body = call("POST", "/placement/candidates",
                    {"values": {"rule_id": 3, "base_generation": 0, "operator": "bob"}})
check("过期代次建候选被拒409", status == 409 and "代次" in body["detail"], str(body))

# 6. 并发争用证据可查
status, body = call("GET", f"/placement/contention?dataset={urllib.parse.quote(dataset_name)}")
check("拒绝方可查看争用证据", status == 200 and body["total"] >= 1, str(body.get("total")))

BUCKETS = bucket_count

# 7. 坏配额候选：过载、无法接纳、拒绝接管（422）
status, body = call("POST", "/placement/candidates",
                    {"values": {"rule_id": 3, "quotas": [1] * BUCKETS, "fragment_size": 2}})
check("过载候选建立并标记blocked", status == 200 and body["entry"]["blocked"])
bad_cid = body["entry"]["candidate_id"]
check("诊断含单桶过载/无法接纳",
      {"单桶过载", "无法接纳"} & {d["category"] for d in body["entry"]["diagnostics"]})
status, body = call("POST", f"/placement/candidates/{bad_cid}/commit", {"values": {}})
check("过载候选接管被拒422", status == 422 and "无法接纳" in body["detail"], str(body))

# 8. 目录在候选未校验完时继续指向原桶（代次1），修订后成功接管到代次2
status, body = call("POST", "/placement/candidates",
                    {"values": {"rule_id": 3, "quotas": [50] * BUCKETS, "fragment_size": 2}})
good_cid = body["entry"]["candidate_id"]
status, catalog = call("GET", f"/placement/catalog?dataset={urllib.parse.quote(dataset_name)}")
check("候选未校验完目录仍指原桶代次1", catalog["generation"] == 1, str(catalog["generation"]))
call("POST", f"/placement/candidates/{good_cid}/resume", {"values": {}})
status, body = call("POST", f"/placement/candidates/{good_cid}/commit",
                    {"values": {"confirm_quarantine": True}})
check("修订候选接管到代次2", status == 200 and body["entry"]["generation"] == 2, str(body))

# 9. 重灌种子后片段事实差异暴露，不被掩盖
status, body = call("POST", "/placement/candidates",
                    {"values": {"rule_id": 3, "quotas": [50] * BUCKETS, "fragment_size": 2}})
tamper_cid = body["entry"]["candidate_id"]
call("POST", "/placement/rules/3/seed",
     {"values": {"samples": [{"样本编号": f"e{i}", shard_field: f"changed-{i}"} for i in range(8)]}})
status, body = call("POST", f"/placement/candidates/{tamper_cid}/resume", {"values": {}})
check("重灌种子后坏片段被定位且未标成功",
      status == 200 and not body["entry"]["done"]
      and body["entry"]["blocking_fragment"]["state"] == "校验失败", str(body.get("entry", {}).get("blocking_fragment")))
status, body = call("POST", f"/placement/candidates/{tamper_cid}/commit", {"values": {}})
check("含坏片段候选无法接管", status in (409, 422))

print("\n全部 HTTP 演练通过")
