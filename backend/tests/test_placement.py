"""分片承载引擎的端到端规则验证。

运行（在 backend 目录）：
    .venv/bin/python -I tests/test_placement.py
覆盖：倾斜四类诊断、旧规则兼容、未知值不补零、片段中断续做、坏片段不标成功、
重灌种子不掩盖差异、目录代次 CAS、并发争用证据、校验事实与目录原子提交。
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.placement import (  # noqa: E402
    CAND_ACTIVE,
    CAND_FAILED,
    CAND_REJECTED,
    DIAG_OVERLOAD,
    DIAG_REJECT,
    DIAG_ZERO,
    FRAG_FAILED,
    FRAG_PENDING,
    FRAG_VERIFIED,
    UNKNOWN,
    PlacementService,
    _DIGEST_PENDING,
    _stable_hash,
)
from app.store import store  # noqa: E402

SEGMENT = "segment"
DATASTORE = "datastore"


def make_rule(rule_id: int, *, dataset: str, shard_field: str | None, bucket_count: int) -> dict:
    row = store.find(SEGMENT, rule_id)
    row["目标数据集"] = dataset
    if shard_field is None:
        row.pop("分片字段", None)
    else:
        row["分片字段"] = shard_field
    row["分片数量"] = bucket_count
    row["_placement_samples"] = []
    return row


def ensure_dataset_effective(name: str) -> None:
    rows = store.rows(DATASTORE)
    match = next((row for row in rows if row.get("数据集名称") == name), None)
    if match is not None:
        match["status"] = "已生效"
        return
    rows.append({
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "数据集编号": f"DATA-T-{name}",
        "数据集名称": name,
        "来源表名": f"tbl_{name}",
        "status": "已生效",
    })


class PlacementEngineTest(unittest.TestCase):
    def setUp(self) -> None:
        # 每个用例一个全新引擎，store 只作为规则/数据集来源
        self.svc = PlacementService()
        ensure_dataset_effective("倾斜数据集")
        ensure_dataset_effective("旧规则数据集")
        ensure_dataset_effective("并发数据集")

    # 1. 确定性哈希：同值同桶
    def test_hash_is_deterministic(self) -> None:
        self.assertEqual(_stable_hash("user-7"), _stable_hash("user-7"))
        self.assertNotEqual(_stable_hash("user-7"), _stable_hash("user-8"))

    # 2. 承载面板：零分片 / 单桶过载 / 无法接纳 / 缺列隔离
    def test_diagnosis_skew_panel(self) -> None:
        # 6 桶容纳 5 个有效键，桶数多于键数，鸽巢原理保证至少一个零分片桶
        make_rule(1, dataset="倾斜数据集", shard_field="city", bucket_count=6)
        samples = [
            {"样本编号": "s1", "city": "beijing"},
            {"样本编号": "s2", "city": "shanghai"},
            {"样本编号": "s3", "city": "guangzhou"},
            {"样本编号": "s4", "city": "shenzhen"},
            {"样本编号": "s5", "city": "hangzhou"},
            {"样本编号": "s6", "city": None},  # 缺列值 -> 隔离，不补零
        ]
        self.svc.seed_samples(1, samples)
        panel, error = self.svc.diagnose_rule(1)
        self.assertIsNone(error)
        self.assertEqual(panel["summary"]["sample_total"], 6)

        categories = {item["category"] for item in panel["diagnostics"]}
        # 必有零分片（3 桶 5 个有效样本，哈希落桶不可能铺满且配额极不均）
        self.assertIn(DIAG_ZERO, categories)
        # 每桶面板要素齐备：样本数、哈希区间、目标配额
        for bucket in panel["buckets"]:
            self.assertIn("sample_count", bucket)
            self.assertIn("quota", bucket)
            self.assertIn("text", bucket["hash_range"])
            self.assertIn("lower", bucket["hash_range"])

        # 缺列样本进隔离，未知值留待确认
        quarantined = panel["quarantine"]
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0]["sample_id"], "s6")
        self.assertEqual(quarantined[0]["shard_value"], UNKNOWN)
        self.assertIsNone(quarantined[0]["bucket"])

    # 3. 单桶过载与“哪段布局无法接纳”：极小配额制造外溢
    def test_overload_and_rejected_segment(self) -> None:
        make_rule(2, dataset="倾斜数据集", shard_field="k", bucket_count=2)
        samples = [{"样本编号": f"s{i}", "k": f"key-{i}"} for i in range(8)]
        self.svc.seed_samples(2, samples)
        # 配额极度不均：桶0 只给 1，其余压到桶1，制造过载外溢
        candidate, error, status = self.svc.create_candidate(2, quotas=[1, 2], fragment_size=3)
        self.assertIsNone(error)
        self.assertTrue(candidate["blocked"])
        self.assertTrue(any("无法完整接纳" in reason for reason in candidate["block_reasons"]))
        categories = {item["category"] for item in candidate["diagnostics"]}
        self.assertIn(DIAG_OVERLOAD, categories)
        self.assertIn(DIAG_REJECT, categories)
        rejected = [item for bucket in candidate["buckets"] for item in bucket["rejected"]]
        self.assertTrue(rejected)
        # 被拒样本没有被偷偷放进任何桶
        placed = {sid for bucket in candidate["buckets"] for sid in bucket["sample_ids"]}
        for item in rejected:
            self.assertNotIn(item["sample_id"], placed)
        # 过载候选不能接管
        result, commit_error, commit_status = self.svc.commit_candidate(candidate["candidate_id"])
        self.assertIsNone(result)
        self.assertEqual(commit_status, 422)

    # 4. 旧规则兼容：缺少分片字段身份，全部隔离、不补零、给出旧规则说明
    def test_legacy_rule_missing_identity(self) -> None:
        make_rule(3, dataset="旧规则数据集", shard_field=None, bucket_count=4)
        samples = [{"样本编号": f"old{i}", "anything": i} for i in range(4)]
        self.svc.seed_samples(3, samples)
        candidate, error, status = self.svc.create_candidate(3, fragment_size=2)
        self.assertIsNone(error)
        self.assertTrue(candidate["legacy_notes"])
        self.assertTrue(any("分片字段" in note for note in candidate["legacy_notes"]))
        self.assertEqual(len(candidate["quarantine"]), 4)
        self.assertTrue(all(item["shard_value"] == UNKNOWN for item in candidate["quarantine"]))
        # 无分片字段 -> 没有可校验片段
        self.assertEqual(candidate["summary"]["fragment_total"], 0)

    # 5. 未知值片段不能标成功（422），保持失败态
    def test_unknown_fragment_never_succeeds(self) -> None:
        make_rule(1, dataset="倾斜数据集", shard_field="city", bucket_count=2)
        samples = [{"样本编号": "a", "city": "x"}, {"样本编号": "b", "city": None}]
        self.svc.seed_samples(1, samples)
        candidate, _, _ = self.svc.create_candidate(1, fragment_size=5)
        # 公开视图不暴露事实摘要，直接在引擎内部把片段事实置为“未知待确认”以触发守卫
        internal = self.svc._candidates[candidate["candidate_id"]]
        self.assertTrue(internal["fragments"])
        for fragment in internal["fragments"]:
            fragment["expected_fact"]["digest"] = _DIGEST_PENDING
        view, error, status = self.svc.verify_fragment(candidate["candidate_id"], 0)
        self.assertEqual(status, 422)
        self.assertIsNotNone(error)
        self.assertEqual(view["state"], FRAG_FAILED)

    # 6. 中断续做：从首个未校验片段继续，已校验片段不重复
    def test_resume_from_first_unverified_fragment(self) -> None:
        make_rule(1, dataset="倾斜数据集", shard_field="k", bucket_count=2)
        samples = [{"样本编号": f"s{i}", "k": f"k-{i}"} for i in range(10)]
        self.svc.seed_samples(1, samples)
        candidate, _, _ = self.svc.create_candidate(1, fragment_size=2)
        cid = candidate["candidate_id"]
        total = len(candidate["fragments"])
        self.assertGreaterEqual(total, 4)
        # 先校验片段 0，模拟中断
        view, error, status = self.svc.verify_fragment(cid, 0)
        self.assertIsNone(error)
        self.assertEqual(view["state"], FRAG_VERIFIED)
        # 续做必须从片段 1 开始
        result, error, status = self.svc.resume_verification(cid)
        self.assertIsNone(error)
        self.assertEqual(result["resumed_from"], 1)
        self.assertTrue(result["done"])
        self.assertTrue(all(f["state"] == FRAG_VERIFIED for f in result["fragments"]))

    # 7. 重灌种子不掩盖差异：坏片段失败并阻塞接管
    def test_reseed_does_not_mask_bad_fragment(self) -> None:
        make_rule(1, dataset="倾斜数据集", shard_field="k", bucket_count=2)
        self.svc.seed_samples(1, [{"样本编号": f"s{i}", "k": f"orig-{i}"} for i in range(6)])
        candidate, _, _ = self.svc.create_candidate(1, fragment_size=2)
        cid = candidate["candidate_id"]
        # 再平衡中断期间重新灌入了不同种子
        self.svc.seed_samples(1, [{"样本编号": f"s{i}", "k": f"tampered-{i}"} for i in range(6)])
        result, _, _ = self.svc.resume_verification(cid)
        self.assertFalse(result["done"])
        self.assertEqual(result["blocking_fragment"]["state"], FRAG_FAILED)
        self.assertIn("事实不一致", result["blocking_fragment"]["fail_reason"])
        # 再次续做：片段0已是失败态，续做从片段1开始并同样失败，坏片段永远不会被标成功
        again, _, _ = self.svc.resume_verification(cid)
        self.assertFalse(again["done"])
        self.assertEqual(again["resumed_from"], 1)
        self.assertEqual(again["blocking_fragment"]["state"], FRAG_FAILED)
        self.assertEqual(self.svc.get_candidate(cid)["state"], CAND_FAILED)
        # 目录仍无代次，继续指向原桶
        catalog = self.svc.get_catalog("倾斜数据集")
        self.assertEqual(catalog["generation"], 0)

    # 8. 原子接管：校验全通过后目录代次才推进，事实随目录一起落定
    def test_commit_advances_catalog_atomically(self) -> None:
        make_rule(1, dataset="倾斜数据集", shard_field="k", bucket_count=2)
        self.svc.seed_samples(1, [{"样本编号": f"s{i}", "k": f"k-{i}"} for i in range(6)])
        candidate, _, _ = self.svc.create_candidate(1, fragment_size=2)
        cid = candidate["candidate_id"]
        # 提交前目录为空
        self.assertEqual(self.svc.get_catalog("倾斜数据集")["generation"], 0)
        # 未校验完不允许提交
        _, error, status = self.svc.commit_candidate(cid)
        self.assertEqual(status, 409)
        self.svc.resume_verification(cid)
        committed, error, status = self.svc.commit_candidate(cid)
        self.assertIsNone(error)
        self.assertEqual(committed["generation"], 1)
        self.assertEqual(self.svc.get_catalog(candidate["dataset"])["generation"], 1)
        self.assertEqual(self.svc.get_candidate(cid)["state"], CAND_ACTIVE)
        # 落定的目录携带校验事实
        active = self.svc.get_catalog("倾斜数据集")["active"]
        self.assertTrue(all(f["state"] == FRAG_VERIFIED for f in active["fragments"]))

    # 9. 并发恢复：过期代次候选被拒，拒绝方可查看争用证据
    def test_concurrent_recovery_cas_and_contention(self) -> None:
        make_rule(2, dataset="并发数据集", shard_field="k", bucket_count=2)
        ensure_dataset_effective("并发数据集")
        self.svc.seed_samples(2, [{"样本编号": f"s{i}", "k": f"k-{i}"} for i in range(4)])
        # 两人各自基于代次 0 建立候选
        first, _, _ = self.svc.create_candidate(2, fragment_size=2, operator="alice")
        second, _, _ = self.svc.create_candidate(2, fragment_size=2, operator="bob")
        self.svc.resume_verification(first["candidate_id"], operator="alice")
        self.svc.resume_verification(second["candidate_id"], operator="bob")
        # alice 先接管，目录推进到代次 1
        self.svc.commit_candidate(first["candidate_id"], operator="alice")
        # bob 显式携带过期代次 0 提交 -> 409
        result, error, status = self.svc.commit_candidate(
            second["candidate_id"], base_generation=0, operator="bob"
        )
        self.assertIsNone(result)
        self.assertEqual(status, 409)
        self.assertIn("目录代次冲突", error)
        self.assertEqual(self.svc.get_candidate(second["candidate_id"])["state"], CAND_REJECTED)
        # 争用证据可查
        contention = self.svc.list_contention("并发数据集")
        self.assertGreaterEqual(contention["total"], 1)
        event = contention["items"][-1]
        self.assertEqual(event["supplied"], 0)
        self.assertEqual(event["effective"], 1)
        self.assertEqual(event["operator"], "bob")

    # 10. 建立候选时即校验代次：过期 base_generation 当场拒绝并留证
    def test_stale_generation_rejected_at_creation(self) -> None:
        make_rule(2, dataset="并发数据集", shard_field="k", bucket_count=2)
        self.svc.seed_samples(2, [{"样本编号": f"s{i}", "k": f"k-{i}"} for i in range(4)])
        first, _, _ = self.svc.create_candidate(2, operator="alice")
        self.svc.resume_verification(first["candidate_id"])
        self.svc.commit_candidate(first["candidate_id"])
        candidate, error, status = self.svc.create_candidate(2, base_generation=0, operator="bob")
        self.assertIsNone(candidate)
        self.assertEqual(status, 409)
        self.assertIn("代次", error)
        self.assertEqual(self.svc.list_contention("并发数据集")["items"][-1]["action"], "建立候选")

    # 11. 数据集未生效：目录代次不允许建立，跨模块联动生效
    def test_dataset_not_effective_blocks_candidate(self) -> None:
        make_rule(3, dataset="草稿数据集", shard_field="k", bucket_count=2)
        store.rows(DATASTORE).append({
            "id": 999,
            "数据集编号": "DATA-DRAFT",
            "数据集名称": "草稿数据集",
            "来源表名": "tbl_draft",
            "status": "待审核",
        })
        self.svc.seed_samples(3, [{"样本编号": "x", "k": "v"}])
        candidate, error, status = self.svc.create_candidate(3)
        self.assertIsNone(candidate)
        self.assertEqual(status, 409)
        self.assertIn("未生效前目录继续指向原桶", error)

    # 12. 隔离未知值：未确认不能接管，显式确认后连同待确认清单一起落定（不补零）
    def test_quarantine_requires_explicit_acknowledgement(self) -> None:
        make_rule(1, dataset="倾斜数据集", shard_field="city", bucket_count=3)
        self.svc.seed_samples(1, [
            {"样本编号": "ok", "city": "beijing"},
            {"样本编号": "unknown", "city": None},
        ])
        candidate, _, _ = self.svc.create_candidate(1, quotas=[10, 10, 10])
        cid = candidate["candidate_id"]
        self.assertFalse(candidate["blocked"])
        self.assertTrue(candidate["warnings"])
        self.svc.resume_verification(cid)
        # 未显式确认隔离区 -> 拒绝接管
        result, error, status = self.svc.commit_candidate(cid)
        self.assertIsNone(result)
        self.assertEqual(status, 409)
        self.assertIn("待确认", error)
        # 显式确认后接管，待确认清单随目录落定，值保持“待确认”而非补零
        committed, error, status = self.svc.commit_candidate(cid, confirm_quarantine=True)
        self.assertIsNone(error)
        active = self.svc.get_catalog("倾斜数据集")["active"]
        self.assertEqual(len(active["quarantine"]), 1)
        self.assertEqual(active["quarantine"][0]["shard_value"], UNKNOWN)

    # 13. 修订后配额合理：候选可通过并接管，桶分布更均衡
    def test_revised_quotas_allow_takeover(self) -> None:
        make_rule(1, dataset="倾斜数据集", shard_field="k", bucket_count=2)
        samples = [{"样本编号": f"s{i}", "k": f"key-{i}"} for i in range(8)]
        self.svc.seed_samples(1, samples)
        # 先给坏配额制造过载
        bad, _, _ = self.svc.create_candidate(1, quotas=[1, 1], fragment_size=4)
        self.assertTrue(bad["blocked"])
        # 修订为充足配额后重建候选
        good, error, status = self.svc.create_candidate(1, quotas=[8, 8], fragment_size=4)
        self.assertIsNone(error)
        self.assertFalse(good["blocked"])
        self.svc.resume_verification(good["candidate_id"])
        committed, error, status = self.svc.commit_candidate(good["candidate_id"])
        self.assertIsNone(error)
        self.assertEqual(committed["generation"], 1)
        self.assertEqual(committed["active"]["summary"]["placed_total"], 8)
        self.assertEqual(committed["active"]["summary"]["rejected_total"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
