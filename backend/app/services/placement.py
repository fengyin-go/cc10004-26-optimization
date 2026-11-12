"""分片承载引擎：倾斜诊断、候选布局修订、再平衡校验与目录代次接管。

设计要点（对应业务约束）：
- 承载面板按桶给出样本数、哈希区间、目标配额，并区分零分片/缺列/全空样本/单桶过载，
  明确指出哪段布局无法接纳样本（容量不足被拒）。
- 候选布局校验未全部通过前，目录（catalog）继续指向原桶；只有校验事实与目录提交原子
  落定后才切换到候选桶。
- 再平衡以分片（fragment）为校验单元并记录进度，中断后从首个未校验片段续做；坏片段
  永远不能被标作成功，重新灌入种子只改变后续诊断，不会改写既存片段事实，差异会暴露为
  校验失败而不是被掩盖。
- 兼容缺少分片字段身份的旧规则：字段缺失/空值样本进隔离区，未知值留待确认，绝不补零。
- 并发恢复采用“目录代次 + 锁 + CAS”：只接纳基于有效代次的候选，拒绝方可以查看争用证据。
"""
from __future__ import annotations

import hashlib
import threading
from typing import Any

from app.store import store

MODULE_SEGMENT = "segment"
MODULE_DATASTORE = "datastore"
DATASET_FIELD = "目标数据集"
SHARD_FIELD_NAME = "分片字段"
BUCKET_COUNT_FIELD = "分片数量"
LEGACY_IDENTITY_KEYS = ["规则编号", "id"]

# 片段状态机
FRAG_PENDING = "未校验"
FRAG_VERIFIED = "已校验"
FRAG_FAILED = "校验失败"

# 候选状态机
CAND_BUILDING = "待校验"
CAND_VERIFIED = "校验通过"
CAND_FAILED = "校验失败"
CAND_ACTIVE = "已接管"
CAND_REJECTED = "已拒绝"

# 隔离/未知标记：未知值留待确认，绝不补零
UNKNOWN = "待确认"

# 诊断分类
DIAG_ZERO = "零分片"
DIAG_EMPTY = "全空样本"
DIAG_OVERLOAD = "单桶过载"
DIAG_REJECT = "无法接纳"
DIAG_MISSING_COLUMN = "缺列"
DIAG_LEGACY = "旧规则兼容"

# 片段事实摘要在缺身份时使用的占位（不参与哈希、不补零）
_DIGEST_PENDING = "pending:未知值待确认"


def _stable_hash(value: str) -> int:
    """确定性哈希：同值永远落同桶，跨进程一致，不依赖内置 hash 随机化。"""
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def _bucket_range(bucket_index: int, bucket_count: int) -> dict[str, Any]:
    """桶对应的哈希区间：把 32 位哈希空间按桶数均分，展示闭开区间。"""
    span = 2**32 // bucket_count
    lower = bucket_index * span
    upper = 2**32 if bucket_index == bucket_count - 1 else (bucket_index + 1) * span
    return {"lower": lower, "upper": upper, "text": f"[0x{lower:08X}, 0x{upper:08X})"}


def _parse_bucket_count(raw: Any) -> int:
    """解析桶数；旧规则可能给出非数字/空值，这里不猜测、不补默认零。"""
    if isinstance(raw, bool):  # bool 是 int 的子类，先排除
        raise ValueError("分片数量不是合法整数")
    if isinstance(raw, int):
        count = raw
    else:
        text = str(raw or "").strip()
        if not text:
            raise ValueError("分片数量缺失，旧规则未声明桶数")
        try:
            count = int(text)
        except ValueError as exc:
            raise ValueError(f"分片数量「{text}」不是合法整数") from exc
    if count <= 0:
        raise ValueError("分片数量必须为正整数")
    return count


def _sample_identity(sample: dict[str, Any], index: int) -> str:
    """样本身份：优先用业务编号，没有就用行号；身份不完整时显式标注。"""
    for key in ("样本编号", "id", "编号"):
        value = sample.get(key)
        if value not in (None, ""):
            return str(value)
    return f"行{index + 1}"


def _fact_digest(keys: list[str]) -> str:
    """片段事实摘要：对片段内样本的分片键集合做确定性哈希。"""
    if not keys:
        return _DIGEST_PENDING
    joined = "\n".join(keys)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


class PlacementService:
    """承载/再平衡状态机，全部状态保存在内存，通过一把锁串行化裁决。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # 目录：数据集 -> 当前生效布局（代次单调递增，校验未完成不切换）
        self._catalog: dict[str, dict[str, Any]] = {}
        # 候选：candidate_id -> 候选布局
        self._candidates: dict[int, dict[str, Any]] = {}
        # 再平衡会话：candidate_id -> 恢复上下文（续做片段等）
        self._sessions: dict[int, dict[str, Any]] = {}
        # 争用证据：被 CAS 拒绝的裁决全部留痕，供拒绝方查看
        self._contention: list[dict[str, Any]] = []
        self._candidate_seq = 0
        self._contention_seq = 0

    # ------------------------------------------------------------------
    # 目录与争用的只读视图
    # ------------------------------------------------------------------
    def get_catalog(self, dataset: str | None = None) -> dict[str, Any]:
        with self._lock:
            if dataset is not None:
                entry = self._catalog.get(dataset)
                if entry is None:
                    return {"dataset": dataset, "generation": 0, "active": None, "note": "目录尚无生效布局，仍指向初始空承载"}
                return {"dataset": dataset, "generation": entry["generation"], "active": self._public_layout(entry)}
            items = [
                {"dataset": name, "generation": entry["generation"], "active": self._public_layout(entry)}
                for name, entry in sorted(self._catalog.items())
            ]
            return {"datasets": items, "total": len(items)}

    def list_contention(self, dataset: str | None = None) -> dict[str, Any]:
        with self._lock:
            events = list(self._contention)
        if dataset is not None:
            events = [event for event in events if event.get("dataset") == dataset]
        return {"total": len(events), "items": events}

    def list_candidates(self, dataset: str | None = None) -> dict[str, Any]:
        with self._lock:
            candidates = [self._public_candidate(candidate) for candidate in self._candidates.values()]
        if dataset is not None:
            candidates = [candidate for candidate in candidates if candidate["dataset"] == dataset]
        candidates.sort(key=lambda item: item["candidate_id"])
        return {"total": len(candidates), "items": candidates}

    def get_candidate(self, candidate_id: int) -> dict[str, Any] | None:
        with self._lock:
            candidate = self._candidates.get(candidate_id)
            return self._public_candidate(candidate) if candidate else None

    # ------------------------------------------------------------------
    # 规则/数据集联动校验（跨模块：segment × datastore）
    # ------------------------------------------------------------------
    def _resolve_rule(self, rule_id: int) -> tuple[dict[str, Any] | None, str | None]:
        rule = store.find(MODULE_SEGMENT, rule_id)
        if rule is None:
            return None, f"分片规则 {rule_id} 不存在或已归档"
        return rule, None

    def _check_dataset_effective(self, dataset: str) -> str | None:
        """目录代次只承认「已生效」的数据集定义；未生效时候选不允许建立。"""
        rows = store.rows(MODULE_DATASTORE)
        match = next((row for row in rows if str(row.get("数据集名称", "")) == dataset), None)
        if match is None:
            return f"目标数据集「{dataset}」在数据集管理中没有定义，目录无法建立代次"
        if match.get("status") != "已生效":
            return f"目标数据集「{dataset}」当前状态为「{match.get('status')}」，未生效前目录继续指向原桶"
        return None

    def diagnose_rule(self, rule_id: int) -> tuple[dict[str, Any] | None, str | None]:
        """只读诊断：基于当前种子给出桶级承载面板，不产生候选、不动目录。"""
        with self._lock:
            rule, error = self._resolve_rule(rule_id)
            if error:
                return None, error
            legacy_notes = self._legacy_notes(rule)
            try:
                bucket_count = _parse_bucket_count(rule.get(BUCKET_COUNT_FIELD))
            except ValueError as exc:
                return {
                    "rule_id": rule_id,
                    "dataset": rule.get(DATASET_FIELD),
                    "legacy_notes": legacy_notes,
                    "blocked": True,
                    "reason": str(exc),
                    "buckets": [],
                    "diagnostics": [self._diag(DIAG_LEGACY, str(exc), [])],
                }, None
            samples = self._load_samples(rule)
            layout = self._build_layout(rule, bucket_count, samples, base_generation=0, base_label="诊断快照")
            layout["rule_id"] = rule_id
            layout["legacy_notes"] = legacy_notes
            return layout, None

    # ------------------------------------------------------------------
    # 候选修订：建立再平衡候选（含乐观代次校验）
    # ------------------------------------------------------------------
    def create_candidate(
        self,
        rule_id: int,
        *,
        quotas: list[int] | None = None,
        fragment_size: int = 5,
        base_generation: int | None = None,
        operator: str = "anonymous",
    ) -> tuple[dict[str, Any] | None, str | None, int | None]:
        """建立候选布局。返回 (候选, 错误说明, HTTP状态码)。"""
        with self._lock:
            rule, error = self._resolve_rule(rule_id)
            if error:
                return None, error, 404

            dataset = str(rule.get(DATASET_FIELD) or "").strip()
            if not dataset:
                return None, "规则缺少目标数据集，无法关联目录代次", 400

            dataset_error = self._check_dataset_effective(dataset)
            if dataset_error:
                return None, dataset_error, 409

            current = self._catalog.get(dataset)
            current_generation = 0 if current is None else int(current["generation"])
            if base_generation is not None and int(base_generation) != current_generation:
                self._record_contention(
                    dataset=dataset,
                    action="建立候选",
                    operator=operator,
                    supplied=int(base_generation),
                    effective=current_generation,
                    detail="候选基于过期目录代次建立，已拒绝",
                )
                return None, (
                    f"目录代次已前进到 {current_generation}，候选依据的代次 {base_generation} 已失效，"
                    "请基于当前代次重新恢复"
                ), 409

            try:
                bucket_count = _parse_bucket_count(rule.get(BUCKET_COUNT_FIELD))
            except ValueError as exc:
                return None, str(exc), 400

            if fragment_size <= 0:
                return None, "片段容量必须为正整数", 400

            samples = self._load_samples(rule)
            self._candidate_seq += 1
            candidate_id = self._candidate_seq
            layout = self._build_layout(
                rule,
                bucket_count,
                samples,
                base_generation=current_generation,
                base_label="原桶布局",
                quotas=quotas,
                fragment_size=fragment_size,
            )
            candidate = {
                "candidate_id": candidate_id,
                "rule_id": rule_id,
                "dataset": dataset,
                "operator": operator,
                "state": CAND_BUILDING,
                "base_generation": current_generation,
                "target_generation": current_generation + 1,
                "created_at_generation": current_generation,
                "legacy_notes": self._legacy_notes(rule),
                "blocked": layout["blocked"],
                "block_reasons": layout["block_reasons"],
                "warnings": layout["warnings"],
                "bucket_count": layout["bucket_count"],
                "buckets": layout["buckets"],
                "fragments": layout["fragments"],
                "diagnostics": layout["diagnostics"],
                "quarantine": layout["quarantine"],
                "summary": layout["summary"],
                "sample_snapshot": layout["sample_snapshot"],
                "shard_field": layout["shard_field"],
            }
            self._candidates[candidate_id] = candidate
            self._sessions[candidate_id] = {
                "next_fragment": 0,
                "resume_count": 0,
                "last_stopped_at": None,
            }
            return self._public_candidate(candidate), None, None

    # ------------------------------------------------------------------
    # 片段校验：再平衡的事实核对单元
    # ------------------------------------------------------------------
    def verify_fragment(
        self,
        candidate_id: int,
        fragment_index: int,
        *,
        operator: str = "anonymous",
    ) -> tuple[dict[str, Any] | None, str | None, int | None]:
        with self._lock:
            candidate = self._candidates.get(candidate_id)
            if candidate is None:
                return None, f"候选 {candidate_id} 不存在", 404
            fragments = candidate["fragments"]
            if fragment_index < 0 or fragment_index >= len(fragments):
                return None, f"片段序号 {fragment_index} 越界，候选共有 {len(fragments)} 个片段", 400

            fragment = fragments[fragment_index]
            if fragment["state"] == FRAG_VERIFIED:
                return self._fragment_view(candidate, fragment), None, None

            # 从当前种子重新计算事实；种子重灌不会改写 expected_fact，差异在此暴露
            actual_keys = self._current_fragment_keys(candidate, fragment)
            actual_digest = _fact_digest(actual_keys)
            actual_count = len(actual_keys)
            fragment["last_actual_digest"] = actual_digest
            fragment["last_actual_count"] = actual_count

            expected_digest = fragment["expected_fact"]["digest"]
            if expected_digest == _DIGEST_PENDING:
                # 含未知身份的片段不能被当作成功：未知值必须先确认
                fragment["state"] = FRAG_FAILED
                fragment["fail_reason"] = "片段包含未知分片值，待确认后才能校验，禁止补零放行"
                candidate["state"] = CAND_FAILED
                return self._fragment_view(candidate, fragment), fragment["fail_reason"], 422

            if actual_count != fragment["expected_fact"]["sample_count"] or actual_digest != expected_digest:
                fragment["state"] = FRAG_FAILED
                fragment["fail_reason"] = (
                    f"片段事实不一致：期望 {fragment['expected_fact']['sample_count']} 条/"
                    f"{expected_digest[:12]}，实际 {actual_count} 条/{actual_digest[:12]}"
                )
                candidate["state"] = CAND_FAILED
                return self._fragment_view(candidate, fragment), fragment["fail_reason"], 422

            fragment["state"] = FRAG_VERIFIED
            fragment["verified_by"] = operator
            fragment["fail_reason"] = None
            self._refresh_candidate_state(candidate)
            return self._fragment_view(candidate, fragment), None, None

    def resume_verification(
        self,
        candidate_id: int,
        *,
        operator: str = "anonymous",
    ) -> tuple[dict[str, Any] | None, str | None, int | None]:
        """从首个未校验片段续做；遇到坏片段立即停下，绝不标成功、不跳过。"""
        with self._lock:
            candidate = self._candidates.get(candidate_id)
            if candidate is None:
                return None, f"候选 {candidate_id} 不存在", 404
            session = self._sessions[candidate_id]
            fragments = candidate["fragments"]

            start = next(
                (idx for idx, fragment in enumerate(fragments) if fragment["state"] == FRAG_PENDING),
                None,
            )
            if start is None:
                session["last_stopped_at"] = None
                return {
                    "candidate_id": candidate_id,
                    "resumed_from": None,
                    "stopped_at": None,
                    "done": True,
                    "candidate_state": candidate["state"],
                    "fragments": [self._fragment_view(candidate, fragment) for fragment in fragments],
                }, None, None

            session["resume_count"] += 1
            stopped_at: int | None = None
            for idx in range(start, len(fragments)):
                fragment = fragments[idx]
                if fragment["state"] != FRAG_PENDING:
                    continue
                view, error, status = self.verify_fragment(candidate_id, idx, operator=operator)
                if status is not None:
                    stopped_at = idx
                    session["last_stopped_at"] = idx
                    break
            session["last_stopped_at"] = stopped_at
            self._refresh_candidate_state(candidate)
            return {
                "candidate_id": candidate_id,
                "resumed_from": start,
                "stopped_at": stopped_at,
                "done": stopped_at is None,
                "blocking_fragment": self._fragment_view(candidate, fragments[stopped_at]) if stopped_at is not None else None,
                "candidate_state": candidate["state"],
                "fragments": [self._fragment_view(candidate, fragment) for fragment in fragments],
            }, None, None

    # ------------------------------------------------------------------
    # 承载接管：校验事实与目录提交原子落定
    # ------------------------------------------------------------------
    def commit_candidate(
        self,
        candidate_id: int,
        *,
        base_generation: int | None = None,
        confirm_quarantine: bool = False,
        operator: str = "anonymous",
    ) -> tuple[dict[str, Any] | None, str | None, int | None]:
        with self._lock:
            candidate = self._candidates.get(candidate_id)
            if candidate is None:
                return None, f"候选 {candidate_id} 不存在", 404

            dataset = candidate["dataset"]
            current = self._catalog.get(dataset)
            current_generation = 0 if current is None else int(current["generation"])
            supplied = candidate["base_generation"] if base_generation is None else int(base_generation)

            if supplied != current_generation:
                self._record_contention(
                    dataset=dataset,
                    action="承载接管",
                    operator=operator,
                    supplied=supplied,
                    effective=current_generation,
                    detail=f"候选 {candidate_id} 基于代次 {supplied} 提交，当前代次 {current_generation}，已拒绝",
                    candidate_id=candidate_id,
                )
                candidate["state"] = CAND_REJECTED
                return None, (
                    f"目录代次冲突：候选基于代次 {supplied}，当前已为 {current_generation}，"
                    "接管被拒绝，可在争用证据中查看先到裁决"
                ), 409

            # 先裁决布局本身能否接纳：存在无法接纳缺陷的候选永远不能接管，
            # 与片段校验进度无关，避免坏布局靠“补齐校验”蒙混过关。
            if candidate["blocked"]:
                return None, "候选存在无法接纳的布局缺陷，不能接管承载：" + "；".join(candidate["block_reasons"]), 422

            # 隔离区的未知值不补零、不静默吞掉：接管方必须显式确认承接这份待确认清单
            if candidate["quarantine"] and not confirm_quarantine:
                return None, (
                    f"候选隔离区仍有 {len(candidate['quarantine'])} 个待确认样本（未知值未补零），"
                    "请在确认承载策略后携带 confirm_quarantine=true 再接管"
                ), 409

            fragments = candidate["fragments"]
            unverified = [idx for idx, fragment in enumerate(fragments) if fragment["state"] != FRAG_VERIFIED]
            if unverified:
                return None, (
                    f"候选尚有 {len(unverified)} 个片段未校验通过（首个：片段 {unverified[0]}），"
                    "目录在候选校验完成前继续指向原桶"
                ), 409

            # 原子落定：在同一把锁内同时写死校验事实快照与新目录代次
            committed_layout = {
                "generation": candidate["target_generation"],
                "rule_id": candidate["rule_id"],
                "dataset": dataset,
                "shard_field": candidate["shard_field"],
                "bucket_count": len(candidate["buckets"]),
                "buckets": candidate["buckets"],
                "fragments": candidate["fragments"],
                "quarantine": candidate["quarantine"],
                "summary": candidate["summary"],
                "committed_by": operator,
                "from_candidate": candidate_id,
            }
            self._catalog[dataset] = committed_layout
            candidate["state"] = CAND_ACTIVE
            candidate["committed_generation"] = committed_layout["generation"]
            return {
                "candidate_id": candidate_id,
                "dataset": dataset,
                "generation": committed_layout["generation"],
                "state": candidate["state"],
                "active": self._public_layout(committed_layout),
            }, None, None

    # ------------------------------------------------------------------
    # 种子管理：重灌不掩盖差异，只影响后续诊断/事实重算
    # ------------------------------------------------------------------
    def seed_samples(self, rule_id: int, samples: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, str | None]:
        with self._lock:
            rule, error = self._resolve_rule(rule_id)
            if error:
                return None, error
            if not isinstance(samples, list):
                return None, "samples 必须是样本对象数组"
            rule["_placement_samples"] = [dict(sample) for sample in samples]
            # 既存候选的 expected_fact 保持不变；重灌后其片段会在重算时如实暴露差异
            affected = [
                cid for cid, candidate in self._candidates.items() if candidate["rule_id"] == rule_id
            ]
            return {
                "rule_id": rule_id,
                "loaded": len(samples),
                "affected_candidates": affected,
                "note": "种子已更新；既存候选事实不会被改写，重算校验将如实反映差异",
            }, None

    # ------------------------------------------------------------------
    # 核心布局构建
    # ------------------------------------------------------------------
    def _build_layout(
        self,
        rule: dict[str, Any],
        bucket_count: int,
        samples: list[dict[str, Any]],
        *,
        base_generation: int,
        base_label: str,
        quotas: list[int] | None = None,
        fragment_size: int = 5,
    ) -> dict[str, Any]:
        shard_field = rule.get(SHARD_FIELD_NAME)
        shard_field = str(shard_field).strip() if shard_field not in (None, "") else None

        if quotas is not None:
            if len(quotas) != bucket_count or any(not isinstance(q, int) or q <= 0 for q in quotas):
                raise ValueError("配额数量必须与桶数一致且均为正整数")
            quota_list = list(quotas)
        else:
            even = max(1, len(samples) // bucket_count) if samples else 1
            quota_list = [even] * bucket_count

        buckets = [
            {
                "bucket_index": idx,
                "hash_range": _bucket_range(idx, bucket_count),
                "quota": quota_list[idx],
                "sample_count": 0,
                "sample_ids": [],
                "shard_keys": [],
                "rejected": [],
            }
            for idx in range(bucket_count)
        ]
        quarantine: list[dict[str, Any]] = []
        diagnostics: list[dict[str, Any]] = []

        if shard_field is None:
            diagnostics.append(self._diag(
                DIAG_MISSING_COLUMN,
                "规则未声明分片字段，旧规则缺少字段身份，全部样本进入隔离区待确认",
                [sample.get("样本编号") for sample in samples],
            ))

        for index, sample in enumerate(samples):
            identity = _sample_identity(sample, index)
            raw_value = None if shard_field is None else sample.get(shard_field)
            if shard_field is None or raw_value in (None, ""):
                quarantine.append({
                    "sample_id": identity,
                    "reason": "分片字段缺失或值为空" if shard_field else "旧规则无分片字段身份",
                    "shard_value": UNKNOWN,
                    "bucket": None,
                })
                continue
            key = str(raw_value)
            bucket_index = _stable_hash(key) % bucket_count
            bucket = buckets[bucket_index]
            if bucket["sample_count"] >= bucket["quota"]:
                bucket["rejected"].append({"sample_id": identity, "shard_value": key})
                diagnostics.append(self._diag(
                    DIAG_REJECT,
                    f"桶 {bucket_index} 哈希区间 {bucket['hash_range']['text']} 配额 {bucket['quota']} 已满，"
                    f"样本 {identity} 无法被该段布局接纳",
                    [identity],
                ))
                continue
            bucket["sample_ids"].append(identity)
            bucket["shard_keys"].append(key)
            bucket["sample_count"] += 1

        fragments = self._build_fragments(buckets, fragment_size)
        self._annotate_bucket_diagnostics(buckets, diagnostics)

        blocked = any(bucket["rejected"] for bucket in buckets)
        block_reasons: list[str] = []
        warnings: list[str] = []
        if any(bucket["rejected"] for bucket in buckets):
            total_rejected = sum(len(bucket["rejected"]) for bucket in buckets)
            block_reasons.append(f"{total_rejected} 个样本因桶配额不足被拒，布局无法完整接纳")
        if quarantine:
            # 未知值不补零、不丢弃：作为待确认清单随目录交接，接管需显式确认
            warnings.append(f"{len(quarantine)} 个样本缺少分片字段身份，已隔离待确认（不补零）")

        summary = {
            "sample_total": len(samples),
            "placed_total": sum(bucket["sample_count"] for bucket in buckets),
            "quarantined_total": len(quarantine),
            "rejected_total": sum(len(bucket["rejected"]) for bucket in buckets),
            "zero_buckets": [bucket["bucket_index"] for bucket in buckets if bucket["sample_count"] == 0 and not bucket["rejected"]],
            "overloaded_buckets": [bucket["bucket_index"] for bucket in buckets if bucket["rejected"]],
            "fragment_total": len(fragments),
            "fragment_verified": 0,
            "fragment_pending": len(fragments),
            "fragment_failed": 0,
        }
        return {
            "base_generation": base_generation,
            "base_label": base_label,
            "shard_field": shard_field,
            "bucket_count": bucket_count,
            "buckets": buckets,
            "fragments": fragments,
            "quarantine": quarantine,
            "diagnostics": diagnostics,
            "blocked": blocked,
            "block_reasons": block_reasons,
            "warnings": warnings,
            "summary": summary,
            "sample_snapshot": [dict(sample) for sample in samples],
        }

    def _build_fragments(self, buckets: list[dict[str, Any]], fragment_size: int) -> list[dict[str, Any]]:
        """按桶顺序把样本切成固定大小片段，片段是再平衡校验与续做的最小单元。"""
        fragments: list[dict[str, Any]] = []
        for bucket in buckets:
            ids = bucket["sample_ids"]
            keys = bucket["shard_keys"]
            if not ids:
                continue
            for offset in range(0, len(ids), fragment_size):
                chunk_ids = ids[offset:offset + fragment_size]
                chunk_keys = keys[offset:offset + fragment_size]
                has_unknown = any(key == UNKNOWN for key in chunk_keys)
                fragments.append({
                    "fragment_index": len(fragments),
                    "bucket_index": bucket["bucket_index"],
                    "hash_range": bucket["hash_range"],
                    "offset": offset,
                    "sample_ids": chunk_ids,
                    "state": FRAG_PENDING,
                    "fail_reason": None,
                    "verified_by": None,
                    "expected_fact": {
                        "sample_count": len(chunk_ids),
                        "digest": _DIGEST_PENDING if has_unknown else _fact_digest(chunk_keys),
                        "keys": list(chunk_keys),
                    },
                    "last_actual_digest": None,
                    "last_actual_count": None,
                })
        return fragments

    def _annotate_bucket_diagnostics(
        self,
        buckets: list[dict[str, Any]],
        diagnostics: list[dict[str, Any]],
    ) -> None:
        max_count = max((bucket["sample_count"] for bucket in buckets), default=0)
        for bucket in buckets:
            tags: list[str] = []
            if bucket["sample_count"] == 0 and not bucket["rejected"]:
                tags.append(DIAG_ZERO)
                diagnostics.append(self._diag(
                    DIAG_ZERO,
                    f"桶 {bucket['bucket_index']} 哈希区间 {bucket['hash_range']['text']} 无样本落入，"
                    "属于零分片（哈希区间存在但无承载）",
                    [],
                ))
            if bucket["sample_count"] == 0 and bucket["rejected"]:
                tags.append(DIAG_EMPTY)
                diagnostics.append(self._diag(
                    DIAG_EMPTY,
                    f"桶 {bucket['bucket_index']} 自身样本为零且承接被拒样本，呈现全空承载",
                    [item["sample_id"] for item in bucket["rejected"]],
                ))
            if bucket["rejected"]:
                tags.append(DIAG_OVERLOAD)
                diagnostics.append(self._diag(
                    DIAG_OVERLOAD,
                    f"桶 {bucket['bucket_index']} 配额 {bucket['quota']} 被打满，"
                    f"{len(bucket['rejected'])} 个样本外溢，单桶过载",
                    [item["sample_id"] for item in bucket["rejected"]],
                ))
            elif max_count > 0 and bucket["sample_count"] == max_count and max_count > 1:
                # 高承载但尚未外溢：标记观察，不误判为故障
                tags.append("高承载观察")
            bucket["diagnostic_tags"] = tags

    # ------------------------------------------------------------------
    # 片段事实重算（依据当前种子，重灌差异在此显形）
    # ------------------------------------------------------------------
    def _current_fragment_keys(
        self,
        candidate: dict[str, Any],
        fragment: dict[str, Any],
    ) -> list[str]:
        rule = store.find(MODULE_SEGMENT, candidate["rule_id"])
        samples = self._load_samples(rule) if rule else []
        shard_field = candidate["shard_field"]
        id_to_sample: dict[str, dict[str, Any]] = {}
        for index, sample in enumerate(samples):
            id_to_sample[_sample_identity(sample, index)] = sample
        # 严格按片段登记的样本顺序重算；样本消失或值为空都记为待确认，绝不补零
        result: list[str] = []
        for identity in fragment["sample_ids"]:
            sample = id_to_sample.get(identity)
            if sample is None or shard_field is None:
                result.append(UNKNOWN)
                continue
            value = sample.get(shard_field)
            result.append(UNKNOWN if value in (None, "") else str(value))
        return result

    def _load_samples(self, rule: dict[str, Any]) -> list[dict[str, Any]]:
        """读取规则挂载的样本；未灌入种子时返回空集（不造假数据、不补零）。"""
        seeded = rule.get("_placement_samples")
        if isinstance(seeded, list):
            return [dict(item) for item in seeded]
        return []

    def _legacy_notes(self, rule: dict[str, Any]) -> list[str]:
        notes: list[str] = []
        if rule.get(SHARD_FIELD_NAME) in (None, ""):
            notes.append("旧规则缺少分片字段身份，未知值进入隔离区待确认，不做补零映射")
        if rule.get(BUCKET_COUNT_FIELD) in (None, ""):
            notes.append("旧规则未声明分片数量，无法切分哈希区间")
        for key in LEGACY_IDENTITY_KEYS:
            if rule.get(key) in (None, ""):
                notes.append(f"旧规则缺少身份字段「{key}」，裁决记录可追溯性受限")
        return notes

    def _refresh_candidate_state(self, candidate: dict[str, Any]) -> None:
        fragments = candidate["fragments"]
        if any(fragment["state"] == FRAG_FAILED for fragment in fragments):
            candidate["state"] = CAND_FAILED
        elif all(fragment["state"] == FRAG_VERIFIED for fragment in fragments) and fragments:
            candidate["state"] = CAND_VERIFIED
        elif not fragments:
            # 没有可校验片段（全隔离/零样本）：保持待校验，由 blocked 决定能否接管
            candidate["state"] = CAND_BUILDING
        else:
            candidate["state"] = CAND_BUILDING
        candidate["summary"].update({
            "fragment_verified": sum(1 for f in fragments if f["state"] == FRAG_VERIFIED),
            "fragment_pending": sum(1 for f in fragments if f["state"] == FRAG_PENDING),
            "fragment_failed": sum(1 for f in fragments if f["state"] == FRAG_FAILED),
        })

    def _record_contention(self, **event: Any) -> None:
        self._contention_seq += 1
        record = {"event_id": self._contention_seq, **event}
        self._contention.append(record)

    @staticmethod
    def _diag(category: str, message: str, sample_ids: list[Any]) -> dict[str, Any]:
        return {"category": category, "message": message, "sample_ids": list(sample_ids)}

    @staticmethod
    def _fragment_view(candidate: dict[str, Any], fragment: dict[str, Any]) -> dict[str, Any]:
        return {
            "fragment_index": fragment["fragment_index"],
            "bucket_index": fragment["bucket_index"],
            "hash_range": fragment["hash_range"],
            "offset": fragment["offset"],
            "sample_ids": fragment["sample_ids"],
            "state": fragment["state"],
            "fail_reason": fragment.get("fail_reason"),
            "verified_by": fragment.get("verified_by"),
            "expected_count": fragment["expected_fact"]["sample_count"],
            "expected_digest": fragment["expected_fact"]["digest"],
            "actual_count": fragment.get("last_actual_count"),
            "actual_digest": fragment.get("last_actual_digest"),
        }

    def _public_candidate(self, candidate: dict[str, Any]) -> dict[str, Any]:
        return {
            "candidate_id": candidate["candidate_id"],
            "rule_id": candidate["rule_id"],
            "dataset": candidate["dataset"],
            "operator": candidate["operator"],
            "state": candidate["state"],
            "base_generation": candidate["base_generation"],
            "target_generation": candidate["target_generation"],
            "committed_generation": candidate.get("committed_generation"),
            "legacy_notes": candidate["legacy_notes"],
            "blocked": candidate["blocked"],
            "block_reasons": candidate["block_reasons"],
            "warnings": candidate["warnings"],
            "shard_field": candidate["shard_field"],
            "bucket_count": candidate["bucket_count"],
            "summary": candidate["summary"],
            "buckets": candidate["buckets"],
            "fragments": [self._fragment_view(candidate, fragment) for fragment in candidate["fragments"]],
            "quarantine": candidate["quarantine"],
            "diagnostics": candidate["diagnostics"],
        }

    @staticmethod
    def _public_layout(layout: dict[str, Any]) -> dict[str, Any]:
        return {
            "generation": layout["generation"],
            "rule_id": layout.get("rule_id"),
            "dataset": layout.get("dataset"),
            "shard_field": layout.get("shard_field"),
            "bucket_count": layout["bucket_count"],
            "committed_by": layout.get("committed_by"),
            "from_candidate": layout.get("from_candidate"),
            "summary": layout.get("summary"),
            "buckets": layout["buckets"],
            "fragments": layout["fragments"],
            "quarantine": layout.get("quarantine", []),
        }


placement_service = PlacementService()
