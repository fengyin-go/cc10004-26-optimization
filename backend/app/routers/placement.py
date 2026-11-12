"""分片承载与再平衡接口：倾斜诊断、候选修订、片段续校、目录代次接管。

所有写动作都走 EntryPayload（与其它模块一致），冲突用 409 返回并保留争用证据，
坏片段用 422 表达“事实不被接纳”，绝不把失败包装成成功。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import EntryPayload
from app.services.placement import placement_service

router = APIRouter(prefix="/api/placement", tags=["分片承载与再平衡"])


def _values(payload: EntryPayload) -> dict[str, Any]:
    return payload.values or {}


@router.get("/rules/{rule_id}/diagnosis")
def diagnose_rule(rule_id: int) -> dict[str, Any]:
    """承载面板：逐桶给出样本数、哈希区间、目标配额与倾斜分类。"""
    result, error = placement_service.diagnose_rule(rule_id)
    if error:
        raise HTTPException(status_code=404, detail=error)
    return result


@router.post("/rules/{rule_id}/seed")
def seed_samples(rule_id: int, payload: EntryPayload) -> dict[str, Any]:
    """灌入样本种子；重灌不改写既存候选事实，差异留给校验暴露。"""
    values = _values(payload)
    samples = values.get("samples")
    if not isinstance(samples, list):
        raise HTTPException(status_code=400, detail="values.samples 必须是样本对象数组")
    result, error = placement_service.seed_samples(rule_id, samples)
    if error:
        raise HTTPException(status_code=404, detail=error)
    return {"ok": True, "message": "样本种子已加载", "entry": result}


@router.post("/candidates")
def create_candidate(payload: EntryPayload) -> dict[str, Any]:
    """基于当前目录代次建立再平衡候选；代次过期返回 409 并记录争用证据。"""
    values = _values(payload)
    rule_id = values.get("rule_id")
    if not isinstance(rule_id, int):
        raise HTTPException(status_code=400, detail="values.rule_id 必填且为整数")
    quotas = values.get("quotas")
    if quotas is not None and (
        not isinstance(quotas, list) or any(not isinstance(q, int) or q <= 0 for q in quotas)
    ):
        raise HTTPException(status_code=400, detail="values.quotas 必须是正整数数组")
    fragment_size = int(values.get("fragment_size") or 5)
    base_generation = values.get("base_generation")
    operator = str(values.get("operator") or "anonymous")
    candidate, error, status = placement_service.create_candidate(
        rule_id,
        quotas=quotas,
        fragment_size=fragment_size,
        base_generation=base_generation if isinstance(base_generation, int) else None,
        operator=operator,
    )
    if error:
        raise HTTPException(status_code=status or 400, detail=error)
    return {"ok": True, "message": "候选布局已建立，等待片段校验", "entry": candidate}


@router.get("/candidates")
def list_candidates(dataset: str | None = Query(default=None)) -> dict[str, Any]:
    return placement_service.list_candidates(dataset)


@router.get("/candidates/{candidate_id}")
def get_candidate(candidate_id: int) -> dict[str, Any]:
    candidate = placement_service.get_candidate(candidate_id)
    if candidate is None:
        raise HTTPException(status_code=404, detail=f"候选 {candidate_id} 不存在")
    return candidate


@router.post("/candidates/{candidate_id}/fragments/{fragment_index}/verify")
def verify_fragment(candidate_id: int, fragment_index: int, payload: EntryPayload) -> dict[str, Any]:
    """校验单个片段；事实不一致返回 422，片段保持失败态，不允许标成功。"""
    operator = str(_values(payload).get("operator") or "anonymous")
    view, error, status = placement_service.verify_fragment(candidate_id, fragment_index, operator=operator)
    if error:
        raise HTTPException(status_code=status or 400, detail=error)
    return {"ok": True, "message": f"片段 {fragment_index} 校验通过", "entry": view}


@router.post("/candidates/{candidate_id}/resume")
def resume_verification(candidate_id: int, payload: EntryPayload) -> dict[str, Any]:
    """从首个未校验片段续做；返回续做起止与阻塞片段，坏片段不会被跳过。"""
    operator = str(_values(payload).get("operator") or "anonymous")
    result, error, status = placement_service.resume_verification(candidate_id, operator=operator)
    if error:
        raise HTTPException(status_code=status or 400, detail=error)
    message = "全部片段已校验" if result["done"] else f"在片段 {result['stopped_at']} 处阻塞，已定位未校验片段"
    return {"ok": result["done"], "message": message, "entry": result}


@router.post("/candidates/{candidate_id}/commit")
def commit_candidate(candidate_id: int, payload: EntryPayload) -> dict[str, Any]:
    """原子接管：校验事实与目录代次在同一裁决内落定，否则目录继续指向原桶。"""
    values = _values(payload)
    base_generation = values.get("base_generation")
    # 隔离区未知值需显式确认承接；默认 False，避免静默吞掉待确认样本
    confirm_quarantine = bool(values.get("confirm_quarantine", False))
    operator = str(values.get("operator") or "anonymous")
    result, error, status = placement_service.commit_candidate(
        candidate_id,
        base_generation=base_generation if isinstance(base_generation, int) else None,
        confirm_quarantine=confirm_quarantine,
        operator=operator,
    )
    if error:
        raise HTTPException(status_code=status or 400, detail=error)
    return {"ok": True, "message": f"承载已接管，目录推进到代次 {result['generation']}", "entry": result}


@router.get("/catalog")
def get_catalog(dataset: str | None = Query(default=None)) -> dict[str, Any]:
    """目录视图：候选未提交前这里始终是原桶代次。"""
    return placement_service.get_catalog(dataset)


@router.get("/contention")
def list_contention(dataset: str | None = Query(default=None)) -> dict[str, Any]:
    """争用证据：被代次 CAS 拒绝的一方可以查看先到裁决与现场。"""
    return placement_service.list_contention(dataset)
