<template>
  <section class="page" data-module="placement">
    <header class="page-head">
      <div>
        <h2>分片承载与再平衡</h2>
        <p class="page-desc">
          按桶对照样本数、哈希区间与目标配额，诊断零分片、缺列、全空样本与单桶过载；
          候选布局校验未完成前目录继续指向原桶，片段中断可续做，接管按目录代次裁决。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" @click="refreshAll">刷新全部视图</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-banner">{{ errorMessage }}</p>

    <!-- 规则与跨模块关联 -->
    <article class="panel">
      <h3>1. 选择分片规则并灌入样本种子</h3>
      <div class="form-grid">
        <label class="filter-item">
          <span>分片规则</span>
          <select v-model.number="ruleId" @change="onRuleChange">
            <option v-for="rule in rules" :key="rule.id" :value="Number(rule.id)">
              {{ rule['规则编号'] }}（{{ rule['目标数据集'] }} · {{ rule['分片数量'] }} 桶）
            </option>
          </select>
        </label>
        <label class="filter-item">
          <span>操作员</span>
          <input v-model="operator" placeholder="用于裁决留痕" />
        </label>
        <label class="filter-item grow">
          <span>样本种子 JSON（数组；分片字段缺失的样本会被隔离待确认，不补零）</span>
          <textarea v-model="seedText" rows="4" :placeholder="seedPlaceholder"></textarea>
        </label>
      </div>
      <div class="btn-row">
        <button class="btn primary" type="button" :disabled="busy" @click="seedSamples">灌入/重灌种子</button>
        <button class="btn" type="button" :disabled="busy" @click="loadDiagnosis">生成承载诊断</button>
      </div>
      <p v-if="seedNote" class="note">{{ seedNote }}</p>
    </article>

    <!-- 承载诊断面板 -->
    <article v-if="panel" class="panel">
      <h3>2. 承载面板（诊断快照，不产生候选、不动目录）</h3>
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">样本总数</span>
          <strong class="stat-value">{{ num(panel.summary.sample_total) }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">已入桶</span>
          <strong class="stat-value">{{ num(panel.summary.placed_total) }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">隔离待确认</span>
          <strong class="stat-value warn">{{ num(panel.summary.quarantined_total) }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">布局拒收</span>
          <strong class="stat-value" :class="{ danger: num(panel.summary.rejected_total) > 0 }">
            {{ num(panel.summary.rejected_total) }}
          </strong>
        </article>
      </div>
      <p v-if="panel.blocked" class="error-banner">布局无法完整接纳：{{ panel.block_reasons.join('；') }}</p>
      <ul v-if="panel.warnings?.length" class="note-list">
        <li v-for="(warning, idx) in panel.warnings" :key="idx" class="warn">{{ warning }}</li>
      </ul>
      <ul v-if="panel.legacy_notes?.length" class="note-list">
        <li v-for="(note, idx) in panel.legacy_notes" :key="`l${idx}`" class="muted">旧规则兼容：{{ note }}</li>
      </ul>

      <table class="data-table">
        <thead>
          <tr>
            <th>桶</th><th>哈希区间</th><th>目标配额</th><th>样本数</th><th>诊断标记</th><th>外溢样本</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="bucket in panel.buckets" :key="bucket.bucket_index">
            <td>{{ bucket.bucket_index }}</td>
            <td class="mono">{{ bucket.hash_range.text }}</td>
            <td>{{ bucket.quota }}</td>
            <td>{{ bucket.sample_count }}</td>
            <td>
              <span v-for="tag in bucket.diagnostic_tags" :key="tag" class="tag" :class="tagClass(tag)">{{ tag }}</span>
              <span v-if="!bucket.diagnostic_tags.length" class="muted">—</span>
            </td>
            <td>
              <span v-for="item in bucket.rejected" :key="item.sample_id" class="danger">
                {{ item.sample_id }}（{{ item.shard_value }}）
              </span>
              <span v-if="!bucket.rejected.length" class="muted">—</span>
            </td>
          </tr>
        </tbody>
      </table>

      <div class="two-col">
        <div>
          <h4>诊断事实</h4>
          <ul class="note-list">
            <li v-for="(diag, idx) in panel.diagnostics" :key="idx" :class="diagClass(diag.category)">
              [{{ diag.category }}] {{ diag.message }}
            </li>
            <li v-if="!panel.diagnostics.length" class="muted">无异常诊断</li>
          </ul>
        </div>
        <div>
          <h4>隔离区（未知值待确认）</h4>
          <table class="data-table compact">
            <thead><tr><th>样本</th><th>分片值</th><th>原因</th></tr></thead>
            <tbody>
              <tr v-for="item in panel.quarantine" :key="item.sample_id">
                <td>{{ item.sample_id }}</td>
                <td class="warn">{{ item.shard_value }}</td>
                <td>{{ item.reason }}</td>
              </tr>
              <tr v-if="!panel.quarantine.length"><td colspan="3" class="empty-state">无隔离样本</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </article>

    <!-- 候选修订 -->
    <article class="panel">
      <h3>3. 候选布局修订与再平衡恢复</h3>
      <div class="form-grid">
        <label class="filter-item">
          <span>片段容量（每条片段样本数）</span>
          <input v-model.number="fragmentSize" type="number" min="1" />
        </label>
        <label class="filter-item">
          <span>配额修订（逗号分隔，留空用默认均摊）</span>
          <input v-model="quotaText" placeholder="如 50,50,50" />
        </label>
        <label class="filter-item">
          <span>依据目录代次（留空取当前有效代次）</span>
          <input v-model="baseGenerationText" type="number" min="0" placeholder="例如 0" />
        </label>
      </div>
      <div class="btn-row">
        <button class="btn primary" type="button" :disabled="busy" @click="createCandidate">建立候选</button>
        <button class="btn" type="button" :disabled="busy" @click="loadCandidates">刷新候选列表</button>
      </div>

      <div v-for="candidate in candidates" :key="candidate.candidate_id" class="candidate-card">
        <header class="candidate-head">
          <div>
            <strong>候选 #{{ candidate.candidate_id }}</strong>
            <span class="tag" :class="stateClass(candidate.state)">{{ candidate.state }}</span>
            <span class="muted">数据集：{{ candidate.dataset }} · 操作人：{{ candidate.operator }}</span>
          </div>
          <div class="muted mono">
            基准代次 {{ candidate.base_generation }} → 目标代次 {{ candidate.target_generation }}
            <template v-if="candidate.committed_generation != null"> · 已落定 {{ candidate.committed_generation }}</template>
          </div>
        </header>
        <p v-if="candidate.blocked" class="error-banner">无法接纳：{{ candidate.block_reasons.join('；') }}</p>
        <ul v-if="candidate.warnings.length" class="note-list">
          <li v-for="(warning, idx) in candidate.warnings" :key="idx" class="warn">{{ warning }}</li>
        </ul>
        <p class="muted">
          片段 {{ candidate.summary.fragment_verified }}/{{ candidate.summary.fragment_total }} 已校验 ·
          失败 {{ candidate.summary.fragment_failed }} · 待校验 {{ candidate.summary.fragment_pending }}
        </p>
        <table class="data-table compact">
          <thead>
            <tr><th>片段</th><th>桶/区间</th><th>状态</th><th>期望事实</th><th>实际事实</th><th>失败原因 / 校验人</th><th>操作</th></tr>
          </thead>
          <tbody>
            <tr v-for="fragment in candidate.fragments" :key="fragment.fragment_index">
              <td>{{ fragment.fragment_index }}</td>
              <td class="mono">{{ fragment.bucket_index }} · {{ fragment.hash_range.text }}</td>
              <td><span class="tag" :class="fragClass(fragment.state)">{{ fragment.state }}</span></td>
              <td class="mono small">{{ fragment.expected_count }} 条 / {{ short(fragment.expected_digest) }}</td>
              <td class="mono small">
                <template v-if="fragment.actual_count != null">
                  {{ fragment.actual_count }} 条 / {{ short(fragment.actual_digest ?? '') }}
                </template>
                <span v-else class="muted">未重算</span>
              </td>
              <td>{{ fragment.fail_reason ?? fragment.verified_by ?? '—' }}</td>
              <td>
                <button
                  v-if="fragment.state === '未校验'"
                  class="link"
                  type="button"
                  :disabled="busy"
                  @click="verifyFragment(candidate.candidate_id, fragment.fragment_index)"
                >
                  校验此片段
                </button>
                <span v-else class="muted">—</span>
              </td>
            </tr>
            <tr v-if="!candidate.fragments.length">
              <td colspan="7" class="empty-state">无可校验片段（样本全部隔离或零样本）</td>
            </tr>
          </tbody>
        </table>
        <div class="btn-row">
          <button class="btn" type="button" :disabled="busy" @click="resume(candidate.candidate_id)">
            从首个未校验片段续做
          </button>
          <label class="inline-check">
            <input v-model="confirmMap[candidate.candidate_id]" type="checkbox" />
            显式确认承接隔离区待确认清单（不补零）
          </label>
          <button class="btn primary" type="button" :disabled="busy" @click="commit(candidate.candidate_id)">
            原子接管（校验事实 + 目录提交）
          </button>
        </div>
      </div>
    </article>

    <!-- 目录代次 -->
    <article class="panel">
      <h3>4. 目录代次（候选未接管前始终指向原桶）</h3>
      <button class="btn" type="button" @click="loadCatalog">刷新目录</button>
      <table v-if="catalogDatasets.length" class="data-table compact">
        <thead><tr><th>数据集</th><th>当前代次</th><th>桶数</th><th>接管来源候选</th><th>隔离待确认</th></tr></thead>
        <tbody>
          <tr v-for="item in catalogDatasets" :key="item.dataset">
            <td>{{ item.dataset }}</td>
            <td><strong>{{ item.generation }}</strong></td>
            <td>{{ item.active?.bucket_count ?? '—' }}</td>
            <td>{{ item.active?.from_candidate ?? '—' }}</td>
            <td :class="{ warn: (item.active?.quarantine.length ?? 0) > 0 }">
              {{ item.active?.quarantine.length ?? 0 }}
            </td>
          </tr>
        </tbody>
      </table>
      <p v-else class="muted">目录尚无生效布局，仍指向初始空承载。</p>
    </article>

    <!-- 争用证据 -->
    <article class="panel">
      <h3>5. 并发争用证据（被代次裁决拒绝方可查看）</h3>
      <button class="btn" type="button" @click="loadContention">刷新争用记录</button>
      <table v-if="contentions.length" class="data-table compact">
        <thead><tr><th>#</th><th>数据集</th><th>动作</th><th>操作人</th><th>提交代次</th><th>有效代次</th><th>说明</th></tr></thead>
        <tbody>
          <tr v-for="event in contentions" :key="event.event_id">
            <td>{{ event.event_id }}</td>
            <td>{{ event.dataset }}</td>
            <td>{{ event.action }}</td>
            <td>{{ event.operator }}</td>
            <td class="danger">{{ event.supplied }}</td>
            <td class="ok">{{ event.effective }}</td>
            <td>{{ event.detail }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else class="muted">暂无争用记录。</p>
    </article>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import { request } from '@/api/client'

interface HashRange { lower: number; upper: number; text: string }
interface RejectedSample { sample_id: string; shard_value: string }
interface Bucket {
  bucket_index: number
  hash_range: HashRange
  quota: number
  sample_count: number
  sample_ids: string[]
  rejected: RejectedSample[]
  diagnostic_tags: string[]
}
interface Fragment {
  fragment_index: number
  bucket_index: number
  hash_range: HashRange
  sample_ids: string[]
  state: string
  fail_reason: string | null
  verified_by: string | null
  expected_count: number
  expected_digest: string
  actual_count: number | null
  actual_digest: string | null
}
interface Diagnostic { category: string; message: string; sample_ids: Array<string | number> }
interface QuarantineItem { sample_id: string; reason: string; shard_value: string; bucket: number | null }
interface PlacementSummary {
  sample_total: number
  placed_total: number
  quarantined_total: number
  rejected_total: number
  zero_buckets: number[]
  overloaded_buckets: number[]
  fragment_total: number
  fragment_verified: number
  fragment_pending: number
  fragment_failed: number
}
interface DiagnosisPanel {
  shard_field: string | null
  bucket_count: number
  blocked: boolean
  block_reasons: string[]
  warnings?: string[]
  legacy_notes?: string[]
  buckets: Bucket[]
  fragments: Fragment[]
  quarantine: QuarantineItem[]
  diagnostics: Diagnostic[]
  summary: PlacementSummary
  reason?: string
}
interface Candidate {
  candidate_id: number
  rule_id: number
  dataset: string
  operator: string
  state: string
  base_generation: number
  target_generation: number
  committed_generation: number | null
  legacy_notes: string[]
  blocked: boolean
  block_reasons: string[]
  warnings: string[]
  shard_field: string | null
  bucket_count: number
  summary: PlacementSummary
  buckets: Bucket[]
  fragments: Fragment[]
  quarantine: QuarantineItem[]
  diagnostics: Diagnostic[]
}
interface CatalogItem {
  dataset: string
  generation: number
  active: { bucket_count: number; from_candidate: number; quarantine: QuarantineItem[] } | null
}
interface ContentionEvent {
  event_id: number
  dataset: string
  action: string
  operator: string
  supplied: number
  effective: number
  detail: string
  candidate_id?: number
}
interface RuleRow {
  id: number
  '规则编号': string
  '目标数据集': string
  '分片字段': string
  '分片数量': number
}

const rules = ref<RuleRow[]>([])
const ruleId = ref<number>(0)
const operator = ref('operator-1')
const seedText = ref('')
const seedNote = ref('')
const fragmentSize = ref(5)
const quotaText = ref('')
const baseGenerationText = ref('')
const panel = ref<DiagnosisPanel | null>(null)
const candidates = ref<Candidate[]>([])
const catalogDatasets = ref<CatalogItem[]>([])
const contentions = ref<ContentionEvent[]>([])
const confirmMap = reactive<Record<number, boolean>>({})
const busy = ref(false)
const errorMessage = ref('')

const seedPlaceholder = '[{"样本编号":"s1","分片字段名":"值A"},{"样本编号":"s2","分片字段名":null}]'

function num(value: unknown): number {
  return typeof value === 'number' ? value : 0
}

function short(digest: string): string {
  return digest.length > 14 ? `${digest.slice(0, 12)}…` : digest
}

function tagClass(tag: string): string {
  if (tag.includes('过载') || tag.includes('无法接纳')) return 'danger-tag'
  if (tag.includes('零') || tag.includes('全空')) return 'warn-tag'
  return 'info-tag'
}

function diagClass(category: string): string {
  if (category === '单桶过载' || category === '无法接纳') return 'danger'
  if (category === '零分片' || category === '全空样本' || category === '缺列') return 'warn'
  return 'muted'
}

function stateClass(state: string): string {
  if (state === '已接管') return 'ok-tag'
  if (state === '校验失败' || state === '已拒绝') return 'danger-tag'
  if (state === '校验通过') return 'ok-tag'
  return 'info-tag'
}

function fragClass(state: string): string {
  if (state === '已校验') return 'ok-tag'
  if (state === '校验失败') return 'danger-tag'
  return 'info-tag'
}

async function callApi<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(path, init)
  const body = (await response.json().catch(() => ({}))) as Record<string, unknown>
  if (!response.ok) {
    const detail = typeof body.detail === 'string' ? body.detail : `请求失败（HTTP ${response.status}）`
    throw new Error(detail)
  }
  return body as T
}

async function loadRules(): Promise<void> {
  const payload = await callApi<{ items: RuleRow[] }>('/api/segment?page=1&size=200')
  rules.value = payload.items
  if (rules.value.length && !ruleId.value) {
    ruleId.value = Number(rules.value[0].id)
  }
}

function onRuleChange(): void {
  panel.value = null
  candidates.value = []
  errorMessage.value = ''
  seedNote.value = ''
}

async function seedSamples(): Promise<void> {
  errorMessage.value = ''
  let samples: unknown
  try {
    samples = JSON.parse(seedText.value || '[]')
  } catch {
    errorMessage.value = '样本种子不是合法 JSON，请检查后重试'
    return
  }
  if (!Array.isArray(samples)) {
    errorMessage.value = '样本种子必须是对象数组'
    return
  }
  await runGuarded(async () => {
    const result = await callApi<{ entry: { loaded: number; affected_candidates: number[]; note: string } }>(
      `/api/placement/rules/${ruleId.value}/seed`,
      { method: 'POST', body: JSON.stringify({ values: { samples } }) },
    )
    seedNote.value = `${result.entry.note}（已加载 ${result.entry.loaded} 条，影响候选 ${result.entry.affected_candidates.join(',') || '无'}）`
  })
}

async function loadDiagnosis(): Promise<void> {
  await runGuarded(async () => {
    panel.value = await callApi<DiagnosisPanel>(`/api/placement/rules/${ruleId.value}/diagnosis`)
  })
}

function parseQuotas(): number[] | undefined {
  const text = quotaText.value.trim()
  if (!text) return undefined
  const quotas = text.split(',').map((part) => Number(part.trim()))
  if (quotas.some((value) => !Number.isInteger(value) || value <= 0)) {
    throw new Error('配额必须是逗号分隔的正整数')
  }
  return quotas
}

async function createCandidate(): Promise<void> {
  await runGuarded(() => {
    const quotas = parseQuotas()
    const values: Record<string, unknown> = {
      rule_id: ruleId.value,
      fragment_size: fragmentSize.value,
      operator: operator.value,
    }
    if (quotas) values.quotas = quotas
    if (baseGenerationText.value !== '') values.base_generation = Number(baseGenerationText.value)
    return callApi('/api/placement/candidates', { method: 'POST', body: JSON.stringify({ values }) })
  })
  await loadCandidates()
}

async function loadCandidates(): Promise<void> {
  const payload = await callApi<{ items: Candidate[] }>('/api/placement/candidates')
  candidates.value = payload.items
}

async function verifyFragment(candidateId: number, fragmentIndex: number): Promise<void> {
  await runGuarded(async () => {
    await callApi(`/api/placement/candidates/${candidateId}/fragments/${fragmentIndex}/verify`,
      { method: 'POST', body: JSON.stringify({ values: { operator: operator.value } }) })
  })
  await loadCandidates()
}

async function resume(candidateId: number): Promise<void> {
  await runGuarded(async () => {
    const result = await callApi<{ entry: { done: boolean; stopped_at: number | null } }>(
      `/api/placement/candidates/${candidateId}/resume`,
      { method: 'POST', body: JSON.stringify({ values: { operator: operator.value } }) },
    )
    if (!result.entry.done) {
      errorMessage.value = `再平衡在片段 ${result.entry.stopped_at} 处阻塞，坏片段未标成功，请查看失败原因`
    }
  })
  await Promise.all([loadCandidates(), loadCatalog()])
}

async function commit(candidateId: number): Promise<void> {
  await runGuarded(async () => {
    const values: Record<string, unknown> = {
      operator: operator.value,
      confirm_quarantine: Boolean(confirmMap[candidateId]),
    }
    if (baseGenerationText.value !== '') values.base_generation = Number(baseGenerationText.value)
    await callApi(`/api/placement/candidates/${candidateId}/commit`,
      { method: 'POST', body: JSON.stringify({ values }) })
  })
  await Promise.all([loadCandidates(), loadCatalog(), loadContention()])
}

async function loadCatalog(): Promise<void> {
  const payload = await callApi<{ datasets: CatalogItem[] }>('/api/placement/catalog')
  catalogDatasets.value = payload.datasets
}

async function loadContention(): Promise<void> {
  const payload = await callApi<{ items: ContentionEvent[] }>('/api/placement/contention')
  contentions.value = payload.items
}

async function runGuarded(action: () => Promise<unknown>): Promise<void> {
  errorMessage.value = ''
  busy.value = true
  try {
    await action()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '承载操作失败'
  } finally {
    busy.value = false
  }
}

async function refreshAll(): Promise<void> {
  await runGuarded(async () => {
    await Promise.all([loadDiagnosis(), loadCandidates(), loadCatalog(), loadContention()])
  })
}

onMounted(async () => {
  await runGuarded(async () => {
    await loadRules()
    await Promise.all([loadCandidates(), loadCatalog(), loadContention()])
  })
})
</script>

<style scoped>
.panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px 16px; margin-bottom: 14px; }
.panel h3 { margin: 0 0 10px; font-size: 15px; }
.panel h4 { margin: 8px 0; font-size: 13px; color: var(--muted); }
.form-grid { display: flex; flex-wrap: wrap; gap: 12px; }
.form-grid .filter-item { flex: 1 1 220px; }
.form-grid .grow { flex: 2 1 320px; }
textarea { width: 100%; font-family: monospace; font-size: 12px; padding: 6px; border: 1px solid var(--border); border-radius: 6px; }
select, input[type='number'] { padding: 6px; border: 1px solid var(--border); border-radius: 6px; }
.btn-row { display: flex; gap: 10px; align-items: center; margin-top: 10px; flex-wrap: wrap; }
.inline-check { display: flex; align-items: center; gap: 6px; font-size: 13px; color: var(--muted); }
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 10px; }
.note-list { list-style: none; padding: 0; margin: 8px 0 0; font-size: 12px; }
.note-list li { margin-bottom: 4px; }
.candidate-card { border: 1px solid var(--border); border-radius: 8px; padding: 12px; margin-top: 12px; }
.candidate-head { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px; }
.compact th, .compact td { padding: 6px 8px; font-size: 12px; }
.mono { font-family: monospace; }
.small { font-size: 11px; }
.tag { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 11px; margin: 0 4px; }
.danger-tag { background: #fee4e2; color: #b42318; }
.warn-tag { background: #fef3c7; color: #92400e; }
.info-tag { background: #e0eaff; color: #1d4ed8; }
.ok-tag { background: #dcfae6; color: #027a48; }
.danger { color: #b42318; }
.warn { color: #b45309; }
.ok { color: #027a48; }
.muted { color: var(--muted); }
.error-banner { background: #fee4e2; color: #b42318; border-radius: 6px; padding: 8px 12px; font-size: 13px; margin: 8px 0; }
@media (max-width: 960px) { .two-col { grid-template-columns: 1fr; } }
</style>
