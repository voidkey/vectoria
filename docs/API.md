# Vectoria API 文档

## 基础信息

- **框架**: FastAPI
- **交互式文档**: `http://localhost:8000/docs`（Swagger UI 自动生成）
- **OpenAPI Spec**: `http://localhost:8000/openapi.json`

---

## 接口一览

### 1. 健康检查

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 服务状态 & 支持的文件类型 |

---

### 2. 文档解析（不入库，仅解析）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/analyze/file` | 上传文件解析为 Markdown（multipart/form-data） |
| POST | `/analyze/url` | 传入 URL 解析为 Markdown |

> `/analyze/*` 仅接受静态 `X-API-Key` 认证（内部/运维用途）；JWT 调用方返回 403。纯 JWT 部署（只设 `JWT_SECRET`、未设 `API_KEY`）下 analyze 不可用 —— 需 analyze 时请配置 `API_KEY`。

#### 请求 - `/analyze/url`（AnalyzeURLRequest）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `url` | string | 是 | 要解析的网页地址 |
| `extract_images` | bool | 否 | 是否提取图片，默认 `true` |

#### 请求 - `/analyze/file`

通过 `multipart/form-data` 上传文件，可选参数 `extract_images`（bool，默认 `true`）。

#### 响应 - AnalyzeResponse

| 字段 | 类型 | 说明 |
|------|------|------|
| `title` | string | 文档标题 |
| `source` | string | 来源（文件名或 URL） |
| `content` | string | 解析后的 Markdown 内容 |
| `outline` | OutlineItem[] | 文档大纲 |
| `image_count` | int | 图片数量 |
| `has_edit` | bool | 是否存在编辑版本。判断有无编辑请用这个字段，不要用 `edited_revision` |
| `edited_revision` | int | 编辑版本号（单调递增，撤回后不归零，故非 0 不代表当前有编辑版本） |
| `edited_at` | string \| null | 当前编辑版本的写入时间（ISO 8601） |
| `images` | ImageInfo[] | 图片列表 |

**OutlineItem**

| 字段 | 类型 | 说明 |
|------|------|------|
| `level` | int | 标题层级（1-6） |
| `title` | string | 标题文本 |

**ImageInfo**

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | 图片标识 |
| `url` | string | 图片 URL |
| `context` | string | 图片周围的文本上下文 |
| `type` | string | 图片类型，默认 `"unknown"` |

---

### 3. 知识库管理

| 方法 | 路径 | 说明 | 状态码 |
|------|------|------|--------|
| POST | `/knowledgebases` | 创建知识库 | 201 |
| GET | `/knowledgebases` | 列出所有知识库 | 200 |
| DELETE | `/knowledgebases/{kb_id}` | 删除知识库（级联删除所有文档） | 204 |

#### 请求 - KnowledgeBaseCreate

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 知识库名称 |
| `description` | string | 否 | 描述，默认空字符串 |

#### 响应 - KnowledgeBaseResponse

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | 知识库 UUID |
| `name` | string | 名称 |
| `description` | string | 描述 |
| `created_at` | string | 创建时间（ISO 8601 格式） |

---

### 4. 文档管理

| 方法 | 路径 | 说明 | 状态码 |
|------|------|------|--------|
| POST | `/knowledgebases/{kb_id}/documents/file` | 上传文件入库（multipart/form-data） | 201 |
| POST | `/knowledgebases/{kb_id}/documents/url` | URL 入库 | 201 |
| POST | `/knowledgebases/{kb_id}/documents/text` | 文本入库（JSON body） | 201 |
| POST | `/knowledgebases/{kb_id}/documents/uploads` | 直传：签发预签名上传 URL | 201 |
| POST | `/knowledgebases/{kb_id}/documents/uploads/{upload_id}/complete` | 直传：校验暂存文件并入库 | 201 |
| GET | `/knowledgebases/{kb_id}/documents` | 列出知识库下所有文档 | 200 |
| GET | `/knowledgebases/{kb_id}/documents/{doc_id}` | 查询单个文档状态 | 200 |
| DELETE | `/knowledgebases/{kb_id}/documents/{doc_id}` | 删除文档及其向量数据 | 204 |
| PUT | `/knowledgebases/{kb_id}/documents/{doc_id}/edited` | 存入编辑后的正文（JSON） | 200 |
| POST | `/knowledgebases/{kb_id}/documents/{doc_id}/edited/file` | 存入编辑后的文件（multipart） | 200 |
| GET | `/knowledgebases/{kb_id}/documents/{doc_id}/edited` | 取回编辑后的内容 | 200 |
| DELETE | `/knowledgebases/{kb_id}/documents/{doc_id}/edited` | 撤回编辑版本 | 204 |

> **注意**: 文档入库是**异步处理**的，接口立即返回 `status: "indexing"`，需轮询单文档接口 `GET /knowledgebases/{kb_id}/documents/{doc_id}` 检查进度。

#### 请求 - DocumentURLRequest（URL 入库）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `url` | string | 是 | 文档 URL |

#### 请求 - DocumentTextRequest（文本入库）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `text` | string | 是 | 要入库的全部文本（UTF-8）。受 `max_upload_bytes` 字节上限约束 |
| `title` | string | 否 | 文档标题；不传则取文本首行（截断 80 字符），首行为空则用 `text-{8位hash}` |

服务内部会把文本以 `.txt` 文件形式写入对象存储并按普通文档流程解析。`GET /knowledgebases/{kb_id}/documents/{doc_id}` 返回的 `content` 字段即为用户原始输入的全部文本。

#### 直传（预签名上传）

大文件可绕过 API 直传对象存储，避免字节流经服务端。分两步：

**第一步** `POST .../documents/uploads` — 请求 CreateUploadRequest：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `filename` | string | 是 | 文件名（决定解析引擎与标题） |
| `sha256` | string | 否 | 客户端预算的内容 sha256；命中已有文档则直接去重、不签发 URL（仅用于跳过上传，不作正确性依据，complete 时用真实字节重算） |
| `size` | int | 否 | 声明大小；超过 `max_upload_bytes` 立即拒绝（建议性早拒，真正的大小闸在 complete 用 HEAD 校验） |

响应 CreateUploadResponse：

| 字段 | 类型 | 说明 |
|------|------|------|
| `dedup_hit` | bool | 命中前置去重时为 `true`，此时 `document` 为已有文档、其余字段为空 |
| `document` | DocumentIngestResponse \| null | 去重命中时返回的已有文档 |
| `upload_id` | string \| null | 不透明句柄，原样回传给 complete |
| `upload_url` | string \| null | 预签名 PUT 直传地址 |
| `method` | string \| null | 固定 `PUT` |
| `expires_at` | string \| null | URL 过期时间（ISO 8601） |

**第二步** 客户端用 `PUT` 把文件字节直传到 `upload_url`，再调 `POST .../documents/uploads/{upload_id}/complete`（可选 query 参数 `?wait=true`，语义同 `/file`）。服务端先 HEAD 校验大小（超限不下载），再拉取字节跑与 `/file` 完全相同的校验闸并去重，最后入库，返回 `DocumentIngestResponse`（与 `/file` 同构）。

> **错误码**: 不支持预签名的存储后端返回 `501`（`1212` `UPLOAD_NOT_SUPPORTED`）；`upload_id` 对应对象不存在 / 上传失败 / 已过期返回 `404`（`1211` `UPLOAD_NOT_FOUND`）。浏览器直传需给桶配置 CORS，并给 `upload_staging/` 前缀配置生命周期过期规则回收未完成的上传——两项桶配置见 [README](../README.md#object-storage-bucket-configuration)。

#### 响应 - DocumentIngestResponse（入库时返回）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | 文档 UUID |
| `kb_id` | string | 所属知识库 ID |
| `title` | string | 文档标题 |
| `source` | string | 来源（URL 或文件名） |
| `chunk_count` | int | 分块数量（初始为 0，完成后更新） |
| `status` | string | 状态：`indexing` / `completed` / `failed` |
| `index_status` | string | 索引状态：`pending` / `completed` / `failed` / `skipped`（见下方说明） |
| `error_msg` | string | 失败时的错误信息 |
| `created_at` | string | 创建时间（ISO 8601 格式） |
| `content` | string | 解析后的 Markdown 内容 |
| `outline` | OutlineItem[] | 文档大纲 |
| `image_count` | int | 图片数量 |

#### 响应 - DocumentResponse（列表时返回）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | 文档 UUID |
| `kb_id` | string | 所属知识库 ID |
| `title` | string | 标题 |
| `source` | string | 来源 |
| `chunk_count` | int | 分块数量 |
| `status` | string | `indexing` / `completed` / `failed` |
| `index_status` | string | `pending` / `completed` / `failed` / `skipped`（见下方说明） |
| `error_msg` | string | 错误信息 |
| `created_at` | string | 创建时间（ISO 8601 格式） |
| `has_edit` | bool | 是否存在编辑版本 |
| `edited_revision` | int | 编辑版本号（单调递增） |
| `edited_at` | string \| null | 当前编辑版本的写入时间 |

> **注意**: 查询单个文档详情 `GET /knowledgebases/{kb_id}/documents/{doc_id}` 返回的是 `DocumentIngestResponse`，包含 `content`、`outline`、`image_count` 等额外字段。

---


### 4.5 编辑后内容（edited content）

下游拿到我们的解析结果后，往往还要再做一轮整理（LLM 清洗、重排版、把图片描述并回正文），产出一份"真正能用"的版本，需要存回我们这边以便后续取用。这组接口就是这份编辑版本的存放位置。

**核心语义：编辑版本与解析结果是两份独立的内容，互不覆盖。**

| | 存放位置 | 谁写 |
|---|---|---|
| 解析结果 | `documents.content`（`GET /documents/{doc_id}` 的 `content` 字段） | 我们的解析管线 |
| 编辑版本 | 对象存储 `edits/{kb_id}/{doc_id}/{revision}/{filename}` | 调用方 |

正因为两者正交，重新解析（含 `retry_dead_docs` 对失败文档的自动重试）不会破坏已存入的编辑版本，反过来存编辑版本也不会影响文档的解析状态机。

> ⚠️ **编辑版本不参与检索。** `POST /knowledgebases/{kb_id}/query` 的 RAG 召回仍然基于原始解析结果 `documents.content`。把编辑版本接入索引需要重新分块和 embedding，是独立的一期工作，当前**未实现**。

#### 请求 - EditedContentRequest（`PUT .../edited`，文本形式）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `content` | string | 是 | 编辑后的正文（UTF-8）。全空白视为非法；受 `max_content_chars` 字符上限约束 |
| `filename` | string | 否 | 存入对象存储时使用的文件名，默认 `content.md`。会被规约为单个路径片段（去掉目录成分），无法越出本文档的前缀 |
| `base_revision` | int | 否 | 乐观锁：传了就必须等于当前 `edited_revision`，否则返回 409。批量写入的调用方建议总是传 |

#### 请求 - `POST .../edited/file`（文件形式）

`multipart/form-data`，字段名 `file`；`base_revision` 作为 query 参数传递。

字节原样存储，**不做 MIME 嗅探、不解析、不做页数闸** —— 那些闸是为保护解析管线而存在的，而这里的产物不进解析管线，只是调用方之后自取的一份不透明文件。仅校验大小（`max_upload_bytes`）与非空。

#### 响应 - EditedContentResponse

| 字段 | 类型 | 说明 |
|------|------|------|
| `doc_id` / `kb_id` | string | 所属文档 / 知识库 |
| `revision` | int | 本次编辑版本号 |
| `filename` | string | 文件名 |
| `object_key` | string | 对象存储 key |
| `url` | string | 预签名下载地址，**会过期**，当作一次性下载句柄用，不要持久化 |
| `edited_at` | string \| null | 写入时间（ISO 8601） |
| `content` | string \| null | 仅当 `GET` 时带 `?include_content=true` 且产物是合法 UTF-8 才有值；二进制产物为 `null`，改用 `url` 下载 |

#### 版本号与并发

`edited_revision` 在写入前被原子地自增并占位，随后才上传对象——因此并发写入的两个调用方拿到的是不同的版本号，绝不会写到同一个 key 上。最后完成的写入者取得指针；先前版本的字节仍留在各自的 key 上。

指针更新带 `edited_revision` 条件：一个占了较早版本号但完成较晚的写入者不会把文档回退到旧内容。

`DELETE .../edited` 是**解除引用**而非清除：对象保留，`edited_revision` 计数器也不归零（归零会让下一次写入复用已撤回版本的 key）。所有版本的对象在文档被删除时统一回收。

#### 错误码

| 场景 | HTTP | code |
|------|------|------|
| 文档不存在 | 404 | `1301` NOT_FOUND |
| 文档没有编辑版本（GET / DELETE） | 404 | `1217` EDIT_NOT_FOUND |
| 文档类型不支持编辑（`site_capture`） | 400 | `1218` EDIT_NOT_SUPPORTED |
| `base_revision` 过期（并发写） | 409 | `1219` EDIT_REVISION_CONFLICT（`retryable: true`，重读当前版本后重新应用） |
| 正文为空 / 全空白 | 400 | `1202` EMPTY_CONTENT |
| 正文超长 | 413 | `1203` CONTENT_TOO_LARGE |
| 上传文件为空 | 400 | `1210` EMPTY_UPLOAD |
| 上传文件超限 | 413 | `1204` UPLOAD_TOO_LARGE |

---

#### 响应 - DocumentSourceURLResponse

| 字段 | 类型 | 说明 |
|------|------|------|
| `doc_id` | string | 文档 ID |
| `source_type` | string | 来源类型：`file`（上传文件）或 `url`（URL 导入） |
| `url` | string | 可访问的地址（预签名 URL 或原始 URL） |

---

### 5.5 网站抓取（site capture）

给一个 URL，渲染后抽取确定性的 **SiteProfile**（品牌色带角色/字体/间距/分区/关键文本/素材/截图），供下游生成类 agent 使用。产出**不进 RAG**（`index_status=skipped`），也不出现在 `GET /documents` 列表里。异步：`POST` 入队返回 `202`，`GET` 轮询，logo/hero 的 vision 描述异步回填。

| 方法 | 路径 | 说明 | 状态码 |
|------|------|------|--------|
| POST | `/knowledgebases/{kb_id}/captures` | 建抓取任务并入队 | 202 |
| GET | `/knowledgebases/{kb_id}/captures/{id}` | 轮询状态 + SiteProfile（素材/截图为预签名 URL） | 200 |
| GET | `/knowledgebases/{kb_id}/captures/{id}/export?format=hyperframes` | 导出目标格式的 `capture/` 目录 zip（见下） | 200 |

**请求 - CreateCaptureRequest**

| 字段 | 类型 | 说明 |
|------|------|------|
| `url` | string | 目标页面 URL（入队前做格式 + SSRF 校验） |
| `max_screenshots` | int \| null | 可选，覆盖本次截图上限。**当前未接线**（被忽略）；实际上限由服务端 `CAPTURE_MAX_SCREENSHOTS`（默认 10）控制 |

**响应 - CaptureResponse**

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | 抓取（Document）ID |
| `status` | string | `queued` / `capturing` / `completed` / `failed` |
| `image_status` | string | 素材落库状态（vision 逐张进度见 profile 内 `assets[].vision_status`） |
| `profile` | object \| null | 完成后为 SiteProfile；未完成为 `null` |

> **字体复用**：抓到的 `font-family` 命中部署方注入的字体目录（`FONT_CATALOG_PATH`）时，profile 里引用该目录的 CDN URL（`renderable=true`，不重存）；未命中则把 WOFF2 下载进本部署的对象存储并标 `renderable=false`。
> **导出未完成**返回 `409`；未知 `format` 返回 `422`；抓取不存在返回 `404`（`1213` `CAPTURE_NOT_FOUND`）。截图/素材存放在本部署桶的 `captures/` 前缀，建议给该前缀配生命周期规则。

#### 导出 zip 目录（`?format=hyperframes`）

产出与目标 `capture/` 格式 **1:1 对齐**。条件产出的文件在无内容时整体省略：

- `capture/meta.json` — `{id, name}`
- `capture/AGENTS.md` · `CLAUDE.md` · `.cursorrules` — 同一份 agent 导引（数据清单 + 品牌摘要）
- `capture/screenshots/scroll-NNN.png` — 逐屏滚动截图（NNN = 滚动百分比）；可选 `contact-sheet*.jpg` 缩略拼图
- `capture/assets/` — 命名品牌素材（logo/hero/og_image/favicon）、整站图片、`svgs/`、`videos/`（+预览）、`lottie/`（+预览）、`fonts/`（woff2 + `fonts.css`）
- `capture/extracted/tokens.json` — 目标格式 DesignTokens（colors/colorStats/fonts/spacing/ctas/headings/svgs/sections/cssVariables/page/ogImage）
- `capture/extracted/` 其余 — `visible-text.txt`、`asset-descriptions.md`，以及条件产出 `fonts-manifest.json` / `design-styles.json` / `page.html` / `animations.json` / `shaders.json` / `video-manifest.json` / `lottie-manifest.json`

> `asset_catalog[]` / `videos[]`（URL-only 整站素材清单）只在 profile JSON 里，**不进 zip**——下游按 URL 自取。逐字段说明与可跑示例见 [`docs/internal/capture-vtest-usage.md`](internal/capture-vtest-usage.md)。

---

### 6. 图片查询

| 方法 | 路径 | 说明 | 状态码 |
|------|------|------|--------|
| GET | `/knowledgebases/{kb_id}/documents/{doc_id}/images` | 获取文档中提取的图片列表 | 200 |
| GET | `/knowledgebases/{kb_id}/documents/{doc_id}/images/{image_id}` | 单张图片的稳定访问地址（307 跳转到实时签名） | 307 |

> **持久化图片链接请用带 `{image_id}` 的这个路径。** 列表接口返回的 `url` 是预签名地址、**会过期**，只适合当下渲染；把它写进要长期保存的内容（例如整理后经 `PUT .../edited` 存回的 Markdown）会在签名失效后变成死链。`/images/{image_id}` 每次请求现签一次，因此永不失效。响应带 `Cache-Control: no-store`，避免中间层把跳转缓存到签名过期之后。

#### 响应 - DocumentImagesListResponse

| 字段 | 类型 | 说明 |
|------|------|------|
| `doc_id` | string | 文档 ID |
| `images` | DocumentImageResponse[] | 图片列表 |

**DocumentImageResponse**

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | 图片 UUID |
| `url` | string | 预签名访问 URL（会过期；需长期保存请改用 `/images/{image_id}`） |
| `filename` | string | 文件名 |
| `index` | int | 在文档中的顺序（从 0 开始） |
| `width` | int \| null | 宽度（px） |
| `height` | int \| null | 高度（px） |
| `aspect_ratio` | string | 宽高比 |
| `description` | string | AI 生成的图片描述 |
| `vision_status` | string | `pending` / `completed` / `failed` / `skipped` |
| `alt` | string | 替代文本 |
| `context` | string | 图片周围的文本上下文 |
| `section_title` | string | 所在章节标题 |

---

### 7. 知识库查询（RAG）

| 方法 | 路径 | 说明 | 状态码 |
|------|------|------|--------|
| POST | `/knowledgebases/{kb_id}/query` | 对知识库提问 | 200 |

#### 请求 - QueryRequest

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `query` | string | 是 | 查询问题 |
| `top_k` | int | 否 | 返回的相关片段数，默认 `5` |
| `rerank` | bool | 否 | 是否启用重排序，默认 `false` |
| `query_rewrite` | bool | 否 | 是否启用查询改写，默认 `true` |

#### 响应 - QueryResponse

| 字段 | 类型 | 说明 |
|------|------|------|
| `answer` | string | LLM 生成的回答 |
| `sources` | dict[] | 引用的来源片段列表 |

> **ENABLE_INDEXING=false 时**: 返回 HTTP `503`，错误码 `1402`（`INDEXING_DISABLED`）。文档仍可通过 `GET /documents/{id}` 访问文本和图片，但无法进行向量检索。

---

## 异步处理流程

文档入库后台依次执行：

1. **文本分块** - 512 字符一块，64 字符重叠
2. **生成 Embedding** - 向量化后存入 pgvector
3. **图片下载** - 上传至 S3，创建数据库记录
4. **Vision 分析** - LLM 生成图片描述（最多 5 路并发）

文档状态流转：`indexing` → `completed` / `failed`

文档索引状态流转（`index_status`）：`pending` → `completed` / `failed` / `skipped`

图片 Vision 状态流转：`pending` → `completed` / `failed` / `skipped`

---

## 枚举值说明

| 字段 | 可选值 | 说明 |
|------|--------|------|
| `Document.status` | `indexing`, `completed`, `failed` | 文档处理状态（解析 + 存储生命周期） |
| `Document.index_status` | `pending`, `completed`, `failed`, `skipped` | 向量索引状态，与 `status` 独立。`completed`=已入 pgvector 可检索；`failed`=embedding 失败但文档仍可读；`skipped`=`ENABLE_INDEXING=false` 或无可索引内容；`pending`=处理中 |
| `DocumentImage.vision_status` | `pending`, `completed`, `failed`, `skipped` | 图片 AI 分析状态 |
