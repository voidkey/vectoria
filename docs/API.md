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
| GET | `/knowledgebases/{kb_id}/documents` | 列出知识库下所有文档 | 200 |
| GET | `/knowledgebases/{kb_id}/documents/{doc_id}` | 查询单个文档状态 | 200 |
| DELETE | `/knowledgebases/{kb_id}/documents/{doc_id}` | 删除文档及其向量数据 | 204 |

> **注意**: 文档入库是**异步处理**的，接口立即返回 `status: "indexing"`，需轮询单文档接口 `GET /knowledgebases/{kb_id}/documents/{doc_id}` 检查进度。

#### 请求 - DocumentURLRequest（URL 入库）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `url` | string | 是 | 文档 URL |

#### 响应 - DocumentIngestResponse（入库时返回）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | 文档 UUID |
| `kb_id` | string | 所属知识库 ID |
| `title` | string | 文档标题 |
| `source` | string | 来源（URL 或文件名） |
| `chunk_count` | int | 分块数量（初始为 0，完成后更新） |
| `status` | string | 状态：`indexing` / `completed` / `failed` |
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
| `error_msg` | string | 错误信息 |
| `created_at` | string | 创建时间（ISO 8601 格式） |

> **注意**: 查询单个文档详情 `GET /knowledgebases/{kb_id}/documents/{doc_id}` 返回的是 `DocumentIngestResponse`，包含 `content`、`outline`、`image_count` 等额外字段。

---

### 5. 文档原始来源

| 方法 | 路径 | 说明 | 状态码 |
|------|------|------|--------|
| GET | `/knowledgebases/{kb_id}/documents/{doc_id}/source_url` | 获取文档的原始文件地址或 URL | 200 |

> 对于**上传的文件**，返回 S3 预签名下载链接；对于 **URL 导入的文档**，返回原始 URL。

#### 响应 - DocumentSourceURLResponse

| 字段 | 类型 | 说明 |
|------|------|------|
| `doc_id` | string | 文档 ID |
| `source_type` | string | 来源类型：`file`（上传文件）或 `url`（URL 导入） |
| `url` | string | 可访问的地址（预签名 URL 或原始 URL） |

---

### 6. 图片查询

| 方法 | 路径 | 说明 | 状态码 |
|------|------|------|--------|
| GET | `/knowledgebases/{kb_id}/documents/{doc_id}/images` | 获取文档中提取的图片列表 | 200 |

#### 响应 - DocumentImagesListResponse

| 字段 | 类型 | 说明 |
|------|------|------|
| `doc_id` | string | 文档 ID |
| `images` | DocumentImageResponse[] | 图片列表 |

**DocumentImageResponse**

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | 图片 UUID |
| `url` | string | 预签名访问 URL |
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

---

## 异步处理流程

文档入库后台依次执行：

1. **文本分块** - 512 字符一块，64 字符重叠
2. **生成 Embedding** - 向量化后存入 pgvector
3. **图片下载** - 上传至 S3，创建数据库记录
4. **Vision 分析** - LLM 生成图片描述（最多 5 路并发）

文档状态流转：`indexing` → `completed` / `failed`

图片 Vision 状态流转：`pending` → `completed` / `failed` / `skipped`

---

## 枚举值说明

| 字段 | 可选值 | 说明 |
|------|--------|------|
| `Document.status` | `indexing`, `completed`, `failed` | 文档处理状态 |
| `DocumentImage.vision_status` | `pending`, `completed`, `failed`, `skipped` | 图片 AI 分析状态 |
