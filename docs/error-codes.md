# Vectoria 错误码对照表（前端用）

> 由 `api/errors.py` 的 `ErrorCode` + `ERROR_META` 生成，是后端的唯一真源。
> 若二者不一致，以代码为准（并请更新本文件）。

## 契约

任何失败——同步（上传/URL 提交时拒绝）或异步（worker 解析失败）——都归到一个 `error_code`。

- **同步错误**：HTTP 错误响应体 `{"code", "detail", "retryable", "suggested_action"}`。
- **异步解析失败**：`GET /documents/{id}` / 列表 / capture 响应上带 `error_code`、`retryable`、`suggested_action`（成功或历史行为 `null`）。
- `retryable`：面向用户的“手动重新提交可能有用”，**与 worker 内部队列重试无关**（`PARSE_ERROR` 落到 failed 时内部已自动重试耗尽）。
- 文案由前端按 `error_code` / `suggested_action` 出，后端只保证稳定机器码。

## suggested_action 取值

| action | 含义（建议引导） |
|---|---|
| `upload_source` | 直接上传源文件（视频/风控/登录/区域类链接抓不到） |
| `check_link` | 检查链接是否有效 |
| `use_other_network` | 换网络（区域不可达/需翻墙） |
| `retry_later` | 稍后重试 |
| `remove_password` | 去掉密码后重新上传 |
| `reduce_size` | 精简 / 拆分后重试 |
| `replace_file` | 换个文件 |
| `contact_support` | 联系支持（兜底） |
| `none` | 无特定引导 |

## 接线图例

- **✅** 本轮已真正接线，会产出该码
- **✅ 同步** 上传/URL 网关早已在用
- **⏸ 预留** 码与注册表条目都在，前端可先建映射表；raise 端后续增量接

---

## 认证类 (1001–1099)

| code | 名称 | retryable | suggested_action | 接线 |
|---|---|:---:|---|---|
| 1001 | `UNAUTHORIZED` 未认证 | ✗ | `none` | ✅ 同步 |
| 1002 | `RATE_LIMITED` 触发限流 | ✓ | `retry_later` | ✅ 同步 |
| 1003 | `FORBIDDEN` 无权限 | ✗ | `none` | ✅ 同步 |

## URL 校验类 (1101–1199) — 同步网关

| code | 名称 | retryable | suggested_action | 接线 |
|---|---|:---:|---|---|
| 1101 | `INVALID_URL` 非法 URL | ✗ | `check_link` | ✅ 同步 |
| 1102 | `UNSUPPORTED_FILE_TYPE` 格式不支持 | ✗ | `replace_file` | ✅ 同步 |
| 1103 | `BLOCKED_ADDRESS` SSRF/内网地址 | ✗ | `check_link` | ✅ 同步 |
| 1104 | `DNS_RESOLVE_FAILED` DNS 解析失败 | ✗ | `check_link` | ✅ 同步 |

## 解析 / 内容类 (1201–1299)

| code | 名称 | retryable | suggested_action | 接线 |
|---|---|:---:|---|---|
| 1201 | `PARSE_ERROR` 未知解析异常 | ✓ | `retry_later` | ✅ |
| 1202 | `EMPTY_CONTENT` 内容空白/低于阈值 | ✗ | `check_link` | ✅ |
| 1203 | `CONTENT_TOO_LARGE` 内容超字符上限 | ✗ | `reduce_size` | ✅ |
| 1204 | `UPLOAD_TOO_LARGE` 文件超大小限制 | ✗ | `reduce_size` | ✅ 同步 |
| 1205 | `PARSE_TIMEOUT` 解析超时 | ✓ | `retry_later` | ⏸ 预留 |
| 1206 | `INGEST_BUSY` 摄取繁忙 | ✓ | `retry_later` | ⏸ 预留 |
| 1207 | `MIME_MISMATCH` 扩展名与内容不符 | ✗ | `replace_file` | ✅ 同步 |
| 1208 | `PDF_TOO_MANY_PAGES` PDF 页数超限 | ✗ | `reduce_size` | ✅ 同步 |
| 1209 | `PPTX_TOO_MANY_SLIDES` PPTX 页数超限 | ✗ | `reduce_size` | ✅ 同步 |
| 1210 | `EMPTY_UPLOAD` 0 字节空文件 | ✗ | `replace_file` | ✅ 同步 |
| 1211 | `UPLOAD_NOT_FOUND` 上传失效/过期 | ✗ | `replace_file` | ✅ 同步 |
| 1212 | `UPLOAD_NOT_SUPPORTED` 上传方式不支持 | ✗ | `replace_file` | ⏸ 预留 |
| 1213 | `CAPTURE_NOT_FOUND` 截图任务未找到 | ✗ | `none` | ✅ 同步 |
| 1214 | `FILE_ENCRYPTED` 加密/带密码 | ✗ | `remove_password` | ⏸ 预留 |
| 1215 | `FILE_CORRUPTED` 文件损坏 | ✗ | `replace_file` | ⏸ 预留 |
| 1216 | `SCANNED_NEEDS_OCR` 纯图扫描件 | ✗ | `none` | ⏸ 预留 |
| 1217 | `EDIT_NOT_FOUND` 文档无编辑版本 | ✗ | `none` | ✅ |
| 1218 | `EDIT_NOT_SUPPORTED` 该类型不支持编辑 | ✗ | `none` | ✅ |
| 1219 | `EDIT_REVISION_CONFLICT` base_revision 过期 | ✓ | `retry_later` | ✅ |
| 1299 | `PARSE_UNRESOLVABLE` permanent 兜底 | ✗ | `contact_support` | ✅ |

## 资源 / 查询类 (1301–1499)

| code | 名称 | retryable | suggested_action | 接线 |
|---|---|:---:|---|---|
| 1301 | `NOT_FOUND` 资源不存在 | ✗ | `none` | ✅ 同步 |
| 1401 | `QUERY_ERROR` 查询出错 | ✓ | `retry_later` | ✅ 同步 |
| 1402 | `INDEXING_DISABLED` 检索写侧已关 | ✗ | `none` | ✅ 同步 |

## 链接抓取类 (1501–1599) — 异步，本轮新增

| code | 名称 | retryable | suggested_action | 接线 |
|---|---|:---:|---|---|
| 1501 | `LINK_VIDEO_UNSUPPORTED` 短视频/播放器链接 | ✗ | `upload_source` | ✅ 黑名单 + 小红书视频笔记 |
| 1502 | `LINK_LOGIN_REQUIRED` 需要登录 | ✗ | `upload_source` | ⏸ larksuite 静态命中已接；通用探测预留 |
| 1503 | `LINK_ANTIBOT_BLOCKED` 风控/人机验证 | ✗ | `upload_source` | ✅ detect_block_reason |
| 1504 | `LINK_REGION_BLOCKED` 区域不可达/需翻墙 | ✗ | `use_other_network` | ✅ UNREACHABLE_DOMAINS |
| 1505 | `LINK_PAGE_GONE` 页面已删除 (404/410) | ✗ | `check_link` | ✅ |
| 1506 | `LINK_FORBIDDEN` 站点拒绝 (403) | ✗ | `check_link` | ⏸ 预留（无独立 raise 点） |
| 1507 | `LINK_FETCH_TIMEOUT` 连接超时/网络错误 | ✓ | `retry_later` | ✅ |

## 通用兜底 (9001–9099)

| code | 名称 | retryable | suggested_action | 接线 |
|---|---|:---:|---|---|
| 9001 | `VALIDATION_ERROR` 参数校验失败 | ✗ | `none` | ✅ 同步 |
| 9999 | `INTERNAL_ERROR` 内部错误 | ✓ | `retry_later` | ✅ 同步 |

---

## 说明与已知取舍

- **`indexing_error`（向量化/索引失败）不进错误码体系**：文档仍 `status="completed"`、内容可读可搜，只在 `index_status="failed"` 上体现，不给 `error_code`。
- **域名反爬冷却**：`parsers/url/__init__.py` 把“域名冷却中”也抛成 `AntiBotBlockedError`→`1503`（标不可重试）。冷却其实会过期，属软性可重试，本轮维持现状。
- **`1216 SCANNED_NEEDS_OCR` 的 action = `none`** 是占位（代码里有 TODO），待扫描件/OCR 探测落地后改成合适的动作。
- **`1219 EDIT_REVISION_CONFLICT` 是唯一可重试的编辑类错误**，但重试方式不是原样重发：应先 `GET .../edited` 读到当前 `revision`，把编辑重新应用在当前版本之上再写。`1217`/`1218` 描述的是文档的既成事实，重试没有意义。
- **预留码**（1205 / 1206 / 1212 / 1214 / 1215 / 1216 / 1506 / 1502 通用探测）目前无对应 raise 点，前端可先纳入映射表，后续增量接线时即生效，无需前端改动。
