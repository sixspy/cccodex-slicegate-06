# 日切放行

> 遵守 `ops/底座生成前必读_效率与技术栈.md`。FastAPI + Vue 3 + SQLite，交付须 docker compose 一键起；库用 SQLite，不挂外置 Postgres。  
> **禁止同构换皮**：本仓主链路、表结构、接口与页面均独立，不得与其它仓共用同一套 `/api/items` 壳。

## 骨架一句话

业务日分区切片登记后裁决完整度，完整才放行下游，并与消费水位对账

## 基本信息

| 项 | 值 |
|----|-----|
| 产品名（首页） | 日切放行 |
| 目录 | `04-slicegate` |
| 领域建议 | 数仓与数据湖 |
| 接口 | http://127.0.0.1:10616 |
| 库 | `data/app.db` |
| 账号 | admin / 123456 |

## 为何能出多题

迟到切片、重切、跨分区依赖、部分完整冻结、消费水位回拨

## 水位回拨能力（同库账本，不另起站点）

回拨与放行态、成功流水、消费水位在同一个 SQLite 事务内同成同败；甲乙口径互斥，先在回拨规则簿择一锁定，当日禁止改口，偏离即拒绝并在账本记「回拨口径偏了」。

| 口径 | 水位 | 切片 | 成功流水 | 再放行 |
|------|------|------|----------|--------|
| 甲 · 软回拨 | 目标日及之后有效水位标「已回拨(软)」，侧栏当前水位重算 | 改回「可复检未放行」 | 保留只读史，lifecycle 标「已回拨(软)」 | 复检后可再次放行 |
| 乙 · 硬回拨作废 | 水位标「已撤回(硬)」 | 标「硬作废-须新切片」 | lifecycle 标「硬作废」，不抹账 | 同切片号禁止再次成功放行，须新切片重走完整度 |

- 规则簿 `rollback_policy`（按日唯一）、回拨账本 `rollbacks`（成功/拒绝/偏口径/争用全留痕，含影响流水与切片号）
- 同一消费方并发回拨：进程内消费方锁串行 + `BEGIN IMMEDIATE` 库写锁；后手按「回拨争用禁默许」拒绝并留痕
- 空回拨（窗口内无有效成功放行/水位）拒绝并留痕；禁止只改水位数字、禁止抹审计流水
- 回拨目录、水位侧栏、切片清单、处置画面均走同一 `data/app.db` 的 API，可交叉核对

接口：`GET/POST /api/rollback/policy`、`POST /api/rollback`、`GET /api/rollback`（均需登录 Bearer）。

## 避开

别做成作业拓扑编排换皮；别做成纯水位大盘

## 互斥切口候选

- 迟到切片整批拒收 vs 进待补齐冻结区
- 重切失败整笔回滚 vs 保留已完整分区
- 放行与水位同步闸 vs 异步放行后纠偏

## 启动（一键）

`powershell
cd d:\document\cccodex\workspace\slicegate\slicegate-06
docker compose up -d --build
`

浏览器：http://127.0.0.1:10616/  
健康检查：GET http://127.0.0.1:10616/api/health

本机开发（可选）：

`powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --app-dir backend --host 127.0.0.1 --port 10616
python -m pytest -q
`
