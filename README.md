# skewpipe — 倾斜拆分与安全合并

一个**零依赖、可验证**的虚构数据管道：分发器 → 多个聚合进程 → 结果发布服务。
它演示的是流式聚合里最难处理的那一类问题：某些分组特别大，普通按键分区会让单个
聚合进程长时间卡住；拆分之后，不同指标的局部结果**不能用同一种相加方法**合并回去。

- 后端：Python 3.11 标准库（`http.server`），无第三方依赖
- 前端：原生 ES2020 + 手写 SVG 图表，无框架、无 CDN
- 数据完全虚构，由种子生成；每个键的真值在内存里精确已知，所以"可验证"不是口号

## 运行

```bash
python3 -m skewpipe.server            # 打开 http://127.0.0.1:8000/
python3 -m skewpipe.server --port 8140 --host 0.0.0.0
```

无浏览器的验证入口：

```bash
python3 -m skewpipe.demo              # 3 种分区 × 4 种指标的真值对照表
python3 -m skewpipe.demo --mode split --metric distinct --detail
python3 -m skewpipe.demo --failures   # 退出进程 / 重复回报 / 旧拆分回放
python3 -m skewpipe.demo --strict     # 任一结果偏离真值则退出码非 0
```

测试（标准库 `unittest`，约 15 秒）：

```bash
python3 -m unittest discover -s tests -t .
```

## 管道形状

```
源数据行
   │  分发器按窗口准入，给每行分配 identity
   ▼
DistributionService ──► AggregateProcess × N ──► ResultPublisher
   │  naive:    identity = key             │  按指标规则合并
   │  preroute: identity = key#p(lane)     │  只接受自己契约内的局部结果
   │  split:    运行中创建 SplitTask        │  代次 / 代码版本 / 任务存活
   ▼                                         ▼
in-flight（已准入、未聚合）              每个基础键的最终结果 + 真值
```

三种模式故意放在一起对照：

| 模式 | 行为 | 代价 |
| --- | --- | --- |
| `naive` 朴素哈希分区 | `hash(key)` 决定归属，热点键整键压在一个进程 | 单进程积压 3,380 行，约 99 tick |
| `preroute` 入口预路由 | 准入前就把热点键改写成 `key#pN` | 必须一开始就知道会倾斜；已有证据无法搬运 |
| `split` 运行时拆分重键 | 从**已观测行数**发现倾斜，运行时创建任务、搬运证据、重键未聚合行 | 需要任务契约与代次校验；峰值积压约 146 行，51 tick |

页面上的"倾斜识别"面板只使用**已观测行数占比**（如 `HOT: 342/720 观测行 = 48% ≥ 阈值 15%`），
不读取真值，因此拆分动机与真实流式系统一致。

## 四种指标的合并代数（本项目的核心）

同一批分片局部结果，四种指标的证据形状与合并规则都不同：

| 指标 | 局部证据 | 合并规则 | 直接相加会怎样 |
| --- | --- | --- | --- |
| `count` | 计数 `n` | `Σn` | 正确 |
| `sum` | `n, sum` | `Σsum` | 正确 |
| `avg` | **分子 + 分母**（`sum, cnt`） | `Σsum / Σcnt` | "均值的均值"被分片大小带偏 |
| `distinct` | **成员集合** | `∪members` 后取基数 | 交集成员被重复计数 |

真实数字（种子 7，热点键 HOT，5,520 行）：

- `distinct`：合并 **2,149** = 真值 2,149；"各分片去重数相加" = **4,199**，偏差 **2,050**
- `avg`：合并 49.8348 = 真值；"各分片均值再平均"给出错误值
- `count` / `sum`：两种方法都对——这正是"不能套同一种方法"的反面例证

页面的"重键的局部贡献与合并"卡片把每个分片的局部值、按规则合并后的值、真值
（虚线参考线）和朴素相加的错误值并排显示。

## 拆分任务的契约（旧任务不能污染新结果）

`SplitTask` 不只是一个路由提示，而是一份契约：

```text
split_id           任务标识（split-01, split-02 …）
inc                代次：进程重启后旧代全部失效
code               代码版本：滚动升级后旧版本局部结果失效
lanes              base#p0 … base#p{f-1}
lane_digest        绑定整个分片集合（重拆分后必然不同）
epoch              同一基础键的第几次拆分
```

发布器按顺序拒绝（`_reject_reason`）：

1. `foreign-incarnation` — 代次不符
2. `stale-code` — 代码版本不符
3. `unknown-split` — 引用了不存在的任务
4. `retired-split` — 任务已被新任务取代
5. `lane-mismatch` — 分片名不在该任务的 lane 集合里
6. `duplicate-publish` — `event_id` 已经合并过（至少一次投递去重）

页面上"旧拆分任务结果""旧代局部结果""重复回报"三个按钮就是逐条触发这些拒绝；
时间线和"拒绝的局部结果"会按原因分类显示，而最终结果仍与真值一致。

## 拆分顺序即正确性保证

`Run.start_split()` 的步骤顺序就是它不丢不重的证明：

1. **冲刷**所有聚合进程的 delta，并把发布队列排空——已聚合行的证据此时只存在于发布器
2. 按指标规则把发布器里该键的证据合并成 `carry`
3. 注册新任务（同时把旧任务标为 retired）
4. 从发布器和聚合进程里**清除**旧 identity
5. 把**已准入但未聚合**的行（队列 + in-flight）重键到新 lane——这些行还没有证据，移动不会重复
6. 把 `carry` 切成新 lane 的局部证据并发布；同时**并入各 lane 的本地状态**，使发布器重启后仍可重建

第 6 步的切分也按指标区分：`count/sum` 直接拆分；`avg` 拆分子同时拆分母；
`distinct` 把成员轮流分到各 lane，并集不变。

## 故障语义

- **工作进程退出**：delta 每 3 个 tick 才发布一次，因此进程里"已折叠未发布"的行会丢失。
  这些行会记入 `orphaned`，`重放丢失行` 从上游持久窗口重放它们；不重放时，丢失是真实可见的
  （进度条与结果都会少）。这是有代价的失败恢复，而不是"失败免费"。
- **发布服务退出**：聚合进程保留完整本地状态，重启后每个进程把状态作为一条完整局部结果重发，
  发布器用它们**替换**（而非累加）自己的状态，结果仍然精确。
- **暂停进程**：积压持续增长，恢复后追上；用状态色 + 图标标注停滞进程。

## 目录

```text
skewpipe/combine.py   四种指标的局部状态、delta、证据形状与合并规则
skewpipe/split.py     SplitTask 契约与任务注册表（代次、lane 摘要、退役）
skewpipe/events.py    代次注册表与事件时间线
skewpipe/engine.py    源数据、分发器、聚合进程、发布器、运行与视图
skewpipe/server.py    标准库 HTTP 服务与 JSON API
skewpipe/demo.py      无浏览器的对照表与故障演练
web/                  仪表盘（index.html / app.js / style.css）
tests/                合并代数、管道端到端、HTTP 接口
```

## 确定性

调度使用虚拟时钟（1 tick = 1 秒），没有真实并发，也不依赖启动顺序。
同一组 `(mode, metric, strategy, aggregators, fanout, seed)` 永远产生同样的
tick 数、队列曲线与结果——测试里对整条 history 做了逐条比较。
