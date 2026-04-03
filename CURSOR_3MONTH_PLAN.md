# Apache HugeGraph × Cursor AI 三个月深度共建计划

> **目标**: 3个月内消耗数百亿 token，产出具有行业影响力的成果
> **方法论**: 以 Cursor AI 作为核心生产力引擎，实现 10x 工程效率
> **定位**: 证明 AI-Native 开发模式可以在顶级开源项目中产出世界级贡献

---

## 一、战略定位与影响力路径

### 为什么是 HugeGraph？

1. **Apache 顶级项目** — 贡献直接进入 Apache 基金会官方项目，全球可见度
2. **图数据库赛道火热** — Neo4j 估值 $2B+，国产图数据库竞争激烈，HugeGraph 是 Apache 唯一的图数据库
3. **技术深度够** — ~1,740 Java 文件、7大模块、分布式架构，足以支撑大规模 AI 辅助开发
4. **改进空间巨大** — 分布式组件处于 BETA、Cypher 实现薄弱、测试覆盖不足、性能优化空间大

### 影响力衡量标准

| 维度 | 目标 |
|------|------|
| **代码贡献** | 100+ PR merged，成为 Top 3 贡献者 |
| **功能交付** | 3-5 个重大特性进入 release |
| **社区影响** | 10+ 篇技术博客/分享，GitHub Star 增长显著 |
| **Token 消耗** | 300亿+ token（Cursor AI 深度使用的里程碑数据） |
| **AI 开发范式** | 输出 "AI-Native 开发方法论" 系列内容 |

---

## 二、三阶段执行计划

---

### 第一阶段：基础设施 & 快速积累（第1-4周）

> **主题**: 打地基、建信任、积累 token
> **Token 预估**: ~80亿（大量代码阅读、理解、重构）

#### 1.1 测试体系全面重建 [预计 40+ PR]

**背景**: 当前测试体系存在重大结构性问题

- 测试源码放在 `src/main/java/` 而非标准 `src/test/java/`
- PD/Store 模块多个测试套件标记为 "unstable" 或被注释掉
- 服务端 CI 只跑 memory/rocksdb/hbase 三种后端
- Cypher 仅有极少的冒烟测试
- 集群测试（hugegraph-cluster-test）仅 28 个 Java 文件

**行动项**:

```
Week 1-2: 测试结构迁移
├── 将 hugegraph-test 的测试从 src/main/java → src/test/java（保持向后兼容）
├── 修复 hg-pd-test 中被禁用的测试套件
├── 修复 hg-store-test 中 jraft 相关的不稳定测试
└── 统一测试命名规范，添加 @Tag 分类标注

Week 3-4: 测试覆盖率提升
├── hugegraph-core 核心路径覆盖率 → 60%+
├── hugegraph-api 端点全覆盖测试
├── 分布式路径（PD/Store）集成测试补全
├── 添加性能回归测试基线（JMH benchmarks）
└── 配置 JaCoCo 阈值门控到 CI
```

**Cursor AI 深度使用方式**:
- 用 Agent 批量阅读所有现有测试 → 生成测试覆盖率 gap 分析报告
- 用 Agent 为每个 public API 自动生成测试用例
- 用多 Agent 并行处理不同模块的测试迁移

#### 1.2 CI/CD 现代化 [预计 10+ PR]

**背景**: CI 配置存在多处 TODO 和技术债务

```
行动项:
├── 升级 CI runner 从 ubuntu-22.04 到 LTS
├── 移除 Java 8 兼容性残留（已迁移到 Java 11+）
├── 合并 pd-store-ci.yml 冗余配置
├── 添加 Raft 模式自动化测试（当前仅在特定分支触发）
├── 集成 SonarQube / CodeQL 深度扫描
├── 添加构建缓存（Maven cache + Docker layer cache）
├── Codecov action 升级到 v5+
└── 添加 nightly benchmark CI（性能回归自动检测）
```

#### 1.3 代码质量 & 依赖治理 [预计 15+ PR]

```
行动项:
├── Sofa-RPC 从 5.7.6 升级到 5.12+（安全修复，commons/AGENTS.md 已标记）
├── 统一日志框架使用（消除直接 System.out 调用）
├── 修复所有 Deprecated API 调用
├── README badges 修复（引用了旧的 ci.yml 而非 server-ci.yml）
├── 消除跨模块循环依赖
└── 统一异常处理模式
```

---

### 第二阶段：核心特性突破（第5-8周）

> **主题**: 高价值特性开发，建立技术影响力
> **Token 预估**: ~120亿（大量代码生成、架构设计、迭代优化）

#### 2.1 🔥 Cypher 引擎重构（旗舰项目）[预计 30+ PR]

**背景**: 当前 Cypher 实现仅是 `opencypher-gremlin:1.0.4` 的薄包装层，只有两个核心类

**目标**: 将 HugeGraph 的 Cypher 支持从 "Demo 级别" 提升到 "生产可用"

```
Phase A (Week 5-6): Cypher 解析器增强
├── 升级 opencypher-gremlin 翻译层或引入自研解析器
├── 支持完整的 MATCH/WHERE/RETURN/WITH/UNWIND 语法
├── 支持 MERGE、DELETE、SET 等写操作
├── 支持子查询和 OPTIONAL MATCH
└── 支持 Cypher 聚合函数 (count, sum, avg, collect 等)

Phase B (Week 7-8): Cypher 优化器
├── 实现 Cypher → HugeGraph 原生查询计划的直接翻译（绕过 Gremlin）
├── 基于 HugeGraph 索引信息的查询优化
├── 支持 EXPLAIN 和 PROFILE 语句
├── 建立 Cypher 兼容性测试套件（参考 openCypher TCK）
└── 性能基准：Cypher vs 等价 Gremlin 查询
```

**Cursor AI 使用方式**:
- 用 Agent 分析 Neo4j 和 openCypher 的公开规范
- 用 Agent 并行生成语法树节点实现
- 用 Agent 自动生成 TCK 兼容性测试
- 用多 Agent 并行处理不同 Cypher 子句的翻译

#### 2.2 🔥 查询性能优化引擎 [预计 20+ PR]

**背景**: `hugegraph-core` 已有基础的遍历优化策略，但仍有大量优化空间

```
行动项:
├── 查询计划可视化（EXPLAIN 输出优化）
├── 智能索引选择器（基于统计信息的索引推荐）
├── 批量查询优化（multi-get, batch scan）
├── 大结果集流式返回（减少内存占用）
├── 缓存策略优化
│   ├── 二级缓存（进程内 + 分布式）
│   ├── 查询结果缓存（基于 query fingerprint）
│   └── Schema 缓存预热
├── 并行图遍历（利用多核 CPU）
└── JMH 性能基准测试套件
```

#### 2.3 分布式组件稳定化 [预计 20+ PR]

**背景**: PD + Store 标记为 BETA，存在多个已知问题

```
行动项:
├── PD 高可用增强
│   ├── Leader 选举异常处理（已有 NPE 修复 #2919）
│   ├── 分区再均衡策略优化
│   ├── 元数据一致性验证
│   └── PD 监控指标完善
├── Store 稳定性
│   ├── Raft 日志压缩和快照优化
│   ├── 节点上下线平滑处理
│   ├── 数据修复工具
│   └── 反压机制（gRPC streaming）
└── 分布式测试
    ├── 混沌测试框架（网络分区、节点故障）
    ├── 长时间稳定性测试
    └── 分布式性能基准
```

---

### 第三阶段：生态扩展 & 影响力输出（第9-12周）

> **主题**: 生态建设、输出方法论、建立行业影响力
> **Token 预估**: ~100亿（跨项目协作、文档生成、博客写作）

#### 3.1 🔥 Graph + AI 深度集成 [预计 20+ PR]

**背景**: `hugegraph-ai` 项目已存在但与核心服务器的集成不够深入

```
行动项:
├── 服务端原生 Vector 属性支持
│   ├── 在 PropertyKey 中添加 VECTOR 数据类型
│   ├── 向量索引存储（基于 RocksDB/HStore）
│   └── 近似最近邻（ANN）查询 API
├── GraphRAG 原生支持
│   ├── 知识图谱自动构建 API
│   ├── 子图检索 API（for RAG pipeline）
│   ├── 图 + 向量混合查询
│   └── LLM 友好的查询结果格式
├── NL2Gremlin/NL2Cypher 服务端集成
│   ├── 自然语言查询 REST endpoint
│   ├── 查询意图识别和路由
│   └── 查询结果自然语言解释
└── AI 数据管道
    ├── Embedding 计算集成
    ├── 图特征提取 API
    └── 与 LangChain/LlamaIndex 的原生集成示例
```

#### 3.2 开发者体验革命 [预计 15+ PR]

```
行动项:
├── 全新 CLI 工具（替代旧的 hugegraph-tools）
│   ├── 交互式图操作
│   ├── 数据导入/导出向导
│   └── 集群管理命令
├── Docker Compose 一键部署优化
│   ├── 单机开发模式
│   ├── 3节点分布式模式
│   └── 带 Hubble 可视化的全栈模式
├── API 文档自动生成（OpenAPI/Swagger）
├── 性能调优指南（基于实际基准测试数据）
└── 多语言 SDK 示例更新
```

#### 3.3 影响力输出 [内容创作]

```
输出计划:
├── 技术博客系列（10+ 篇）
│   ├── "AI-Native 开发实践: 如何用 Cursor 3个月重塑 Apache 顶级项目"
│   ├── "从 Demo 到生产: HugeGraph Cypher 引擎重构全记录"
│   ├── "图数据库性能优化: 从查询计划到缓存策略"
│   ├── "分布式图数据库的混沌测试实践"
│   ├── "Graph + AI: 知识图谱在 RAG 中的工程实践"
│   ├── "几百亿 Token 的启示: AI 辅助编程的边界在哪里"
│   └── ...更多主题
├── 社区演讲
│   ├── Apache Community 月度分享
│   ├── 国内技术大会投稿
│   └── Cursor/AI 开发者社区分享
└── 开源贡献报告
    ├── 月度贡献 review
    ├── 季度影响力报告
    └── AI-Native 开发方法论白皮书
```

---

## 三、Token 消耗详细预估

### 按任务类型分解

| 任务类型 | 单次 Token | 频次/天 | 天数 | 小计 |
|----------|-----------|---------|------|------|
| **代码阅读理解** (Agent 分析大文件) | 50K-200K | 30-50次 | 90 | ~600亿输入 |
| **代码生成** (测试、特性、重构) | 20K-100K | 20-40次 | 90 | ~300亿 |
| **代码审查** (Agent review PR) | 30K-150K | 10-20次 | 90 | ~150亿 |
| **调试排错** (Agent 分析日志/错误) | 20K-80K | 10-20次 | 90 | ~100亿 |
| **文档/博客** (内容生成和迭代) | 10K-50K | 5-10次 | 90 | ~40亿 |
| **架构讨论** (Agent 设计评审) | 30K-100K | 5-10次 | 90 | ~50亿 |

**总计预估: ~300-500亿 token**

### Token 效率最大化策略

1. **多 Agent 并行**: 不同模块同时分析/开发，并行消耗 token
2. **深度上下文**: 每次会话加载完整模块上下文，不做碎片化提问
3. **迭代优化**: 对生成的代码进行多轮 review-refine 循环
4. **探索式开发**: 使用 Cursor 的 explore 模式大量探索代码库
5. **Best-of-N**: 关键组件使用多分支并行开发，选最优方案

---

## 四、里程碑与关键节点

### Month 1 里程碑
- [ ] 测试覆盖率：核心模块 > 50%
- [ ] CI 构建时间 < 15 分钟
- [ ] 修复所有 "unstable" 标记的测试
- [ ] Sofa-RPC 升级完成
- [ ] 产出 20+ PR merged
- [ ] 发布第 1 篇技术博客

### Month 2 里程碑
- [ ] Cypher 引擎 MVP：支持 80% 常用查询语法
- [ ] 查询性能提升 30%+（benchmark 验证）
- [ ] PD/Store 模块移除 BETA 标签的技术条件达成
- [ ] 产出 40+ PR merged（累计 60+）
- [ ] 发布第 3-5 篇技术博客

### Month 3 里程碑
- [ ] Vector 属性 + ANN 查询 MVP
- [ ] GraphRAG API 原型
- [ ] 完整的 Docker Compose 分布式部署方案
- [ ] 产出 40+ PR merged（累计 100+）
- [ ] 方法论白皮书初稿
- [ ] 社区演讲 1-2 次

---

## 五、风险与应对

| 风险 | 概率 | 影响 | 应对策略 |
|------|------|------|----------|
| PR review 瓶颈（社区 reviewer 有限） | 高 | 高 | 主动在 Slack 推进；先提小 PR 建立信任 |
| Cypher 重构复杂度超预期 | 中 | 高 | MVP 先行；分阶段交付；保持与 Gremlin 翻译的回退方案 |
| 分布式测试环境搭建困难 | 中 | 中 | 利用 Docker Compose；必要时申请社区测试资源 |
| AI 生成代码质量被质疑 | 低 | 高 | 严格 code review；完善的测试覆盖；增量提交 |
| Token 预算不足 | 低 | 中 | 优化 prompt 策略；聚焦高 ROI 任务 |

---

## 六、Cursor AI 使用最佳实践（方法论核心）

### 6.1 Agent 使用模式矩阵

| 场景 | Agent 类型 | 使用方式 |
|------|-----------|----------|
| 代码探索 | explore (quick/medium/thorough) | 快速定位代码、理解架构 |
| 特性开发 | generalPurpose | 完整的设计 → 实现 → 测试 |
| 并行实验 | best-of-n-runner | 关键设计决策的多方案对比 |
| 代码审查 | generalPurpose (readonly) | PR 级别的代码审查 |
| 重构迁移 | generalPurpose | 大规模代码迁移和重构 |

### 6.2 Prompt 工程策略

```
高效 Prompt 结构:
1. Context: 提供精确的文件路径和代码范围
2. Goal: 明确的单一目标
3. Constraints: 代码风格、性能要求、兼容性约束
4. Examples: 类似的已有实现作为参考
5. Verification: 定义"完成"的标准
```

### 6.3 Daily Workflow

```
每日流程:
09:00 - 用 explore Agent 回顾昨日 PR 状态和上游变更
09:30 - 启动 2-3 个 generalPurpose Agent 并行开发不同任务
12:00 - Review Agent 产出，手动调整和合并
14:00 - 启动测试 Agent，验证上午的变更
15:00 - 用 Agent 编写/更新文档和测试
17:00 - PR 提交、代码 review 反馈处理
18:00 - 整理当日 token 使用量和产出效率数据
```

---

## 七、贡献路线图总览

```
            Month 1                    Month 2                    Month 3
    ┌─────────────────────┐   ┌─────────────────────┐   ┌─────────────────────┐
    │  🔧 测试体系重建     │   │  🚀 Cypher 引擎重构  │   │  🤖 Graph + AI 集成  │
    │  🔧 CI/CD 现代化     │   │  🚀 查询性能优化     │   │  📦 开发者体验优化    │
    │  🔧 代码质量治理     │   │  🚀 分布式稳定化     │   │  📝 方法论输出       │
    │                     │   │                     │   │  🎤 社区影响力       │
    │  产出: 20+ PR       │   │  产出: 40+ PR       │   │  产出: 40+ PR       │
    │  Token: ~80亿       │   │  Token: ~120亿      │   │  Token: ~100亿      │
    └─────────────────────┘   └─────────────────────┘   └─────────────────────┘
                          ↓                          ↓
                    信任建立完成                 核心贡献者身份确立
```

---

## 八、为什么这个计划有影响力？

1. **规模前所未有**: 在 Apache 顶级项目中，用 AI 完成 100+ PR 的系统性贡献，没有先例
2. **技术深度**: Cypher 引擎、分布式系统、向量检索，每一项都是硬核技术
3. **方法论价值**: "AI-Native 开发" 的实证数据（token 用量、效率对比、质量指标）
4. **时机正确**: 图数据库 + AI 是 2025-2026 最热的技术交叉点
5. **数据说话**: 几百亿 token 的使用数据本身就是一个故事

---

*计划创建时间: 2026-04-03*
*基于 Apache HugeGraph v1.7.0 代码库分析*
*目标分支: cursor/historical-cursor-plan-328e*
