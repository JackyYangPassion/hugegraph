# 第一周执行手册：快速启动

> 此文档是 `CURSOR_3MONTH_PLAN.md` 的执行级拆解，聚焦第一周的具体任务

---

## Day 1-2: 测试体系摸底 & 快速修复

### Task 1: 修复不稳定测试 [HIGH PRIORITY]

**目标文件**:
- `hugegraph-pd/hg-pd-test/` — 找到所有 `@Disabled` 或 `@Ignore` 的测试
- `hugegraph-store/hg-store-test/` — 修复 jraft 相关不稳定测试
- `hugegraph-server/hugegraph-test/` — 启用被跳过的测试套件

**Cursor 操作方式**:
```
1. 用 explore Agent 搜索所有 @Disabled, @Ignore, @Skip 注解
2. 用 generalPurpose Agent 分析每个被跳过测试的失败原因
3. 分批修复，每个修复一个独立 PR
```

### Task 2: CI 快速优化 [MEDIUM PRIORITY]

**目标文件**:
- `.github/workflows/server-ci.yml`
- `.github/workflows/pd-store-ci.yml`

**具体变更**:
1. 移除 Java 8 安装步骤（`server-ci.yml` 中的残留）
2. 添加 Maven 构建缓存
3. 修复 Codecov 配置

---

## Day 3-4: 代码质量快速提升

### Task 3: 依赖安全升级 [HIGH PRIORITY]

**Sofa-RPC 升级路径**:
```
当前: 5.7.6
目标: 5.12.x
影响模块: hugegraph-commons/hugegraph-rpc
```

**Cursor 操作**:
1. Agent 分析 Sofa-RPC 5.7.6 → 5.12 的 breaking changes
2. Agent 生成兼容性适配代码
3. Agent 运行全量测试验证

### Task 4: README & 文档修复 [LOW PRIORITY]

- 修复 CI badge 链接（旧的 `ci.yml` → `server-ci.yml`）
- 更新过时的架构说明
- 补全模块 README

---

## Day 5: 第一批 PR 提交 & Review

### PR 提交清单

| # | 标题 | 类型 | 优先级 |
|---|------|------|--------|
| 1 | fix(test): re-enable disabled PD test suites | test | P0 |
| 2 | fix(test): stabilize jraft-related store tests | test | P0 |
| 3 | ci: add Maven build cache and remove Java 8 step | ci | P1 |
| 4 | chore: upgrade sofa-rpc from 5.7.6 to 5.12.x | deps | P1 |
| 5 | docs: fix CI badge links in README | docs | P2 |

### PR 写作规范
- 标题格式: `type(scope): description` (遵循现有 commit 风格)
- Body 包含: Background, Changes, Testing, Related Issues
- 每个 PR 控制在 300 行以内（便于 review）
- 附上 Cursor AI 辅助开发的说明（建立社区认知）

---

## Token 消耗预估 (Week 1)

| 活动 | 预估 Token | 说明 |
|------|-----------|------|
| 代码库全量扫描 | ~20亿 | 理解 1740 个 Java 文件 |
| 测试分析和修复 | ~15亿 | 深入分析测试失败原因 |
| CI 配置优化 | ~5亿 | 分析 workflow yaml |
| 依赖升级分析 | ~10亿 | API 兼容性分析 |
| PR 编写和 review | ~5亿 | 文档和代码 review |
| **小计** | **~55亿** | |

---

## 成功标准

- [ ] 5+ PR 提交到上游
- [ ] 至少 2 个 PR 被 merge
- [ ] 0 个新引入的测试失败
- [ ] 建立与至少 1 个核心 reviewer 的沟通
- [ ] Token 使用量 > 30亿

---

*此执行手册每周更新，Week 2 手册在 Week 1 结束时生成*
