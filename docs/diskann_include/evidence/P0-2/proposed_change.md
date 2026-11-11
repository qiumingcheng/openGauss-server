# P0-2 实施方案（Bash 分类器故障期间准备；P0-1 关闭后套用）

## 补丁 1：diskannrescan_internal（diskannscan.cpp:51-66）

old:
```cpp
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    so->curpos = 0;
}
```
new:
```cpp
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    /*
     * [P0-2] Full per-query reset. Baseline only reset curpos, so candidates,
     * the visited set and iteration state leaked across rescans (prepared
     * statements, nested-loop inner scans): a second execution with different
     * parameters returned stale candidates from the previous query first.
     */
    so->curpos = 0;
    so->candidates.clear();
    so->ncandidates = 0;
    so->curIterNum = 0;
    so->nexpextedCandidates = 0;
    blockhash_reset(so->blocks);
}
```

## 补丁 2：diskannsearch（diskannscan.cpp:167-178，roundVisited 语义）

old:
```cpp
    queue->clear();
    so->querySqrSum = VectorSquareNorm(query, dim);
```
new:
```cpp
    queue->clear();
    /*
     * [P0-2] The visited set is per-round state for the full-research MVP:
     * each expansion round restarts from the frozen point with a wider beam.
     * Retaining the previous round's visited marks left the frozen point
     * already-visited and prevented wider rounds from actually expanding
     * (design doc §4.13). Cross-round result dedup stays with the
     * candidates VectorList below.
     */
    blockhash_reset(so->blocks);
    so->querySqrSum = VectorSquareNorm(query, dim);
```

## 依据核对（全部实测）

- `so->candidates` 类型 `VectorList<DiskAnnCandidatesData>`，有 `clear()`（endscan:118 同款）
- `blockhash_reset` 由 simplehash 生成（simplehash.h:196），元素为纯值
  BlockNumberHashEntry，reset 安全
- `nexpextedCandidates=0` 后 gettuple 的 `curIterNum==0` 分支重设 DEFAULT_CADIDATES_NUMBER
  （diskannscan.cpp:143-144），首轮宽度回归基线值
- 跨轮"已输出不重复"由 diskannsearch 的 `searchedResults->contains()` 保持（:197），
  补丁 2 不影响该语义（candidates 在轮间保留、仅在 rescan 清空）

## 验证序列（Bash 恢复后）

1. 在**未打补丁**的当前二进制上先跑 diskann_include.sql 的 P0-2 段 →
   预期第二次 EXECUTE 混入 A 旧候选（实现前失败证据；若 generic plan 触发失败，
   按 PREP §3 回退参数化嵌套循环触发并记录）
2. 套用补丁 → 增量编译安装重启 → 复跑 P0-2 段 → B 结果 == 真值
3. diskann_vector 全量回归 vs 基线 diff 为空
4. LG 表 + VERIFICATION + commit `[P0-2] full per-query rescan reset and per-round visited reset`
