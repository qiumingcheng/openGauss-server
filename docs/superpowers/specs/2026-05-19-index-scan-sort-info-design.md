# IndexScanImplRule 排序信息设置重构

## 目标

当 `IndexScanImplRule::transform` 检测到索引匹配 ORDER BY 时，在 `IndexPath` 分支创建 `PhysicalIndexScan` 后调用 `add_sort_column` 设置排序信息，使物理计划节点携带排序元数据。

## 修改位置

`src/gausskernel/optimizer/cascades/rule/index_scan_impl_rule.cpp`

## 详细设计

在 `if (IsA(path, IndexPath))` 分支内，创建 `physical_scan` 后添加排序信息设置逻辑：

```cpp
if (IsA(path, IndexPath)) {
    IndexPath* index_path = (IndexPath*)path;
    List* pathkeys = index_path->path.pathkeys;

    // ... 现有匹配逻辑 ...

    physical_scan = NEW_EX(PhysicalIndexScan, (Path*)index_path);

    // 设置排序信息：从 sort_clause 提取（只在 IndexPath 分支）
    if (sort_clause != nullptr) {
        List* clauses = static_cast<List*>(sort_clause);
        List* targetlist = root->parse->targetList;
        ListCell* lc = nullptr;
        foreach (lc, clauses) {
            SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc));
            TargetEntry* tle = get_sortgroupref_tle(sgcl->tleSortGroupRef, targetlist, false);
            if (tle != nullptr && tle->expr != nullptr && IsA(tle->expr, Var)) {
                Var* var = (Var*)tle->expr;
                bool ascending = (sgcl->sortop < 500);  // heuristic
                physical_scan->add_sort_column(var->varattno, ascending);
            }
        }
    }
} else if (IsA(path, BitmapHeapPath)) {
    // BitmapHeapPath 不能提供排序输出，不设置排序信息
    if (sort_clause != nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, skipping BitmapPath, cannot satisfy sort")));
        continue;
    }
    physical_scan = NEW_EX(PhysicalIndexScan, (Path*)bitmap_path);
}
```

## 说明

- `BitmapHeapPath` 无法提供有序输出，因此排序信息设置只在 `IndexPath` 分支内进行
- 排序信息提取逻辑参考 `sort_impl_rule.cpp` 第 364-376 行

## 验证

- 编译通过
- 集成测试验证索引扫描计划包含正确的排序信息