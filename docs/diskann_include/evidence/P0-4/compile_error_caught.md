# P0-4 编译失败记录（被构建门禁抓获）

- 首次编译（build_incremental.log，BUILD_EXIT=2）：`StaticAssertStmt` 在本 C++ 构建中
  展开为运行期表达式（c.h:1111），不能用于文件作用域 → 编译错误。
- 修正：改用 C++11 原生 `static_assert`（fsyntax-only 快检通过后全量重编）。
- 教训入库：本仓库文件作用域编译期断言用 static_assert，不用 StaticAssertStmt。
