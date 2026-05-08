---
name: integration-testing
description: Use when testing Cascades optimizer changes end-to-end, verifying SQL execution plans and query results match the original optimizer
---

# Cascades Optimizer Integration Testing

## Overview

Test Cascades optimizer changes by compiling, deploying, and verifying SQL execution against the original optimizer.

## When to Use

- After modifying Cascades optimizer code (translator, rules, optimizer)
- When EXPLAIN output differs from original optimizer
- When query execution fails or returns wrong results
- Before committing optimizer changes

## Integration Test Steps

### 1. Compile Cascades Optimizer

**重要：必须在 tmp_build 目录下执行 make**

```bash
cd /home/hcy/projects/openGauss-server/tmp_build
export THIRD_BIN_PATH=/home/hcy/projects/binarylibs
make cascadesoptimizer -j4 && make install
```

### 2. Restart gaussdb

```bash
export LD_LIBRARY_PATH=/home/hcy/gaussdb/lib:$LD_LIBRARY_PATH
/home/hcy/gaussdb/bin/gs_ctl restart -D /home/hcy/gaussdata2/ -l /tmp/gaussdb.log
sleep 3
```

### 3. Execute SQL with Cascades Optimizer

```bash
export LD_LIBRARY_PATH=/home/hcy/gaussdb/lib:$LD_LIBRARY_PATH
/home/hcy/gaussdb/bin/gsql -d testdb -c "SET enable_cascades_optimizer = on; EXPLAIN (VERBOSE, COSTS OFF) <SQL>;"
```

### 4. Compare with Original Optimizer

```bash
# Cascades optimizer
/home/hcy/gaussdb/bin/gsql -d testdb -c "SET enable_cascades_optimizer = on; EXPLAIN (VERBOSE, COSTS OFF) <SQL>;"

# Original optimizer
/home/hcy/gaussdb/bin/gsql -d testdb -c "SET enable_cascades_optimizer = off; EXPLAIN (VERBOSE, COSTS OFF) <SQL>;"

# Actual execution
/home/hcy/gaussdb/bin/gsql -d testdb -c "<SQL>;"
```

## Debugging Failed Tests

1. Check gaussdb log: `tail -f /tmp/gaussdb.log`
2. Look for Cascades debug messages in output
3. Enable verbose EXPLAIN to see plan details

### Using gdb to Analyze Crashes

When gaussdb crashes, it generates a core dump. Use gdb to analyze the stack trace:

```bash
# Step 1: Clean up residual processes and lock files
ps aux | grep gaussdb | grep -v grep | awk '{print $2}' | xargs -r kill -9
rm -f /home/hcy/gaussdata2/postmaster.pid /home/hcy/gaussdata2/pg_ctl.lock

# Step 2: Enable core dump and start database normally
ulimit -c unlimited
export LD_LIBRARY_PATH=/home/hcy/gaussdb/lib:$LD_LIBRARY_PATH
/home/hcy/gaussdb/bin/gs_ctl restart -D /home/hcy/gaussdata2/ -l /tmp/gaussdb.log

# Step 3: Run the crashing query
/home/hcy/gaussdb/bin/gsql -d testdb -p 9666 -c "SET enable_cascades_optimizer = on; <crashing_sql>;"

# Step 4: Find the core dump file
ls -la ./core.*

# Step 5: Analyze with gdb
gdb --args /home/hcy/gaussdb/bin/gaussdb -D /home/hcy/gaussdata2/ ./core.*
bt
```

## Quick Reference

| Step | Command |
|------|---------|
| Compile | `make cascadesoptimizer -j4 && make install` |
| Restart | `gs_ctl restart -D /home/hcy/gaussdata2/ -l /tmp/gaussdb.log` |
| Cascades EXPLAIN | `SET enable_cascades_optimizer = on; EXPLAIN ...` |
| Original EXPLAIN | `SET enable_cascades_optimizer = off; EXPLAIN ...` |
| gsql | `/home/hcy/gaussdb/bin/gsql -d testdb -c "..."` |

## Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| Changes not taking effect | Binary not reinstalled | Run `make install` |
| Connection refused | Database not running | Restart with `gs_ctl restart` |
| Plan differs from original | Optimization bug | Check translate_expression flow |
| Query execution error | Var reference issue | Check convert_var_references_mutator |
