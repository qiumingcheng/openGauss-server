/*
 * diskannfilter.h — DiskANN In-Filter descriptor and three-valued logic (P2-2)
 *
 * Design source: 多模_向标融合_openGauss_DiskANN包含列与索引内过滤详细设计.md §15.1.
 *
 * DiskAnnFilterDesc is a RUNTIME object owned by the executor scan lifecycle
 * (filterFnCtx holds FmgrInfo, filterArgCtx holds current parameter values).
 * It must never be embedded in a Plan node or serialized: FmgrInfo contains
 * process addresses and MemoryContext pointers.
 *
 * The three-valued-logic helpers below are pure functions (no backend
 * dependencies beyond stdint/stdbool) so they compile into the standalone
 * unit test src/test/diskann_include_tools/ut_diskann_filter_logic.cpp.
 */
#ifndef DISKANN_FILTER_H
#define DISKANN_FILTER_H

/*
 * DISKANN_FILTER_LOGIC_ONLY compiles just the pure three-valued helpers for
 * the standalone unit test (no backend headers). The full descriptor needs
 * the backend types below.
 */
#ifndef DISKANN_FILTER_LOGIC_ONLY
#include "postgres.h"
#include "fmgr.h"
#include "access/itup.h"
#include "nodes/plannodes.h" /* AnnFilterMode */
#else
#include <stdint.h>
#include <stdbool.h>
typedef int16_t int16;
typedef unsigned int Oid;
typedef unsigned int AttrNumber;
typedef double Selectivity;
typedef uintptr_t Datum;
typedef unsigned int uint32;
/* descriptor body needs these only as opaque placeholders in logic-only mode */
typedef struct { char opaque_fmgr[1]; } FmgrInfo;
#endif

/*
 * [P2-2] Filter key kinds (design doc 15.1). V1 supports AND only; OR needs
 * a small boolean tree and is deliberately out of scope.
 */
typedef enum DiskAnnFilterKind {
    DISKANN_FILTER_OP = 0,        /* col op Const/Param (strict immutable op) */
    DISKANN_FILTER_IS_NULL = 1,
    DISKANN_FILTER_IS_NOT_NULL = 2,
    DISKANN_FILTER_ARRAY_ANY = 3  /* col IN (Const/Param...) */
} DiskAnnFilterKind;

typedef struct DiskAnnFilterKey {
    AttrNumber indexAttno;   /* 1-based position in the index TupleDesc */
    DiskAnnFilterKind kind;
    Oid operatorOid;
    Oid functionOid;         /* underlying comparison function */
    Oid collation;
    Oid leftType;
    Oid rightType;
    FmgrInfo function;       /* lives in filterFnCtx */
    Datum argument;          /* current param value; lives in filterArgCtx */
    bool argumentIsNull;
    bool argumentByVal;
    int16 argumentLen;
    bool functionStrict;
} DiskAnnFilterKey;

typedef struct DiskAnnFilterDesc {
    int nkeys;
    DiskAnnFilterKey* keys;  /* array[nkeys] */
    Selectivity estimatedSelectivity;
    uint32 maxExpansions;
} DiskAnnFilterDesc;

/* ---------------------------------------------------------------------
 * Pure three-valued-logic helpers (design doc 15.1 "必须实现 SQL WHERE 的
 * 三值逻辑，而不是 C++ bool：只有 TRUE 进入 eligibleResults，FALSE 和
 * NULL 都拒绝"). TriState mirrors SQL booleans; testable standalone.
 * ------------------------------------------------------------------- */
typedef enum DiskAnnTriState {
    DISKANN_TRI_FALSE = 0,
    DISKANN_TRI_TRUE = 1,
    DISKANN_TRI_NULL = 2
} DiskAnnTriState;

/* strict comparison with NULL operand: NULL propagates */
static inline DiskAnnTriState diskann_tri_strict_op(bool valueIsNull, bool argumentIsNull, bool cmpResult)
{
    if (valueIsNull || argumentIsNull) {
        return DISKANN_TRI_NULL;
    }
    return cmpResult ? DISKANN_TRI_TRUE : DISKANN_TRI_FALSE;
}

/* IS NULL / IS NOT NULL: never returns NULL */
static inline DiskAnnTriState diskann_tri_nulltest(bool valueIsNull, bool isNotNull)
{
    if (isNotNull) {
        return valueIsNull ? DISKANN_TRI_FALSE : DISKANN_TRI_TRUE;
    }
    return valueIsNull ? DISKANN_TRI_TRUE : DISKANN_TRI_FALSE;
}

/*
 * ScalarArrayOpExpr ANY per SQL: TRUE if any element compares TRUE; NULL if
 * no TRUE but at least one NULL (or the array itself is NULL); FALSE only
 * when every element is FALSE/empty-non-null array. An empty (zero-element)
 * array yields FALSE; a NULL array yields NULL.
 */
static inline DiskAnnTriState diskann_tri_array_any(int nElems, const DiskAnnTriState* elemResults, bool arrayIsNull)
{
    int i;

    if (arrayIsNull) {
        return DISKANN_TRI_NULL;
    }
    if (nElems <= 0) {
        return DISKANN_TRI_FALSE; /* empty array: FALSE, per SQL semantics */
    }
    for (i = 0; i < nElems; i++) {
        if (elemResults[i] == DISKANN_TRI_TRUE) {
            return DISKANN_TRI_TRUE;
        }
    }
    for (i = 0; i < nElems; i++) {
        if (elemResults[i] == DISKANN_TRI_NULL) {
            return DISKANN_TRI_NULL;
        }
    }
    return DISKANN_TRI_FALSE;
}

/* AND fold with Kleene logic (V1 top-level connector) */
static inline DiskAnnTriState diskann_tri_and(DiskAnnTriState a, DiskAnnTriState b)
{
    if (a == DISKANN_TRI_FALSE || b == DISKANN_TRI_FALSE) {
        return DISKANN_TRI_FALSE;
    }
    if (a == DISKANN_TRI_NULL || b == DISKANN_TRI_NULL) {
        return DISKANN_TRI_NULL;
    }
    return DISKANN_TRI_TRUE;
}

/* only SQL TRUE admits a candidate to eligibleResults */
static inline bool diskann_tri_admits(DiskAnnTriState t)
{
    return t == DISKANN_TRI_TRUE;
}

#endif /* DISKANN_FILTER_H */
