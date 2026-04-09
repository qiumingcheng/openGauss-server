/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *        src/test/ut/cascades/logical_scan_test.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "logical_scan_test.h"

using namespace cascades;

GUNIT_TEST_REGISTRATION(LogicalScanTest, TestCase_ConstructorWithTableId)
GUNIT_TEST_REGISTRATION(LogicalScanTest, TestCase_ConstructorWithPredicate)
GUNIT_TEST_REGISTRATION(LogicalScanTest, TestCase_TableId)
GUNIT_TEST_REGISTRATION(LogicalScanTest, TestCase_PredicateId)
GUNIT_TEST_REGISTRATION(LogicalScanTest, TestCase_HasPredicate)
GUNIT_TEST_REGISTRATION(LogicalScanTest, TestCase_Rel)
GUNIT_TEST_REGISTRATION(LogicalScanTest, TestCase_Clone)
GUNIT_TEST_REGISTRATION(LogicalScanTest, TestCase_HashConsistency)
GUNIT_TEST_REGISTRATION(LogicalScanTest, TestCase_HashDifference)
GUNIT_TEST_REGISTRATION(LogicalScanTest, TestCase_ToString)

void LogicalScanTest::SetUp() {}

void LogicalScanTest::TearDown() {}

void LogicalScanTest::TestCase_ConstructorWithTableId() {
    LogicalScan op(42);

    ASSERT_EQ(op.get_type(), OptExprOpType::LOGICAL_SCAN);
    ASSERT_TRUE(op.is_logical());
    ASSERT_FALSE(op.is_physical());
    ASSERT_EQ(op.get_table_id(), 42);
    ASSERT_EQ(op.get_predicate_id(), -1);
    ASSERT_FALSE(op.has_predicate());
}

void LogicalScanTest::TestCase_ConstructorWithPredicate() {
    LogicalScan op(42, 100);

    ASSERT_EQ(op.get_table_id(), 42);
    ASSERT_EQ(op.get_predicate_id(), 100);
    ASSERT_TRUE(op.has_predicate());
}

void LogicalScanTest::TestCase_TableId() {
    LogicalScan op(1);
    ASSERT_EQ(op.get_table_id(), 1);
}

void LogicalScanTest::TestCase_PredicateId() {
    LogicalScan op(1);

    ASSERT_EQ(op.get_predicate_id(), -1);
    ASSERT_FALSE(op.has_predicate());

    op.set_predicate_id(50);
    ASSERT_EQ(op.get_predicate_id(), 50);
    ASSERT_TRUE(op.has_predicate());
}

void LogicalScanTest::TestCase_Rel() {
    LogicalScan op(1);
    RelOptInfo* rel = nullptr;  // In real use, this would be a valid RelOptInfo

    ASSERT_EQ(op.get_rel(), nullptr);

    op.set_rel(rel);
    ASSERT_EQ(op.get_rel(), rel);
}

void LogicalScanTest::TestCase_Clone() {
    LogicalScan op(42, 100);

    OptExprOp* cloned = op.clone();
    LogicalScan* cloned_scan = dynamic_cast<LogicalScan*>(cloned);

    ASSERT_NE(cloned_scan, nullptr);
    ASSERT_EQ(cloned_scan->get_type(), OptExprOpType::LOGICAL_SCAN);
    ASSERT_EQ(cloned_scan->get_table_id(), 42);
    ASSERT_EQ(cloned_scan->get_predicate_id(), 100);

    delete cloned;
}

void LogicalScanTest::TestCase_HashConsistency() {
    LogicalScan op1(42);
    LogicalScan op2(42);

    ASSERT_EQ(op1.hash(), op2.hash());
}

void LogicalScanTest::TestCase_HashDifference() {
    LogicalScan op1(42);
    LogicalScan op2(43);

    ASSERT_NE(op1.hash(), op2.hash());
}

void LogicalScanTest::TestCase_ToString() {
    LogicalScan op(1);
    ASSERT_EQ(op.to_string(), "LogicalScan");
}
