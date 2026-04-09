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
 *        src/test/ut/cascades/logical_filter_test.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "logical_filter_test.h"

using namespace cascades;

GUNIT_TEST_REGISTRATION(LogicalFilterTest, TestCase_DefaultConstructor)
GUNIT_TEST_REGISTRATION(LogicalFilterTest, TestCase_ChildConstructor)
GUNIT_TEST_REGISTRATION(LogicalFilterTest, TestCase_QualConstructor)
GUNIT_TEST_REGISTRATION(LogicalFilterTest, TestCase_Qual)
GUNIT_TEST_REGISTRATION(LogicalFilterTest, TestCase_PredicateId)
GUNIT_TEST_REGISTRATION(LogicalFilterTest, TestCase_Clone)
GUNIT_TEST_REGISTRATION(LogicalFilterTest, TestCase_HashConsistency)
GUNIT_TEST_REGISTRATION(LogicalFilterTest, TestCase_ToString)

void LogicalFilterTest::SetUp() {}

void LogicalFilterTest::TearDown() {}

void LogicalFilterTest::TestCase_DefaultConstructor() {
    LogicalFilter op;

    ASSERT_EQ(op.get_type(), OptExprOpType::LOGICAL_FILTER);
    ASSERT_TRUE(op.is_logical());
    ASSERT_FALSE(op.is_physical());
    ASSERT_EQ(op.get_qual(), nullptr);
    ASSERT_EQ(op.get_predicate_id(), -1);
}

void LogicalFilterTest::TestCase_ChildConstructor() {
    OptExpression* child = OptExpression::create(nullptr);

    LogicalFilter op(child);

    ASSERT_EQ(op.get_type(), OptExprOpType::LOGICAL_FILTER);
    ASSERT_TRUE(op.is_logical());
}

void LogicalFilterTest::TestCase_QualConstructor() {
    Node* qual = nullptr;  // In real use, this would be a valid Node

    LogicalFilter op(qual);

    ASSERT_EQ(op.get_type(), OptExprOpType::LOGICAL_FILTER);
    ASSERT_EQ(op.get_qual(), qual);
}

void LogicalFilterTest::TestCase_Qual() {
    LogicalFilter op;
    Node* qual = nullptr;

    ASSERT_EQ(op.get_qual(), nullptr);

    op.set_qual(qual);
    ASSERT_EQ(op.get_qual(), qual);
}

void LogicalFilterTest::TestCase_PredicateId() {
    LogicalFilter op;

    ASSERT_EQ(op.get_predicate_id(), -1);

    op.set_predicate_id(42);
    ASSERT_EQ(op.get_predicate_id(), 42);
}

void LogicalFilterTest::TestCase_Clone() {
    LogicalFilter op;
    op.set_predicate_id(100);

    OptExprOp* cloned = op.clone();
    LogicalFilter* cloned_filter = dynamic_cast<LogicalFilter*>(cloned);

    ASSERT_NE(cloned_filter, nullptr);
    ASSERT_EQ(cloned_filter->get_type(), OptExprOpType::LOGICAL_FILTER);
    ASSERT_EQ(cloned_filter->get_predicate_id(), 100);

    delete cloned;
}

void LogicalFilterTest::TestCase_HashConsistency() {
    LogicalFilter op1;
    LogicalFilter op2;

    ASSERT_EQ(op1.hash(), op2.hash());
}

void LogicalFilterTest::TestCase_ToString() {
    LogicalFilter op;
    ASSERT_EQ(op.to_string(), "LogicalFilter");
}
