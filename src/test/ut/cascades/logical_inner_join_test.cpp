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
 *        src/test/ut/cascades/logical_inner_join_test.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "logical_inner_join_test.h"

using namespace cascades;

GUNIT_TEST_REGISTRATION(LogicalInnerJoinTest, TestCase_DefaultConstructor)
GUNIT_TEST_REGISTRATION(LogicalInnerJoinTest, TestCase_JoinKeysConstructor)
GUNIT_TEST_REGISTRATION(LogicalInnerJoinTest, TestCase_ChildrenConstructor)
GUNIT_TEST_REGISTRATION(LogicalInnerJoinTest, TestCase_JoinKeys)
GUNIT_TEST_REGISTRATION(LogicalInnerJoinTest, TestCase_JoinType)
GUNIT_TEST_REGISTRATION(LogicalInnerJoinTest, TestCase_SwapChildren)
GUNIT_TEST_REGISTRATION(LogicalInnerJoinTest, TestCase_Clone)
GUNIT_TEST_REGISTRATION(LogicalInnerJoinTest, TestCase_HashConsistency)
GUNIT_TEST_REGISTRATION(LogicalInnerJoinTest, TestCase_HashDifference)
GUNIT_TEST_REGISTRATION(LogicalInnerJoinTest, TestCase_ToString)

void LogicalInnerJoinTest::SetUp() {}

void LogicalInnerJoinTest::TearDown() {}

void LogicalInnerJoinTest::TestCase_DefaultConstructor() {
    LogicalInnerJoin op;

    ASSERT_EQ(op.get_type(), OptExprOpType::LOGICAL_INNER_JOIN);
    ASSERT_TRUE(op.is_logical());
    ASSERT_FALSE(op.is_physical());
    ASSERT_TRUE(op.get_join_keys().empty());
    ASSERT_EQ(op.get_join_type(), JOIN_INNER);
}

void LogicalInnerJoinTest::TestCase_JoinKeysConstructor() {
    std::vector<int> join_keys = {1, 2, 3};

    LogicalInnerJoin op(join_keys);

    ASSERT_EQ(op.get_join_keys().size(), (size_t)3);
    ASSERT_EQ(op.get_join_keys()[0], 1);
    ASSERT_EQ(op.get_join_keys()[1], 2);
    ASSERT_EQ(op.get_join_keys()[2], 3);
}

void LogicalInnerJoinTest::TestCase_ChildrenConstructor() {
    OptExpression* left = OptExpression::create(nullptr);
    OptExpression* right = OptExpression::create(nullptr);

    LogicalInnerJoin op(left, right);

    ASSERT_EQ(op.get_type(), OptExprOpType::LOGICAL_INNER_JOIN);
}

void LogicalInnerJoinTest::TestCase_JoinKeys() {
    LogicalInnerJoin op;
    std::vector<int> keys = {1, 2};

    op.set_join_keys(keys);

    ASSERT_EQ(op.get_join_keys().size(), (size_t)2);
    ASSERT_EQ(op.get_join_keys()[0], 1);
    ASSERT_EQ(op.get_join_keys()[1], 2);
}

void LogicalInnerJoinTest::TestCase_JoinType() {
    LogicalInnerJoin op;

    ASSERT_EQ(op.get_join_type(), JOIN_INNER);

    op.set_join_type(JOIN_LEFT);
    ASSERT_EQ(op.get_join_type(), JOIN_LEFT);
}

void LogicalInnerJoinTest::TestCase_SwapChildren() {
    OptExpression* left = OptExpression::create(nullptr);
    OptExpression* right = OptExpression::create(nullptr);

    LogicalInnerJoin op(left, right);

    // Get initial children
    auto initial_children = op.get_children();
    ASSERT_EQ(initial_children.size(), (size_t)2);
    ASSERT_EQ(initial_children[0], left);
    ASSERT_EQ(initial_children[1], right);

    // Swap children
    op.swap_children();

    auto swapped_children = op.get_children();
    ASSERT_EQ(swapped_children.size(), (size_t)2);
    ASSERT_EQ(swapped_children[0], right);
    ASSERT_EQ(swapped_children[1], left);
}

void LogicalInnerJoinTest::TestCase_Clone() {
    std::vector<int> join_keys = {1, 2};

    LogicalInnerJoin op(join_keys);

    OptExprOp* cloned = op.clone();
    LogicalInnerJoin* cloned_join = dynamic_cast<LogicalInnerJoin*>(cloned);

    ASSERT_NE(cloned_join, nullptr);
    ASSERT_EQ(cloned_join->get_type(), OptExprOpType::LOGICAL_INNER_JOIN);
    ASSERT_EQ(cloned_join->get_join_keys(), join_keys);

    delete cloned;
}

void LogicalInnerJoinTest::TestCase_HashConsistency() {
    std::vector<int> join_keys = {1, 2};

    LogicalInnerJoin op1(join_keys);
    LogicalInnerJoin op2(join_keys);

    ASSERT_EQ(op1.hash(), op2.hash());
}

void LogicalInnerJoinTest::TestCase_HashDifference() {
    LogicalInnerJoin op1;
    LogicalInnerJoin op2;

    op1.set_join_keys({1, 2});
    op2.set_join_keys({1, 3});

    ASSERT_NE(op1.hash(), op2.hash());
}

void LogicalInnerJoinTest::TestCase_ToString() {
    LogicalInnerJoin op;
    ASSERT_EQ(op.to_string(), "LogicalInnerJoin");
}
