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
 *        src/test/ut/cascades/logical_join_test.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "logical_join_test.h"
#include "operators/logical_inner_join.h"

using namespace cascades;

GUNIT_TEST_REGISTRATION(LogicalJoinTest, TestCase_JoinKeys)
GUNIT_TEST_REGISTRATION(LogicalJoinTest, TestCase_LeftRightJoinKeys)
GUNIT_TEST_REGISTRATION(LogicalJoinTest, TestCase_JoinType)
GUNIT_TEST_REGISTRATION(LogicalJoinTest, TestCase_Qual)
GUNIT_TEST_REGISTRATION(LogicalJoinTest, TestCase_SwapChildren)

void LogicalJoinTest::SetUp() {}

void LogicalJoinTest::TearDown() {}

void LogicalJoinTest::TestCase_JoinKeys() {
    // Test through LogicalInnerJoin which inherits from LogicalJoin
    LogicalInnerJoin op;
    std::vector<int> keys = {1, 2, 3};

    op.set_join_keys(keys);

    ASSERT_EQ(op.get_join_keys().size(), (size_t)3);
    ASSERT_EQ(op.get_join_keys()[0], 1);
    ASSERT_EQ(op.get_join_keys()[1], 2);
    ASSERT_EQ(op.get_join_keys()[2], 3);
}

void LogicalJoinTest::TestCase_LeftRightJoinKeys() {
    LogicalInnerJoin op;
    std::vector<int> left_keys = {1, 2};
    std::vector<int> right_keys = {3, 4};

    op.set_join_keys(left_keys, right_keys);

    ASSERT_EQ(op.get_left_join_keys().size(), (size_t)2);
    ASSERT_EQ(op.get_left_join_keys()[0], 1);
    ASSERT_EQ(op.get_left_join_keys()[1], 2);

    ASSERT_EQ(op.get_right_join_keys().size(), (size_t)2);
    ASSERT_EQ(op.get_right_join_keys()[0], 3);
    ASSERT_EQ(op.get_right_join_keys()[1], 4);
}

void LogicalJoinTest::TestCase_JoinType() {
    LogicalInnerJoin op;

    ASSERT_EQ(op.get_join_type(), JOIN_INNER);

    op.set_join_type(JOIN_LEFT);
    ASSERT_EQ(op.get_join_type(), JOIN_LEFT);

    op.set_join_type(JOIN_RIGHT);
    ASSERT_EQ(op.get_join_type(), JOIN_RIGHT);

    op.set_join_type(JOIN_FULL);
    ASSERT_EQ(op.get_join_type(), JOIN_FULL);

    op.set_join_type(JOIN_SEMI);
    ASSERT_EQ(op.get_join_type(), JOIN_SEMI);
}

void LogicalJoinTest::TestCase_Qual() {
    LogicalInnerJoin op;
    Node* qual = nullptr;  // In real use, this would be a valid Node

    ASSERT_EQ(op.get_qual(), nullptr);

    op.set_qual(qual);
    ASSERT_EQ(op.get_qual(), qual);
}

void LogicalJoinTest::TestCase_SwapChildren() {
    OptExpression* left = OptExpression::create(nullptr);
    OptExpression* right = OptExpression::create(nullptr);

    LogicalInnerJoin op(left, right);

    auto initial_children = op.get_children();
    ASSERT_EQ(initial_children.size(), (size_t)2);
    ASSERT_EQ(initial_children[0], left);
    ASSERT_EQ(initial_children[1], right);

    op.swap_children();

    auto swapped_children = op.get_children();
    ASSERT_EQ(swapped_children.size(), (size_t)2);
    ASSERT_EQ(swapped_children[0], right);
    ASSERT_EQ(swapped_children[1], left);
}
