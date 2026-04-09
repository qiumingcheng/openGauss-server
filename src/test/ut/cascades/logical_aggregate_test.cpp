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
 *        src/test/ut/cascades/logical_aggregate_test.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "logical_aggregate_test.h"

using namespace cascades;

GUNIT_TEST_REGISTRATION(LogicalAggregateTest, TestCase_DefaultConstructor)
GUNIT_TEST_REGISTRATION(LogicalAggregateTest, TestCase_GroupKeysConstructor)
GUNIT_TEST_REGISTRATION(LogicalAggregateTest, TestCase_GroupKeys)
GUNIT_TEST_REGISTRATION(LogicalAggregateTest, TestCase_Clone)
GUNIT_TEST_REGISTRATION(LogicalAggregateTest, TestCase_HashConsistency)
GUNIT_TEST_REGISTRATION(LogicalAggregateTest, TestCase_HashDifference)
GUNIT_TEST_REGISTRATION(LogicalAggregateTest, TestCase_ToString)

void LogicalAggregateTest::SetUp() {}

void LogicalAggregateTest::TearDown() {}

void LogicalAggregateTest::TestCase_DefaultConstructor() {
    LogicalAggregate op;

    ASSERT_EQ(op.get_type(), OptExprOpType::LOGICAL_AGGREGATE);
    ASSERT_TRUE(op.is_logical());
    ASSERT_FALSE(op.is_physical());
    ASSERT_TRUE(op.get_group_keys().empty());
}

void LogicalAggregateTest::TestCase_GroupKeysConstructor() {
    std::vector<int> group_keys = {1, 2, 3};

    LogicalAggregate op(group_keys);

    ASSERT_EQ(op.get_group_keys().size(), (size_t)3);
    ASSERT_EQ(op.get_group_keys()[0], 1);
    ASSERT_EQ(op.get_group_keys()[1], 2);
    ASSERT_EQ(op.get_group_keys()[2], 3);
}

void LogicalAggregateTest::TestCase_GroupKeys() {
    LogicalAggregate op;
    std::vector<int> keys = {0, 1};

    op.set_group_keys(keys);

    ASSERT_EQ(op.get_group_keys().size(), (size_t)2);
    ASSERT_EQ(op.get_group_keys()[0], 0);
    ASSERT_EQ(op.get_group_keys()[1], 1);
}

void LogicalAggregateTest::TestCase_Clone() {
    std::vector<int> group_keys = {1, 2};

    LogicalAggregate op(group_keys);

    OptExprOp* cloned = op.clone();
    LogicalAggregate* cloned_agg = dynamic_cast<LogicalAggregate*>(cloned);

    ASSERT_NE(cloned_agg, nullptr);
    ASSERT_EQ(cloned_agg->get_type(), OptExprOpType::LOGICAL_AGGREGATE);
    ASSERT_EQ(cloned_agg->get_group_keys(), group_keys);

    delete cloned;
}

void LogicalAggregateTest::TestCase_HashConsistency() {
    std::vector<int> group_keys = {1, 2};

    LogicalAggregate op1(group_keys);
    LogicalAggregate op2(group_keys);

    ASSERT_EQ(op1.hash(), op2.hash());
}

void LogicalAggregateTest::TestCase_HashDifference() {
    LogicalAggregate op1;
    LogicalAggregate op2;

    op1.set_group_keys({1, 2});
    op2.set_group_keys({1, 3});

    ASSERT_NE(op1.hash(), op2.hash());
}

void LogicalAggregateTest::TestCase_ToString() {
    LogicalAggregate op;
    ASSERT_EQ(op.to_string(), "LogicalAggregate");
}
