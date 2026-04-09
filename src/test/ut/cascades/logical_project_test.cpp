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
 *        src/test/ut/cascades/logical_project_test.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "logical_project_test.h"

using namespace cascades;

GUNIT_TEST_REGISTRATION(LogicalProjectTest, TestCase_DefaultConstructor)
GUNIT_TEST_REGISTRATION(LogicalProjectTest, TestCase_ChildConstructor)
GUNIT_TEST_REGISTRATION(LogicalProjectTest, TestCase_Projections)
GUNIT_TEST_REGISTRATION(LogicalProjectTest, TestCase_TargetList)
GUNIT_TEST_REGISTRATION(LogicalProjectTest, TestCase_Clone)
GUNIT_TEST_REGISTRATION(LogicalProjectTest, TestCase_HashConsistency)
GUNIT_TEST_REGISTRATION(LogicalProjectTest, TestCase_HashDifference)
GUNIT_TEST_REGISTRATION(LogicalProjectTest, TestCase_ToString)

void LogicalProjectTest::SetUp() {}

void LogicalProjectTest::TearDown() {}

void LogicalProjectTest::TestCase_DefaultConstructor() {
    LogicalProject op;

    ASSERT_EQ(op.get_type(), OptExprOpType::LOGICAL_PROJECT);
    ASSERT_TRUE(op.is_logical());
    ASSERT_FALSE(op.is_physical());
    ASSERT_TRUE(op.get_projections().empty());
    ASSERT_EQ(op.get_target_list(), nullptr);
}

void LogicalProjectTest::TestCase_ChildConstructor() {
    OptExpression* child = OptExpression::create(nullptr);

    LogicalProject op(child);

    ASSERT_EQ(op.get_type(), OptExprOpType::LOGICAL_PROJECT);
    ASSERT_TRUE(op.is_logical());
}

void LogicalProjectTest::TestCase_Projections() {
    LogicalProject op;
    std::vector<int> cols = {0, 1, 2};

    op.set_projections(cols);

    ASSERT_EQ(op.get_projections().size(), (size_t)3);
    ASSERT_EQ(op.get_projections()[0], 0);
    ASSERT_EQ(op.get_projections()[1], 1);
    ASSERT_EQ(op.get_projections()[2], 2);
}

void LogicalProjectTest::TestCase_TargetList() {
    LogicalProject op;
    List* target_list = nullptr;  // In real use, this would be a valid List

    ASSERT_EQ(op.get_target_list(), nullptr);

    op.set_target_list(target_list);
    ASSERT_EQ(op.get_target_list(), target_list);
}

void LogicalProjectTest::TestCase_Clone() {
    LogicalProject op;
    std::vector<int> cols = {0, 1};
    op.set_projections(cols);

    OptExprOp* cloned = op.clone();
    LogicalProject* cloned_project = dynamic_cast<LogicalProject*>(cloned);

    ASSERT_NE(cloned_project, nullptr);
    ASSERT_EQ(cloned_project->get_type(), OptExprOpType::LOGICAL_PROJECT);
    ASSERT_EQ(cloned_project->get_projections(), cols);

    delete cloned;
}

void LogicalProjectTest::TestCase_HashConsistency() {
    LogicalProject op1;
    LogicalProject op2;

    op1.set_projections({0, 1});
    op2.set_projections({0, 1});

    ASSERT_EQ(op1.hash(), op2.hash());
}

void LogicalProjectTest::TestCase_HashDifference() {
    LogicalProject op1;
    LogicalProject op2;

    op1.set_projections({0, 1});
    op2.set_projections({0, 2});

    ASSERT_NE(op1.hash(), op2.hash());
}

void LogicalProjectTest::TestCase_ToString() {
    LogicalProject op;
    ASSERT_EQ(op.to_string(), "LogicalProject");
}
