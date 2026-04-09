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
 *        src/test/ut/cascades/physical_nest_loop_test.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "physical_nest_loop_test.h"
#include "nodes/makefuncs.h"

using namespace cascades;

GUNIT_TEST_REGISTRATION(PhysicalNestLoopTest, TestCase_DefaultConstructor)
GUNIT_TEST_REGISTRATION(PhysicalNestLoopTest, TestCase_ParameterizedConstructor)
GUNIT_TEST_REGISTRATION(PhysicalNestLoopTest, TestCase_LeftPath)
GUNIT_TEST_REGISTRATION(PhysicalNestLoopTest, TestCase_RightPath)
GUNIT_TEST_REGISTRATION(PhysicalNestLoopTest, TestCase_Clone)
GUNIT_TEST_REGISTRATION(PhysicalNestLoopTest, TestCase_HashConsistency)
GUNIT_TEST_REGISTRATION(PhysicalNestLoopTest, TestCase_ToString)

void PhysicalNestLoopTest::SetUp() {}

void PhysicalNestLoopTest::TearDown() {}

void PhysicalNestLoopTest::TestCase_DefaultConstructor() {
    PhysicalNestLoop op;

    ASSERT_EQ(op.get_type(), OptExprOpType::PHYSICAL_NESTLOOP_JOIN);
    ASSERT_TRUE(op.is_physical());
    ASSERT_FALSE(op.is_logical());
    ASSERT_EQ(op.get_left_path(), nullptr);
    ASSERT_EQ(op.get_right_path(), nullptr);
}

void PhysicalNestLoopTest::TestCase_ParameterizedConstructor() {
    Path* left_path = makeNode(Path);
    Path* right_path = makeNode(Path);
    Path* join_path = makeNode(Path);

    PhysicalNestLoop op(join_path, left_path, right_path);

    ASSERT_EQ(op.get_left_path(), left_path);
    ASSERT_EQ(op.get_right_path(), right_path);
    ASSERT_EQ(op.get_path(), join_path);
}

void PhysicalNestLoopTest::TestCase_LeftPath() {
    PhysicalNestLoop op;
    Path* left_path = makeNode(Path);

    PhysicalNestLoop op2(makeNode(Path), left_path, makeNode(Path));
    ASSERT_EQ(op2.get_left_path(), left_path);
}

void PhysicalNestLoopTest::TestCase_RightPath() {
    PhysicalNestLoop op;
    Path* right_path = makeNode(Path);

    PhysicalNestLoop op2(makeNode(Path), makeNode(Path), right_path);
    ASSERT_EQ(op2.get_right_path(), right_path);
}

void PhysicalNestLoopTest::TestCase_Clone() {
    Path* left_path = makeNode(Path);
    Path* right_path = makeNode(Path);
    Path* join_path = makeNode(Path);

    PhysicalNestLoop op(join_path, left_path, right_path);

    OptExprOp* cloned = op.clone();
    PhysicalNestLoop* cloned_nest = dynamic_cast<PhysicalNestLoop*>(cloned);

    ASSERT_NE(cloned_nest, nullptr);
    ASSERT_EQ(cloned_nest->get_left_path(), left_path);
    ASSERT_EQ(cloned_nest->get_right_path(), right_path);

    delete cloned;
}

void PhysicalNestLoopTest::TestCase_HashConsistency() {
    Path* left_path = makeNode(Path);
    Path* right_path = makeNode(Path);
    Path* join_path = makeNode(Path);

    PhysicalNestLoop op1(join_path, left_path, right_path);
    PhysicalNestLoop op2(join_path, left_path, right_path);

    ASSERT_EQ(op1.hash(), op2.hash());
}

void PhysicalNestLoopTest::TestCase_ToString() {
    PhysicalNestLoop op;
    ASSERT_EQ(op.to_string(), "PhysicalNestLoop");
}
