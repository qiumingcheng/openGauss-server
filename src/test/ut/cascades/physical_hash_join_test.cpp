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
 *        src/test/ut/cascades/physical_hash_join_test.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "physical_hash_join_test.h"
#include "nodes/makefuncs.h"

using namespace cascades;

GUNIT_TEST_REGISTRATION(PhysicalHashJoinTest, TestCase_DefaultConstructor)
GUNIT_TEST_REGISTRATION(PhysicalHashJoinTest, TestCase_ParameterizedConstructor)
GUNIT_TEST_REGISTRATION(PhysicalHashJoinTest, TestCase_BuildKeys)
GUNIT_TEST_REGISTRATION(PhysicalHashJoinTest, TestCase_ProbeKeys)
GUNIT_TEST_REGISTRATION(PhysicalHashJoinTest, TestCase_Clone)
GUNIT_TEST_REGISTRATION(PhysicalHashJoinTest, TestCase_HashConsistency)
GUNIT_TEST_REGISTRATION(PhysicalHashJoinTest, TestCase_HashDifference)
GUNIT_TEST_REGISTRATION(PhysicalHashJoinTest, TestCase_ToString)

void PhysicalHashJoinTest::SetUp() {}

void PhysicalHashJoinTest::TearDown() {}

void PhysicalHashJoinTest::TestCase_DefaultConstructor() {
    PhysicalHashJoin op;

    ASSERT_EQ(op.get_type(), OptExprOpType::PHYSICAL_HASH_JOIN);
    ASSERT_TRUE(op.is_physical());
    ASSERT_FALSE(op.is_logical());
    ASSERT_EQ(op.get_left_path(), nullptr);
    ASSERT_EQ(op.get_right_path(), nullptr);
    ASSERT_TRUE(op.get_build_keys().empty());
    ASSERT_TRUE(op.get_probe_keys().empty());
}

void PhysicalHashJoinTest::TestCase_ParameterizedConstructor() {
    Path* left_path = makeNode(Path);
    Path* right_path = makeNode(Path);
    Path* join_path = makeNode(Path);

    PhysicalHashJoin op(join_path, left_path, right_path);

    ASSERT_EQ(op.get_left_path(), left_path);
    ASSERT_EQ(op.get_right_path(), right_path);
    ASSERT_EQ(op.get_path(), join_path);
}

void PhysicalHashJoinTest::TestCase_BuildKeys() {
    PhysicalHashJoin op;
    std::vector<int> keys = {1, 2, 3};

    op.set_build_keys(keys);

    ASSERT_EQ(op.get_build_keys().size(), (size_t)3);
    ASSERT_EQ(op.get_build_keys()[0], 1);
    ASSERT_EQ(op.get_build_keys()[1], 2);
    ASSERT_EQ(op.get_build_keys()[2], 3);
}

void PhysicalHashJoinTest::TestCase_ProbeKeys() {
    PhysicalHashJoin op;
    std::vector<int> keys = {4, 5};

    op.set_probe_keys(keys);

    ASSERT_EQ(op.get_probe_keys().size(), (size_t)2);
    ASSERT_EQ(op.get_probe_keys()[0], 4);
    ASSERT_EQ(op.get_probe_keys()[1], 5);
}

void PhysicalHashJoinTest::TestCase_Clone() {
    Path* left_path = makeNode(Path);
    Path* right_path = makeNode(Path);
    Path* join_path = makeNode(Path);

    PhysicalHashJoin op(join_path, left_path, right_path);
    std::vector<int> build_keys = {1, 2};
    std::vector<int> probe_keys = {3, 4};
    op.set_build_keys(build_keys);
    op.set_probe_keys(probe_keys);

    OptExprOp* cloned = op.clone();
    PhysicalHashJoin* cloned_hash_join = dynamic_cast<PhysicalHashJoin*>(cloned);

    ASSERT_NE(cloned_hash_join, nullptr);
    ASSERT_EQ(cloned_hash_join->get_left_path(), left_path);
    ASSERT_EQ(cloned_hash_join->get_right_path(), right_path);
    ASSERT_EQ(cloned_hash_join->get_build_keys(), build_keys);
    ASSERT_EQ(cloned_hash_join->get_probe_keys(), probe_keys);

    delete cloned;
}

void PhysicalHashJoinTest::TestCase_HashConsistency() {
    Path* left_path = makeNode(Path);
    Path* right_path = makeNode(Path);
    Path* join_path = makeNode(Path);

    PhysicalHashJoin op1(join_path, left_path, right_path);
    PhysicalHashJoin op2(join_path, left_path, right_path);

    std::vector<int> keys = {1, 2, 3};
    op1.set_build_keys(keys);
    op2.set_build_keys(keys);

    ASSERT_EQ(op1.hash(), op2.hash());
}

void PhysicalHashJoinTest::TestCase_HashDifference() {
    Path* left_path = makeNode(Path);
    Path* right_path = makeNode(Path);
    Path* join_path = makeNode(Path);

    PhysicalHashJoin op1(join_path, left_path, right_path);
    PhysicalHashJoin op2(join_path, left_path, right_path);

    op1.set_build_keys({1, 2, 3});
    op2.set_build_keys({1, 2, 4});

    ASSERT_NE(op1.hash(), op2.hash());
}

void PhysicalHashJoinTest::TestCase_ToString() {
    PhysicalHashJoin op;
    ASSERT_EQ(op.to_string(), "PhysicalHashJoin");
}