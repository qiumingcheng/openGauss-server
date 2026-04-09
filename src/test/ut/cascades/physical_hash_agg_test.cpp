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
 *        src/test/ut/cascades/physical_hash_agg_test.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "physical_hash_agg_test.h"
#include "nodes/makefuncs.h"

using namespace cascades;

GUNIT_TEST_REGISTRATION(PhysicalHashAggTest, TestCase_DefaultConstructor)
GUNIT_TEST_REGISTRATION(PhysicalHashAggTest, TestCase_ParameterizedConstructor)
GUNIT_TEST_REGISTRATION(PhysicalHashAggTest, TestCase_ChildPath)
GUNIT_TEST_REGISTRATION(PhysicalHashAggTest, TestCase_GroupKeys)
GUNIT_TEST_REGISTRATION(PhysicalHashAggTest, TestCase_Clone)
GUNIT_TEST_REGISTRATION(PhysicalHashAggTest, TestCase_HashConsistency)
GUNIT_TEST_REGISTRATION(PhysicalHashAggTest, TestCase_HashDifference)
GUNIT_TEST_REGISTRATION(PhysicalHashAggTest, TestCase_ToString)

void PhysicalHashAggTest::SetUp() {}

void PhysicalHashAggTest::TearDown() {}

void PhysicalHashAggTest::TestCase_DefaultConstructor() {
    PhysicalHashAgg op;

    ASSERT_EQ(op.get_type(), OptExprOpType::PHYSICAL_HASH_AGG);
    ASSERT_TRUE(op.is_physical());
    ASSERT_FALSE(op.is_logical());
    ASSERT_EQ(op.get_child_path(), nullptr);
    ASSERT_TRUE(op.get_group_keys().empty());
}

void PhysicalHashAggTest::TestCase_ParameterizedConstructor() {
    Path* child_path = makeNode(Path);
    Path* agg_path = makeNode(Path);
    std::vector<int> group_keys = {1, 2};

    PhysicalHashAgg op(agg_path, child_path, group_keys);

    ASSERT_EQ(op.get_child_path(), child_path);
    ASSERT_EQ(op.get_path(), agg_path);
    ASSERT_EQ(op.get_group_keys().size(), (size_t)2);
    ASSERT_EQ(op.get_group_keys()[0], 1);
    ASSERT_EQ(op.get_group_keys()[1], 2);
}

void PhysicalHashAggTest::TestCase_ChildPath() {
    PhysicalHashAgg op;
    Path* child_path = makeNode(Path);

    ASSERT_EQ(op.get_child_path(), nullptr);

    PhysicalHashAgg op2(makeNode(Path), child_path, {});
    ASSERT_EQ(op2.get_child_path(), child_path);
}

void PhysicalHashAggTest::TestCase_GroupKeys() {
    PhysicalHashAgg op;
    std::vector<int> keys = {1, 2, 3};

    PhysicalHashAgg op2(makeNode(Path), makeNode(Path), keys);

    ASSERT_EQ(op2.get_group_keys().size(), (size_t)3);
    ASSERT_EQ(op2.get_group_keys()[0], 1);
    ASSERT_EQ(op2.get_group_keys()[1], 2);
    ASSERT_EQ(op2.get_group_keys()[2], 3);
}

void PhysicalHashAggTest::TestCase_Clone() {
    Path* child_path = makeNode(Path);
    Path* agg_path = makeNode(Path);
    std::vector<int> group_keys = {1, 2};

    PhysicalHashAgg op(agg_path, child_path, group_keys);

    OptExprOp* cloned = op.clone();
    PhysicalHashAgg* cloned_agg = dynamic_cast<PhysicalHashAgg*>(cloned);

    ASSERT_NE(cloned_agg, nullptr);
    ASSERT_EQ(cloned_agg->get_child_path(), child_path);
    ASSERT_EQ(cloned_agg->get_group_keys(), group_keys);

    delete cloned;
}

void PhysicalHashAggTest::TestCase_HashConsistency() {
    Path* child_path = makeNode(Path);
    Path* agg_path = makeNode(Path);
    std::vector<int> group_keys = {1, 2};

    PhysicalHashAgg op1(agg_path, child_path, group_keys);
    PhysicalHashAgg op2(agg_path, child_path, group_keys);

    ASSERT_EQ(op1.hash(), op2.hash());
}

void PhysicalHashAggTest::TestCase_HashDifference() {
    Path* child_path = makeNode(Path);
    Path* agg_path = makeNode(Path);

    PhysicalHashAgg op1(agg_path, child_path, {1, 2});
    PhysicalHashAgg op2(agg_path, child_path, {1, 3});

    ASSERT_NE(op1.hash(), op2.hash());
}

void PhysicalHashAggTest::TestCase_ToString() {
    PhysicalHashAgg op;
    ASSERT_EQ(op.to_string(), "PhysicalHashAgg");
}
