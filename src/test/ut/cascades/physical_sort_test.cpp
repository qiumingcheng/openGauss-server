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
 *        src/test/ut/cascades/physical_sort_test.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "physical_sort_test.h"
#include "nodes/makefuncs.h"

using namespace cascades;

GUNIT_TEST_REGISTRATION(PhysicalSortTest, TestCase_DefaultConstructor)
GUNIT_TEST_REGISTRATION(PhysicalSortTest, TestCase_ParameterizedConstructor)
GUNIT_TEST_REGISTRATION(PhysicalSortTest, TestCase_ChildPath)
GUNIT_TEST_REGISTRATION(PhysicalSortTest, TestCase_Clone)
GUNIT_TEST_REGISTRATION(PhysicalSortTest, TestCase_HashConsistency)
GUNIT_TEST_REGISTRATION(PhysicalSortTest, TestCase_ToString)

void PhysicalSortTest::SetUp() {}

void PhysicalSortTest::TearDown() {}

void PhysicalSortTest::TestCase_DefaultConstructor() {
    PhysicalSort op;

    ASSERT_EQ(op.get_type(), OptExprOpType::PHYSICAL_SORT);
    ASSERT_TRUE(op.is_physical());
    ASSERT_FALSE(op.is_logical());
    ASSERT_EQ(op.get_child_path(), nullptr);
}

void PhysicalSortTest::TestCase_ParameterizedConstructor() {
    Path* child_path = makeNode(Path);
    Path* sort_path = makeNode(Path);

    PhysicalSort op(sort_path, child_path);

    ASSERT_EQ(op.get_child_path(), child_path);
    ASSERT_EQ(op.get_path(), sort_path);
}

void PhysicalSortTest::TestCase_ChildPath() {
    PhysicalSort op;
    Path* child_path = makeNode(Path);

    // Initially null
    ASSERT_EQ(op.get_child_path(), nullptr);

    // Test setting via constructor (would need setter if existed)
    PhysicalSort op2(makeNode(Path), child_path);
    ASSERT_EQ(op2.get_child_path(), child_path);
}

void PhysicalSortTest::TestCase_Clone() {
    Path* child_path = makeNode(Path);
    Path* sort_path = makeNode(Path);

    PhysicalSort op(sort_path, child_path);

    OptExprOp* cloned = op.clone();
    PhysicalSort* cloned_sort = dynamic_cast<PhysicalSort*>(cloned);

    ASSERT_NE(cloned_sort, nullptr);
    ASSERT_EQ(cloned_sort->get_child_path(), child_path);
    ASSERT_EQ(cloned_sort->get_path(), sort_path);

    delete cloned;
}

void PhysicalSortTest::TestCase_HashConsistency() {
    Path* child_path = makeNode(Path);
    Path* sort_path = makeNode(Path);

    PhysicalSort op1(sort_path, child_path);
    PhysicalSort op2(sort_path, child_path);

    ASSERT_EQ(op1.hash(), op2.hash());
}

void PhysicalSortTest::TestCase_ToString() {
    PhysicalSort op;
    ASSERT_EQ(op.to_string(), "PhysicalSort");
}
