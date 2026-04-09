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
 *        src/test/ut/cascades/physical_scan_test.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "physical_scan_test.h"
#include "nodes/makefuncs.h"

using namespace cascades;

GUNIT_TEST_REGISTRATION(PhysicalScanTest, TestCase_DefaultConstructor)
GUNIT_TEST_REGISTRATION(PhysicalScanTest, TestCase_ParameterizedConstructor)
GUNIT_TEST_REGISTRATION(PhysicalScanTest, TestCase_Clone)
GUNIT_TEST_REGISTRATION(PhysicalScanTest, TestCase_HashConsistency)
GUNIT_TEST_REGISTRATION(PhysicalScanTest, TestCase_ToString)

void PhysicalScanTest::SetUp() {}

void PhysicalScanTest::TearDown() {}

void PhysicalScanTest::TestCase_DefaultConstructor() {
    PhysicalScan op;

    ASSERT_EQ(op.get_type(), OptExprOpType::PHYSICAL_SCAN);
    ASSERT_TRUE(op.is_physical());
    ASSERT_FALSE(op.is_logical());
    ASSERT_EQ(op.get_path(), nullptr);
}

void PhysicalScanTest::TestCase_ParameterizedConstructor() {
    Path* scan_path = makeNode(Path);

    PhysicalScan op(scan_path);

    ASSERT_EQ(op.get_path(), scan_path);
}

void PhysicalScanTest::TestCase_Clone() {
    Path* scan_path = makeNode(Path);

    PhysicalScan op(scan_path);

    OptExprOp* cloned = op.clone();
    PhysicalScan* cloned_scan = dynamic_cast<PhysicalScan*>(cloned);

    ASSERT_NE(cloned_scan, nullptr);
    ASSERT_EQ(cloned_scan->get_type(), OptExprOpType::PHYSICAL_SCAN);
    ASSERT_EQ(cloned_scan->get_path(), scan_path);

    delete cloned;
}

void PhysicalScanTest::TestCase_HashConsistency() {
    Path* scan_path = makeNode(Path);

    PhysicalScan op1(scan_path);
    PhysicalScan op2(scan_path);

    ASSERT_EQ(op1.hash(), op2.hash());
}

void PhysicalScanTest::TestCase_ToString() {
    PhysicalScan op;
    ASSERT_EQ(op.to_string(), "PhysicalScan");
}
