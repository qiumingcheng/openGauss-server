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
 *        src/test/ut/cascades/physical_exchange_test.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "physical_exchange_test.h"
#include "nodes/makefuncs.h"

using namespace cascades;

GUNIT_TEST_REGISTRATION(PhysicalExchangeTest, TestCase_DefaultConstructor)
GUNIT_TEST_REGISTRATION(PhysicalExchangeTest, TestCase_ParameterizedConstructor)
GUNIT_TEST_REGISTRATION(PhysicalExchangeTest, TestCase_ChildPath)
GUNIT_TEST_REGISTRATION(PhysicalExchangeTest, TestCase_DistributionType)
GUNIT_TEST_REGISTRATION(PhysicalExchangeTest, TestCase_Clone)
GUNIT_TEST_REGISTRATION(PhysicalExchangeTest, TestCase_HashConsistency)
GUNIT_TEST_REGISTRATION(PhysicalExchangeTest, TestCase_HashDifference)
GUNIT_TEST_REGISTRATION(PhysicalExchangeTest, TestCase_ToString)

void PhysicalExchangeTest::SetUp() {}

void PhysicalExchangeTest::TearDown() {}

void PhysicalExchangeTest::TestCase_DefaultConstructor() {
    PhysicalExchange op;

    ASSERT_EQ(op.get_type(), OptExprOpType::PHYSICAL_EXCHANGE);
    ASSERT_TRUE(op.is_physical());
    ASSERT_FALSE(op.is_logical());
    ASSERT_EQ(op.get_child_path(), nullptr);
    ASSERT_EQ(op.get_distribution_type(), DistributionType::ANY);
}

void PhysicalExchangeTest::TestCase_ParameterizedConstructor() {
    Path* child_path = makeNode(Path);
    Path* exchange_path = makeNode(Path);

    PhysicalExchange op(exchange_path, child_path, DistributionType::HASH_DISTRIBUTED);

    ASSERT_EQ(op.get_child_path(), child_path);
    ASSERT_EQ(op.get_path(), exchange_path);
    ASSERT_EQ(op.get_distribution_type(), DistributionType::HASH_DISTRIBUTED);
}

void PhysicalExchangeTest::TestCase_ChildPath() {
    PhysicalExchange op;
    Path* child_path = makeNode(Path);

    ASSERT_EQ(op.get_child_path(), nullptr);

    PhysicalExchange op2(makeNode(Path), child_path, DistributionType::ANY);
    ASSERT_EQ(op2.get_child_path(), child_path);
}

void PhysicalExchangeTest::TestCase_DistributionType() {
    PhysicalExchange op;

    ASSERT_EQ(op.get_distribution_type(), DistributionType::ANY);

    PhysicalExchange op2(makeNode(Path), makeNode(Path), DistributionType::REPLICATED);
    ASSERT_EQ(op2.get_distribution_type(), DistributionType::REPLICATED);
}

void PhysicalExchangeTest::TestCase_Clone() {
    Path* child_path = makeNode(Path);
    Path* exchange_path = makeNode(Path);

    PhysicalExchange op(exchange_path, child_path, DistributionType::HASH_DISTRIBUTED);

    OptExprOp* cloned = op.clone();
    PhysicalExchange* cloned_exchange = dynamic_cast<PhysicalExchange*>(cloned);

    ASSERT_NE(cloned_exchange, nullptr);
    ASSERT_EQ(cloned_exchange->get_type(), OptExprOpType::PHYSICAL_EXCHANGE);
    ASSERT_EQ(cloned_exchange->get_child_path(), child_path);
    ASSERT_EQ(cloned_exchange->get_distribution_type(), DistributionType::HASH_DISTRIBUTED);

    delete cloned;
}

void PhysicalExchangeTest::TestCase_HashConsistency() {
    Path* child_path = makeNode(Path);
    Path* exchange_path = makeNode(Path);

    PhysicalExchange op1(exchange_path, child_path, DistributionType::HASH_DISTRIBUTED);
    PhysicalExchange op2(exchange_path, child_path, DistributionType::HASH_DISTRIBUTED);

    ASSERT_EQ(op1.hash(), op2.hash());
}

void PhysicalExchangeTest::TestCase_HashDifference() {
    Path* child_path = makeNode(Path);
    Path* exchange_path = makeNode(Path);

    PhysicalExchange op1(exchange_path, child_path, DistributionType::HASH_DISTRIBUTED);
    PhysicalExchange op2(exchange_path, child_path, DistributionType::REPLICATED);

    ASSERT_NE(op1.hash(), op2.hash());
}

void PhysicalExchangeTest::TestCase_ToString() {
    PhysicalExchange op;
    ASSERT_EQ(op.to_string(), "PhysicalExchange");
}
