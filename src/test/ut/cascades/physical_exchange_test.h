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
 *        src/test/ut/cascades/physical_exchange_test.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PHYSICAL_EXCHANGE_TEST_H
#define PHYSICAL_EXCHANGE_TEST_H

#include "gunit_test.h"
#include "gausskernel/optimizer/cascades/operator/operators/physical_exchange.h"

class PhysicalExchangeTest : public testing::Test {
    GUNIT_TEST_SUITE(PhysicalExchangeTest);

public:
    virtual void SetUp();
    virtual void TearDown();

public:
    void TestCase_DefaultConstructor();
    void TestCase_ParameterizedConstructor();
    void TestCase_ChildPath();
    void TestCase_DistributionType();
    void TestCase_Clone();
    void TestCase_HashConsistency();
    void TestCase_HashDifference();
    void TestCase_ToString();
};

#endif
