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
 *        src/test/ut/cascades/physical_hash_join_test.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PHYSICAL_HASH_JOIN_TEST_H
#define PHYSICAL_HASH_JOIN_TEST_H

#include "gunit_test.h"
#include "gausskernel/optimizer/cascades/operator/operators/physical_hash_join.h"

class PhysicalHashJoinTest : public testing::Test {
    GUNIT_TEST_SUITE(PhysicalHashJoinTest);

public:
    virtual void SetUp();
    virtual void TearDown();

public:
    void TestCase_DefaultConstructor();
    void TestCase_ParameterizedConstructor();
    void TestCase_BuildKeys();
    void TestCase_ProbeKeys();
    void TestCase_Clone();
    void TestCase_HashConsistency();
    void TestCase_HashDifference();
    void TestCase_ToString();
};

#endif