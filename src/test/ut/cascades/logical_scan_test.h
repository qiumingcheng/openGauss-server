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
 *        src/test/ut/cascades/logical_scan_test.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef LOGICAL_SCAN_TEST_H
#define LOGICAL_SCAN_TEST_H

#include "gunit_test.h"
#include "gausskernel/optimizer/cascades/operator/operators/logical_scan.h"

class LogicalScanTest : public testing::Test {
    GUNIT_TEST_SUITE(LogicalScanTest);

public:
    virtual void SetUp();
    virtual void TearDown();

public:
    void TestCase_ConstructorWithTableId();
    void TestCase_ConstructorWithPredicate();
    void TestCase_TableId();
    void TestCase_PredicateId();
    void TestCase_HasPredicate();
    void TestCase_Rel();
    void TestCase_Clone();
    void TestCase_HashConsistency();
    void TestCase_HashDifference();
    void TestCase_ToString();
};

#endif
