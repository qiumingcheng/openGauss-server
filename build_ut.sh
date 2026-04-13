#!/bin/bash

# 设置环境变量
export BINARYLIBS=/home/hcy/projects/binarylibs
export DEBUG_TYPE=debug
export BOOST_ROOT=/home/hcy/projects/binarylibs/kernel/dependency/boost/llt

# 清理旧的 cmake 缓存
rm -rf /home/hcy/projects/openGauss-server/tmp_build

# 重新配置，确保 ENABLE_UT=ON 使 SUPPORT_LLT=llt
cd /home/hcy/projects/openGauss-server
sh build.sh -m debug -3rd $BINARYLIBS --cmake --cmake_opt "-DENABLE_UT=ON" --cmake_opt "-DBOOST_INCLUDE_PATH=/home/hcy/projects/binarylibs/kernel/dependency/boost/llt/include"


