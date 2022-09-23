#!/bin/bash

if ! [ -e /usr/bin/clang ]; then
  ln -s /usr/bin/clang++-15 /usr/bin/clang++
  ln -s /usr/bin/clang-15 /usr/bin/clang
fi

if ! [ -d ../boost ]; then
  git clone --recursive -j4 https://github.com/boostorg/boost.git ../boost
  cd ../boost
  ./bootstrap.sh --with-toolset="clang"
  ./b2 install toolset=clang cxxflags="-stdlib=libc++" linkflags="-stdlib=libc++"
  cd ../hcache-uring
fi

if ! [ -d ../liburing ]; then
  git clone --recursive https://github.com/axboe/liburing.git ../liburing
  cd ../liburing
  CC=clang CXX=clang++ ./configure
  make -j$(nproc)
  make install
  cd ../hcache-uring
fi 

CXX=clang++ CC=clang cmake -Bbuild -DCMAKE_BUILD_TYPE=Release -GNinja .
cmake --build build
strip build/hcache
cp build/hcache /usr/bin/hcache