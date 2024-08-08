#!/bin/bash -xeu

DIR="${DIR:=$(realpath `dirname "${0}"`)}"

. "${DIR}/env.sh"

BUILD_DIR="${DIR}/.build/static"
INSTALL_DIR="${DIR}/.deps/static"
mkdir -p "${BUILD_DIR}" "${INSTALL_DIR}" "${INSTALL_DIR}/lib" "${INSTALL_DIR}/include"
BUILD_DIR="$(realpath "${BUILD_DIR}")"
INSTALL_DIR="$(realpath "${INSTALL_DIR}")"

export LDFLAGS="-static"
export CFLAGS=""
MACHINE_TYPE="$(uname -m)"
if [ "${MACHINE_TYPE}" = "ppc64le" ]; then
  MACHINE_TYPE="powerpc64le"
  export CFLAGS="-mlong-double-64"
fi

"${DIR}/deps.sh"

# build musl
if [ ! -f "${INSTALL_DIR}/musl/bin/musl-gcc" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf musl
    git clone "${REPO_MUSL}" --depth 1 --branch "${TAG_MUSL}" musl > /dev/null
    cd musl
    ./configure --prefix="${INSTALL_DIR}/musl" > /dev/null
    make -j > /dev/null
    make -j install > /dev/null || true

    # missing musl header files
    ln -s "/usr/include/${MACHINE_TYPE}-linux-gnu/sys/queue.h" "${INSTALL_DIR}/musl/include/sys/queue.h" || true
    ln -s "/usr/include/${MACHINE_TYPE}-linux-gnu/asm" "${INSTALL_DIR}/musl/include/asm" || true
    ln -s /usr/include/asm-generic "${INSTALL_DIR}/musl/include/asm-generic" || true
    ln -s /usr/include/linux "${INSTALL_DIR}/musl/include/linux" || true
  )
fi

export PATH="${PATH}:${INSTALL_DIR}/musl/bin"
export CC=musl-gcc

# build libtirpc
if [ ! -f "${BUILD_DIR}/libtirpc/src/libtirpc.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf libtirpc
    git clone "${REPO_LIBTIRPC}" --depth 1 --branch "${TAG_LIBTIRPC}" > /dev/null
    cd libtirpc
    chmod +x autogen.sh
    ./autogen.sh > /dev/null
    ./configure --disable-shared --disable-gssapi > /dev/null
    make -j > /dev/null
  )
fi

# build libnsl
if [ ! -f "${BUILD_DIR}/libnsl/src/libnsl.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf libnsl
    git clone "${REPO_LIBNSL}" --depth 1 --branch "${TAG_LIBNSL}" > /dev/null
    cd libnsl
    ./autogen.sh > /dev/null
    autoreconf -i > /dev/null
    autoconf > /dev/null
    ./configure --disable-shared \
      CFLAGS="${CFLAGS} -I${BUILD_DIR}/libtirpc/tirpc" \
      LDFLAGS="${LDFLAGS} -L${BUILD_DIR}/libtirpc/src" \
      TIRPC_CFLAGS="-I${BUILD_DIR}/libtirpc/tirpc" \
      TIRPC_LIBS="-L${BUILD_DIR}/libtirpc/src" \
      > /dev/null
    make -j > /dev/null
  )
fi

# build libuv
if [ ! -f "${BUILD_DIR}/libuv/libuv.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf libuv
    git clone "${REPO_LIBUV}" --depth 1 --branch "${TAG_LIBUV}" > /dev/null
    cd libuv
    ./autogen.sh > /dev/null
    ./configure > /dev/null
    make -j > /dev/null
  )
fi

# build liblz4
if [ ! -f "${BUILD_DIR}/lz4/lib/liblz4.a" ] || [ ! -f "${BUILD_DIR}/lz4/lib/liblz4.so" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf lz4
    git clone "${REPO_LIBLZ4}" --depth 1 --branch "${TAG_LIBLZ4}" > /dev/null
    cd lz4
    make lib -j > /dev/null
  )
fi

# build sqlite3
if [ ! -f "${BUILD_DIR}/sqlite/libsqlite3.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf sqlite
    git clone "${REPO_SQLITE}" --depth 1 --branch "${TAG_SQLITE}" > /dev/null
    cd sqlite
    ./configure --disable-shared --disable-readline \
      CFLAGS="${CFLAGS} -DSQLITE_ENABLE_DBSTAT_VTAB=1" \
      > /dev/null
    make libsqlite3.la -j BCC="${CC} -g -O2 ${CFLAGS} ${LDFLAGS}" > /dev/null
  )
fi

# build dqlite
if [ ! -f "${BUILD_DIR}/dqlite/libdqlite.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf dqlite
    git clone "${REPO_DQLITE}" --depth 1 --branch "${TAG_DQLITE}" > /dev/null
    cd dqlite
    autoreconf -i > /dev/null
    ./configure --disable-shared --enable-build-raft \
      CFLAGS="${CFLAGS} -I${BUILD_DIR}/sqlite -I${BUILD_DIR}/libuv/include -I${BUILD_DIR}/lz4/lib -I${INSTALL_DIR}/musl/include -Werror=implicit-function-declaration" \
      LDFLAGS="${LDFLAGS} -L${BUILD_DIR}/libuv/.libs -L${BUILD_DIR}/lz4/lib -L${BUILD_DIR}/libnsl/src" \
      UV_CFLAGS="-I${BUILD_DIR}/libuv/include" \
      UV_LIBS="-L${BUILD_DIR}/libuv/.libs" \
      SQLITE_CFLAGS="-I${BUILD_DIR}/sqlite" \
      LZ4_CFLAGS="-I${BUILD_DIR}/lz4/lib" \
      LZ4_LIBS="-L${BUILD_DIR}/lz4/lib" \
      > /dev/null

    make -j > /dev/null
  )
fi

# collect libraries
(
  cd "${BUILD_DIR}"
  cp libuv/.libs/* "${INSTALL_DIR}/lib"
  cp lz4/lib/*.a "${INSTALL_DIR}/lib"
  cp lz4/lib/*.so* "${INSTALL_DIR}/lib"
  cp sqlite/.libs/*.a "${INSTALL_DIR}/lib"
  cp dqlite/.libs/*.a "${INSTALL_DIR}/lib"
)

# collect headers
(
  cd "${BUILD_DIR}"
  cp -r libuv/include/* "${INSTALL_DIR}/include"
  cp -r sqlite/*.h "${INSTALL_DIR}/include"
  cp -r dqlite/include/* "${INSTALL_DIR}/include"
)

export CGO_CFLAGS="-I${INSTALL_DIR}/include"
export CGO_LDFLAGS="-L${INSTALL_DIR}/lib -luv -ldqlite -llz4 -lsqlite3 -Wl,-z,stack-size=1048576"
