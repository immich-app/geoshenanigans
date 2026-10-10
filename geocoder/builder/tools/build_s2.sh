#!/usr/bin/env bash
# Usage: build_s2.sh <prefix>
#
# Builds Ubuntu noble's S2 package source (s2geometry 0.10.0-6.1build4: the
# upstream release plus its Debian patches) as a static libs2.a with the
# package's own compile flags plus -DNDEBUG, and installs it into <prefix>.
# The libs2-dev build is CMake build type None, so S2_DCHECK and the s2debug
# validity checks run on every call. Point CMake at the result with
# -DCMAKE_PREFIX_PATH=<prefix>; it links against the distro abseil and OpenSSL
# (libabsl-dev, libssl-dev), as the package does.
#
# The flags only drop the debug checks: no -march, -O3 or -ffast-math, so the
# floating point results are the package's and the indexes stay byte
# identical. Left out of the package's flags: -g, the source path maps, and
# -flto=auto -ffat-lto-objects, whose LTO half a static library linked into a
# non-LTO binary never uses.
set -euo pipefail

prefix=$(realpath -m "$1")
version=0.10.0
debian=6.1build4
pool=http://archive.ubuntu.com/ubuntu/pool/universe/s/s2geometry
orig_sha=1c17b04f1ea20ed09a67a83151ddd5d8529716f509dde49a8190618d70532a3d
debian_sha=aa64f747ec51e0773ec7ce28c2603bcfd265a8fe68b35874e87f0016a2ad34ae
flags="-O2 -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer -fstack-protector-strong -fstack-clash-protection -Wformat -Werror=format-security -fcf-protection -Wdate-time -D_FORTIFY_SOURCE=3 -DNDEBUG"

work=$(mktemp -d "${TMPDIR:-/tmp}/build-s2.XXXXXX")
trap 'rm -rf "$work"' EXIT
cd "$work"

fetch() {
  curl -sSL --fail --retry 5 --retry-delay 10 --retry-all-errors -o "$1" "$pool/$1"
  echo "$2  $1" | sha256sum -c --quiet
}
fetch "s2geometry_${version}.orig.tar.gz" "$orig_sha"
fetch "s2geometry_${version}-${debian}.debian.tar.xz" "$debian_sha"

tar xzf "s2geometry_${version}.orig.tar.gz"
cd "s2geometry-${version}"
tar xJf "../s2geometry_${version}-${debian}.debian.tar.xz"
while read -r p; do
  [ -n "$p" ] && patch -s -p1 < "debian/patches/$p"
done < debian/patches/series

cmake -S . -B build -DCMAKE_BUILD_TYPE=None -DCMAKE_CXX_FLAGS="$flags" \
  -DBUILD_SHARED_LIBS=OFF -DBUILD_EXAMPLES=OFF -DWITH_PYTHON=OFF \
  -DCMAKE_INSTALL_PREFIX="$prefix" -DCMAKE_INSTALL_LIBDIR=lib
cmake --build build -j"$(nproc)"
cmake --install build
echo "S2 ${version}-${debian} (NDEBUG, static) installed in $prefix"
