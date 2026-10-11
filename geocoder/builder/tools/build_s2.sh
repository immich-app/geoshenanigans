#!/usr/bin/env bash
# Usage: build_s2.sh <prefix>
#
# Builds Ubuntu noble's S2 and abseil package sources (s2geometry
# 0.10.0-6.1build4 and abseil 20220623.1-3.1ubuntu3.2: the upstream releases
# plus their Debian patches) as static libraries with the packages' own
# compile flags plus -DNDEBUG, and installs them into <prefix>. Both packages
# are CMake build type None, so S2_DCHECK and the s2debug validity checks run
# on every call, and every absl::Mutex runs deadlock detection under one
# global lock. Point CMake at the result with -DGEOCODER_S2_PREFIX=<prefix>;
# <prefix>/SHA256SUMS lists every file installed there.
#
# The flags only drop the debug checks: no -march, -O3 or -ffast-math, so the
# floating point results are the packages' and the indexes stay byte
# identical. Left out of the packages' flags: -g, the Debian source paths, and
# S2's -flto=auto -ffat-lto-objects, whose LTO half a static library linked
# into a non-LTO binary never uses. Abseil builds with the default g++, not
# the package's g++-12.
set -euo pipefail

prefix=$(realpath -m "$1")
flags="-O2 -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer -fstack-protector-strong -fstack-clash-protection -Wformat -Werror=format-security -fcf-protection -Wdate-time -D_FORTIFY_SOURCE=3 -DNDEBUG"

work=$(mktemp -d "${TMPDIR:-/tmp}/build-s2.XXXXXX")
trap 'rm -rf "$work"' EXIT
cd "$work"
# The libraries' __FILE__ strings name the sources relative to this directory,
# so a rebuild gives the same bytes.
flags+=" -ffile-prefix-map=$work/="

# unpack <source> <version> <debian revision> <orig sha256> <debian sha256>:
# fetch a package's upstream and Debian tarballs, check them, apply the
# Debian patches; leaves the tree in <source>-<version>. Launchpad keeps every
# published version's files, where the archive pool drops a version once a
# newer one supersedes it.
unpack() {
  local files=https://launchpad.net/ubuntu/+archive/primary/+sourcefiles/$1/$2-$3
  local orig=$1_$2.orig.tar.gz deb=$1_$2-$3.debian.tar.xz
  fetch "$orig" "$files/$orig"
  fetch "$deb" "$files/$deb"
  printf '%s  %s\n%s  %s\n' "$4" "$orig" "$5" "$deb" | sha256sum -c --quiet
  tar xzf "$orig"
  tar xJf "$deb" -C "$1-$2"
  apply_series "$1-$2" "$1-$2/debian/patches"
}

# fetch <file> <url>: a stalled transfer times out and is retried.
fetch() {
  curl -sSL --fail --connect-timeout 30 --max-time 300 --retry 5 --retry-delay 10 --retry-all-errors -o "$1" "$2"
}

# apply_series <tree> <patch dir>: the patches quilt's series file lists, in
# order. A line is a patch and its patch options (-p1 when none); # starts a
# comment; the last line may lack its newline.
apply_series() {
  local line words
  while IFS= read -r line || [ -n "$line" ]; do
    read -r -a words <<< "${line%%#*}"
    [ ${#words[@]} -gt 0 ] || continue
    [ ${#words[@]} -gt 1 ] || words+=(-p1)
    patch -s -d "$1" "${words[@]:1}" < "$2/${words[0]}"
  done < "$2/series"
}

# build_static <source dir> [cmake args...]: build and install into <prefix>.
build_static() {
  cmake -S "$1" -B "$1/build" -DCMAKE_BUILD_TYPE=None -DCMAKE_CXX_FLAGS="$flags" \
    -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_PREFIX="$prefix" -DCMAKE_INSTALL_LIBDIR=lib "${@:2}"
  cmake --build "$1/build" -j"$(nproc)"
  cmake --install "$1/build"
}

unpack abseil 20220623.1 3.1ubuntu3.2 \
  abfe2897f3a30edaa74bc34365afe3c2a3cd012091a97dc7e008f7016adcd5fe \
  10cd95eb3664eeee653789488662aa9dfd8c185c1892fe759f073bfe15db055a
build_static abseil-20220623.1 -DCMAKE_CXX_STANDARD=17 -DBUILD_TESTING=OFF

unpack s2geometry 0.10.0 6.1build4 \
  1c17b04f1ea20ed09a67a83151ddd5d8529716f509dde49a8190618d70532a3d \
  aa64f747ec51e0773ec7ce28c2603bcfd265a8fe68b35874e87f0016a2ad34ae
build_static s2geometry-0.10.0 -DBUILD_EXAMPLES=OFF -DWITH_PYTHON=OFF -Dabsl_DIR="$prefix/lib/cmake/absl"

# A manifest of everything installed: a restored copy is checked against it.
(cd "$prefix" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > "$work/SHA256SUMS")
mv "$work/SHA256SUMS" "$prefix/SHA256SUMS"

echo "S2 and abseil (NDEBUG, static) installed in $prefix"
