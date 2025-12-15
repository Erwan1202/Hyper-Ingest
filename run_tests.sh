#!/bin/bash

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${PROJECT_DIR}/build"

echo "=== CivicCore HyperIngest Test Runner ==="
echo ""

command -v conan >/dev/null 2>&1 || { echo "Error: conan is required but not installed."; exit 1; }
command -v cmake >/dev/null 2>&1 || { echo "Error: cmake is required but not installed."; exit 1; }

echo "[1/4] Setting up build directory..."
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"

echo "[2/4] Installing Conan dependencies..."
conan install "${PROJECT_DIR}" -of="${BUILD_DIR}" --build=missing -s build_type=Release

TOOLCHAIN_FILE=$(find "${BUILD_DIR}" -name "conan_toolchain.cmake" 2>/dev/null | head -1)
if [ -z "${TOOLCHAIN_FILE}" ]; then
    echo "Error: conan_toolchain.cmake not found"
    exit 1
fi
echo "Found toolchain: ${TOOLCHAIN_FILE}"

echo "[3/4] Configuring with CMake..."
cd "${BUILD_DIR}"
cmake "${PROJECT_DIR}" \
    -DCMAKE_TOOLCHAIN_FILE="${TOOLCHAIN_FILE}" \
    -DCMAKE_BUILD_TYPE=Release

echo "[4/4] Building project and tests..."
cmake --build . --parallel

echo ""
echo "=== Running Tests ==="
ctest --output-on-failure --parallel $(nproc)

echo ""
echo "=== Test Summary ==="
ctest -N | tail -1
