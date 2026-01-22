#!/usr/bin/env bash
set -euo pipefail

# Script to validate Linux NAPI binary
# Usage: ./validate-binary.sh <binary_path> <libc_type> <arch>
#   libc_type: gnu or musl
#   arch: x86_64 or aarch64

BINARY_PATH="${1:-}"
LIBC_TYPE="${2:-}"
ARCH="${3:-}"

if [[ -z "$BINARY_PATH" || -z "$LIBC_TYPE" || -z "$ARCH" ]]; then
    echo "Error: Missing required arguments"
    echo "Usage: $0 <binary_path> <libc_type> <arch>"
    echo "  libc_type: gnu or musl"
    echo "  arch: x86_64 or aarch64"
    exit 1
fi

if [[ ! -f "$BINARY_PATH" ]]; then
    echo "Error: Binary not found at $BINARY_PATH"
    exit 1
fi

echo "=========================================="
echo "Validating NAPI Binary"
echo "=========================================="
echo "Binary: $BINARY_PATH"
echo "Expected libc: $LIBC_TYPE"
echo "Expected arch: $ARCH"
echo ""

# Check file type
echo "File type:"
file "$BINARY_PATH"
echo ""

# Verify it's a shared object
if ! file "$BINARY_PATH" | grep -q "shared object"; then
    echo "Error: Binary is not a shared object"
    exit 1
fi

# Verify architecture
if [[ "$ARCH" == "x86_64" ]]; then
    if ! file "$BINARY_PATH" | grep -q "x86-64"; then
        echo "Error: Binary is not x86-64 architecture"
        exit 1
    fi
elif [[ "$ARCH" == "aarch64" ]]; then
    if ! file "$BINARY_PATH" | grep -q "aarch64"; then
        echo "Error: Binary is not aarch64 architecture"
        exit 1
    fi
fi

# Check libc linkage with ldd
echo "Library dependencies (ldd):"
ldd "$BINARY_PATH" || true
echo ""

# Verify libc type
if [[ "$LIBC_TYPE" == "gnu" ]]; then
    if ldd "$BINARY_PATH" 2>&1 | grep -q "musl"; then
        echo "Error: Binary is linked against musl, expected glibc"
        exit 1
    fi
    # Check for glibc
    if ! ldd "$BINARY_PATH" 2>&1 | grep -q "libc.so"; then
        echo "Warning: Could not verify glibc linkage"
    fi
elif [[ "$LIBC_TYPE" == "musl" ]]; then
    if ! ldd "$BINARY_PATH" 2>&1 | grep -qi "musl"; then
        echo "Error: Binary is not linked against musl"
        exit 1
    fi
fi

# Get readelf output for detailed verification
echo "ELF Header:"
readelf -h "$BINARY_PATH" | head -n 20
echo ""

# Check dynamic section
echo "Dynamic section (first 20 entries):"
readelf -d "$BINARY_PATH" | head -n 20
echo ""

echo "=========================================="
echo "Binary validation PASSED ✓"
echo "=========================================="
echo "Binary: $BINARY_PATH"
echo "Architecture: $ARCH"
echo "Libc: $LIBC_TYPE"
echo ""
