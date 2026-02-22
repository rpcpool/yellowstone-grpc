#!/usr/bin/env bash
set -euo pipefail

# Script to validate macOS NAPI binary
# Usage: ./validate-binary-macos.sh <binary_path> <arch>
#   arch: x86_64 or arm64

BINARY_PATH="${1:-}"
ARCH="${2:-}"

if [[ -z "$BINARY_PATH" || -z "$ARCH" ]]; then
    echo "Error: Missing required arguments"
    echo "Usage: $0 <binary_path> <arch>"
    echo "  arch: x86_64 or arm64"
    exit 1
fi

if [[ ! -f "$BINARY_PATH" ]]; then
    echo "Error: Binary not found at $BINARY_PATH"
    exit 1
fi

echo "=========================================="
echo "Validating macOS NAPI Binary"
echo "=========================================="
echo "Binary: $BINARY_PATH"
echo "Expected arch: $ARCH"
echo ""

# Check file type
echo "File type:"
file "$BINARY_PATH"
echo ""

# Verify it's a Mach-O binary
if ! file "$BINARY_PATH" | grep -q "Mach-O"; then
    echo "Error: Binary is not a Mach-O file"
    exit 1
fi

# Verify architecture with file command
if [[ "$ARCH" == "x86_64" ]]; then
    if ! file "$BINARY_PATH" | grep -q "x86_64"; then
        echo "Error: Binary is not x86_64 architecture"
        exit 1
    fi
elif [[ "$ARCH" == "arm64" ]]; then
    if ! file "$BINARY_PATH" | grep -q "arm64"; then
        echo "Error: Binary is not arm64 architecture"
        exit 1
    fi
fi

# Check dynamic library dependencies with otool
echo "Dynamic library dependencies (otool -L):"
otool -L "$BINARY_PATH"
echo ""

# Get detailed architecture info
echo "Architecture details (lipo -info):"
lipo -info "$BINARY_PATH"
echo ""

# Verify architecture matches with lipo
if [[ "$ARCH" == "x86_64" ]]; then
    if ! lipo -info "$BINARY_PATH" | grep -q "x86_64"; then
        echo "Error: lipo shows binary is not x86_64"
        exit 1
    fi
elif [[ "$ARCH" == "arm64" ]]; then
    if ! lipo -info "$BINARY_PATH" | grep -q "arm64"; then
        echo "Error: lipo shows binary is not arm64"
        exit 1
    fi
fi

# Check Mach-O header
echo "Mach-O header (otool -h):"
otool -h "$BINARY_PATH"
echo ""

echo "=========================================="
echo "Binary validation PASSED ✓"
echo "=========================================="
echo "Binary: $BINARY_PATH"
echo "Architecture: $ARCH"
echo ""
