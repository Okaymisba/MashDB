# Compilation Guide

This document explains how to compile and run the MashDB project from the command line. It includes both a simple direct g++ invocation and a recommended CMake-based workflow with step-by-step instructions.

## Prerequisites
- POSIX-compatible shell (Linux/macOS). Commands below assume a Linux environment.
- A C++ compiler (g++). A modern version supporting C++17 or newer is recommended (g++ >= 7.5).
  - Verify with: g++ --version
- CMake (recommended for multi-file projects):
  - If `cmake` is not installed, on Debian/Ubuntu: sudo apt install cmake
  - Or: sudo snap install cmake

## Quick (single-command) build
From the repository root:
```bash
g++ src/main.cpp src/createDatabase.cpp -o main
./main
```
- Produces a `main` executable in the current directory.

## Recommended: CMake-based build (step-by-step)
CMake simplifies builds as the project grows. The following is a simple, reliable workflow.

1. Create and enter a separate build directory (out-of-source build):
```bash
mkdir build
cd build
```

2. Configure the project with CMake:
- Default (Debug or whatever CMakeLists sets):
```bash
cmake ..
```

If `cmake` is missing, install it (example for Debian/Ubuntu):
```bash
sudo apt update
sudo apt install cmake
```

3. Build the project:
- With Makefiles (default generator on many systems):
```bash
make
```

4. Run the produced executable:
- If the target is named `MashDB` (as in this repository), run:
```bash
./MashDB
```

Notes:
- The executable is created inside `build/` (or the build directory you specified).
- Use `cmake --build . --target <target>` to build a specific target.
