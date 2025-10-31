# MashDB - Developer Guide

## Table of Contents

- [Prerequisites](#prerequisites)
    - [Linux](#linux-prerequisites)
    - [Windows](#windows-prerequisites)
- [Cloning the Repository](#cloning-the-repository)
- [Building the Project](#building-the-project)
    - [Linux](#building-on-linux)
    - [Windows](#building-on-windows)
- [Running the Application](#running-the-application)
- [Contributing](#contributing)

## Prerequisites

### Linux Prerequisites

For Debian/Ubuntu-based distributions:

```bash
sudo apt update
sudo apt install -y git build-essential cmake g++
```

For Fedora/RHEL-based distributions:

```bash
sudo dnf install -y git cmake gcc-c++ make
```

### Windows Prerequisites

1. Install Git for Windows: [Download Git](https://git-scm.com/download/win)
2. Install MinGW-w64 (Minimalist GNU for Windows):
    - Download from [MSYS2](https://www.msys2.org/)
    - Follow installation instructions
    - Add MinGW-w64 to your system PATH (typically `C:\msys64\mingw64\bin`)
3. Install CMake: [Download CMake](https://cmake.org/download/)
    - During installation, select "Add CMake to the system PATH for all users"
4. Install VSCode: [Download VSCode](https://code.visualstudio.com/)
5. Install VSCode Extensions:
    - C/C++
    - CMake
    - CMake Tools

## Cloning the Repository

### Linux/macOS

```bash
git clone https://github.com/Okaymisba/MashDB
cd MashDB
```

### Windows (Command Prompt)

```cmd
git clone https://github.com/Okaymisba/MashDB
cd MashDB
```

## Building the Project

### Building on Linux

1. Create and navigate to build directory:
   ```bash
   mkdir -p build && cd build
   ```

2. Generate build files:
   ```bash
   cmake ..
   ```

3. Build the project:
   ```bash
   make -j$(nproc)
   ```

### Building on Windows (Using VSCode Terminal)

1. Open VSCode and open the project folder
2. Open a new terminal in VSCode (Ctrl+`)
3. In the terminal, run the following commands:
   ```bash
   # Create and navigate to build directory
   mkdir build
   cd build
   
   # Generate build files with MinGW Makefiles
   cmake .. -G "MinGW Makefiles"
   
   # Build the project (use -j flag for parallel build)
   mingw32-make
   ```

#### Alternative: Using VSCode CMake Tools

1. Open the project in VSCode
2. Press `Ctrl+Shift+P` and select "CMake: Configure"
3. Select "GCC x86_64-w64-mingw32" as the kit
4. Press `F7` or use the build button in the status bar to build the project

## Running the Application

### Linux/macOS

```bash
./build/MashDB
```

### Windows

```bash
./build/MashDB.exe
```

## Contributing

1. Create a new branch for your feature or bugfix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes and commit them:
   ```bash
   git add .
   git commit -m "Add your commit message here"
   ```

3. Push your changes to the remote repository:
   ```bash
   git push origin feature/your-feature-name
   ```

4. Create a Pull Request on GitHub

---

## License

This project is licensed under the [MIT License](LICENSE).

© 2025 MashDB Team
