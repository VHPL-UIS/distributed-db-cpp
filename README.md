# Distributed Database Engine

A high-performance distributed key-value store with SQL-like capabilities built with modern C++. This project demonstrates distributed systems concepts, consensus algorithms, and database internals through a complete implementation.

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Client API    │    │   Query Engine  │    │  Consensus      │
│                 │    │                 │    │  (Raft)         │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ Network Layer   │    │ Storage Engine  │    │ Cluster Mgmt    │
│                 │    │                 │    │                 │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│       Consistent Hashing & Replication      │ Node Discovery  │
└─────────────────────────────────────────────┴─────────────────┘
```

## Quick Start

### Prerequisites

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y build-essential cmake git libssl-dev

# macOS
brew install cmake openssl

# CentOS/RHEL
sudo yum groupinstall -y "Development Tools"
sudo yum install -y cmake openssl-devel
```

### Building from Source

```bash
# Clone the repository
git clone git@github.com:VHPL-UIS/distributed-db-cpp.git
cd distributed_db

# Create build directory
mkdir build && cd build

# Configure and build
cmake ..
make
```

## 📚 Learning Resources

### Distributed Systems

- [Designing Data-Intensive Applications](https://dataintensive.net/) by Martin Kleppmann
- [Distributed Systems](https://www.distributed-systems.net/) by Maarten van Steen

### Modern C++

- [Effective Modern C++](https://www.oreilly.com/library/view/effective-modern-c/9781491908419/) by Scott Meyers
- [C++ Core Guidelines](https://isocpp.github.io/CppCoreGuidelines/)

### Database Internals

- [Database Internals](https://databass.dev/) by Alex Petrov
- [Architecture of a Database System](http://db.cs.berkeley.edu/papers/fntdb07-architecture.pdf)

---
