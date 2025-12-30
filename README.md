# DecLock - Efficient, Scalable, and Fair Locking for Disaggregated Memory

DecLock is an efficient, scalable, and fair locking mechanism for
disaggregated memory (DM) systems. It minimizes the number of
memory node (MN) accesses of lock operations to avoid MN-NIC IOPS
contention with data accesses, while preserving strong fairness
between readers and writers acquiring the lock.

## Hardware Requirements

The experiments require at least two machines (physical or virtual) 
that act as the compute node (CN) and the memory node (MN), respectively.
Each node must equip one ConnectX RDMA NIC of any generation.

## Configure and Build

### Cluster Configuration

The cluster information is maintained in script `scripts/set-env.sh`. 
Please edit the file according to the setup on your own cluster.

### Download Submodules

After cloning the DMLock repository, execute:

```
git submodule update --init --recursive
```

to download all submodules.

### Build Prerequisites

Execute `scripts/build-prerequisites.sh` to build all prerequisites.

### Build the Microbenchmark

To build the microbenchmark for DecLock and all the evaluated baselines, execute
`make SYSTEM=<xxx>`, where `<xxx>` is the name of the lock mechanism 
(`dmlock|dmlock_tf|dslr|caslock`).

Whenever the binaries are updated, execute `scripts/sync-env.sh` to synchronize
the updated files to all nodes in the cluster.

### Build Sherman

#### Dependencies

Sherman relies on CityHash and SSMalloc libraries, which should be installed prior
to building Sherman. Make sure `libcityhash.so` and `libssmalloc.so` are available 
in your system. If they are installed locally (e.g., in `~/.local/lib`), 
set `LD_LIBRARY_PATH` to the installation path before compilation.

#### Configure and Build

Follow the below steps:

1. In DecLock's project root directory: 
  - Execute `make static_lib APPLICATION=sherman` to generate `libdmlock_sherman.a` in the `build/` directory.
  - Execute `source scripts/set-env.sh` to setup build environments.
2. Enter Sherman's directory (`app/sherman`):
  - Execute `mkdir -p build && cd build`;
  - Execute `cmake .. && make -j` to build Sherman.

When building Sherman, there are three compilation flag combinations
corresponding to three tested lock mechanisms:

- Sherman-NH: `cmake -DUSE_LOCAL_LOCK=off -DUSE_DMLOCK=off .. && make -j`
- Sherman: `cmake -DUSE_LOCAL_LOCK=on -DUSE_DMLOCK=off .. && make -j`
- Sherman+DecLock: `cmake -DUSE_LOCAL_LOCK=off -DUSE_DMLOCK=on .. && make -j`

Then, under `app/sherman`, execute `./script/sync-env.sh` to distributed built
binaries to all nodes in the cluster. These binaries will be placed in
`SHERMAN_PATH` specified in `scripts/set-env.sh`. 

### Build ShiftLock

We build our microbenchmark upon [ShiftLock's artifact](https://github.com/thustorage/shiftlock).
For fair comparison, we implemented coroutines and multi-MN supports for ShiftLock. 
The resulting codebase is in `lock/shiftlock`. 
Like editing `set-env.sh` in DecLock, you should edit `scripts/utils/set-nodes.sh`
in ShiftLock's codebase to configure your cluster information.

To build ShiftLock, enter `lock/shiftlock` and execute `cargo build --release`.
After the build finishes, execute `scripts/sync-bin.sh` to synchronize built
binaries to all nodes in the cluster.

## Reproduce Experiment Results

We have prepared a script `getresults.sh` for automatically executing experiments and collecting results.
Its usage is described as follow:

```bash
bash getresults.sh [testname] [lock_mechanism] [all|run|data] [args]
```

For most tests, `lock_mechanism` is one of `caslock|dslr|shiftlock|dmlock_tf|dmlock`.

Supported tests includes:
- Microbenchmark (Fig.12 and Fig.13):
  1. testname=`intra-cn`
  2. testname=`inter-cn`
  3. testname=`mn-num`
  4. testname=`read-ratio`
  5. testname=`exec-time` (latency results are used in Fig.13)
  6. testname=`zipf-alpha`
- Fairness test (Fig.14): testname=`cdf`, args: `[mgc] [read_ratio]`
  + w/o Hierarchy: lock_mechanism=`cqlock`
  + Remote-Prefer: lock_mechanism=`chtlock`, mgc=`1`
  + Local-Prefer: lock_mechanism=`chtlock`, mgc=`256`
  + Local-Bound: lock_mechanism=`chtlock`, mgc=`4`
  + TS-TF: lock_mechanism=`dmlock_tf`
  + TS-PF: lock_mechanism=`dmlock`
- Factor analysis:
  + Section 6.4 (Fig.15 left): testname=`fetch`
  + Section 6.5 (Fig.15 right): testname=`qcap`
  + Section 6.6 (Fig.16 left): testname=`reset`
  + Section 6.7 (Fig.16 right): testname=`failure`
- End-to-end tests (Fig.17):
  + NoSQL store: testname=`store`
  + Transaction: testname=`txn`
  + DB index (Sherman): testname=`sherman`
