
# The username must be consistent on all machines in the cluster.
export USERNAME=''

# The paths to the project root of DMLock, 
# must be identical on all machines in the cluster.
export DMLOCK_PATH=/path/to/dmlock
export DMLOCK_LOG_PATH=$DMLOCK_PATH/log
export SHERMAN_PATH=/path/to/sherman

# Path to store experiment results.
export RESULT_PATH=/Users/hanzezhang/Work/projects/dmlock/data

# Specify machines in the cluster.
# Format: <hostname>-<ip>-<numa_id>
export HOSTS="
host1-192.168.12.1-0 
host1-192.168.12.1-1
host2-192.168.12.2-0 
host2-192.168.12.2-1
...
"

# Variables used by getresults.sh.
# MASTER_NODE is the node running `run-on-all.sh`.
# REPORT_PATH is the path containing generated results.
MASTER_NODE=localhost
REPORT_PATH=/path/to/results
