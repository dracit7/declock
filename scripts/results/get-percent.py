import sys
import os
import numpy as np
from matplotlib import pyplot as plt

if len(sys.argv) < 3:
  print("Usage: python3 get-percent.py [test]")
  exit(1)

test = sys.argv[1]
lat = sys.argv[2]

if "RESULT_PATH" not in os.environ:
  print(
    """
    The project root is not set. 
    Please run `source scripts/set-env.sh` before executing this script.
    """)  
  exit(1)
  
RESULT_DIR = os.getenv("RESULT_PATH")
INPUT_DIR = RESULT_DIR

def array_p(array, percent):
  my_array = np.array(array)
  return np.percentile(my_array, percent, method='higher')

def main():
  target_pos = [50, 90, 99, 99.9, 99.99]
  inf = "{}/{}/latency-{}".format(INPUT_DIR, test, lat)
  array = np.fromfile(inf, dtype=float, sep="\n")
  for pos in target_pos:
    print("p{}: {}".format(pos, array_p(array.tolist(), pos)))

if __name__=="__main__":
  main()