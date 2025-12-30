import os
import sys
import math

if len(sys.argv) < 2:
  print("Usage: python3 gen-cdf-data.py [test]")
  exit(1)

test = sys.argv[1]

if "RESULT_PATH" not in os.environ:
  print(
    """
    The project root is not set. 
    Please run `source scripts/set-env.sh` before executing this script.
    """)  
  exit(1)

input_file = os.getenv("RESULT_PATH") + "/{}/latency-acq-0".format(test)

with open(input_file, 'r') as file:
  raw_data = file.readlines()

data = []
for line in raw_data:
  try:
    data.append(float(line.strip()))
  except ValueError:
    pass

sorted_data = sorted(data)
n = len(sorted_data)
cumulative_prob = [i / n for i in range(n)]
inv_prob = [-math.log(1-cumulative_prob[i], 10) for i in range(n)]

print('0.01 0.0 0.0')
for idx in range(n):
  if idx < n * 0.9 and idx % 1000 != 0:
    continue
  if idx < n * 0.99 and idx % 100 != 0:
    continue
  i = idx
  print('{} {} {}'.format(sorted_data[i], inv_prob[i], 
                            cumulative_prob[i] * 100))
print('100000.0 6.0 100.0')