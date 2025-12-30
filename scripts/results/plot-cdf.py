import sys
import os
import numpy as np
from matplotlib import pyplot as plt

if len(sys.argv) < 2:
  print("Usage: python3 plot-cdf.py [function]")
  exit(1)

function = sys.argv[1]

if "RESULT_PATH" not in os.environ:
  print(
    """
    The project root is not set. 
    Please run `source scripts/set-env.sh` before executing this script.
    """)  
  exit(1)
  
RESULT_DIR = os.getenv("RESULT_PATH")
OUTPUT_DIR = RESULT_DIR + "/figures/"
INPUT_DIR = RESULT_DIR + "/data/"

FIGURE_SIZE = (6, 5)
BIG_FIGURE_SIZE = (10, 5)

def plot_cdf(data, axis, label):
  count, bins_count = np.histogram(data, bins=1000000)
  cdf_x = bins_count[1:]
  cdf_y = np.cumsum(count / sum(count))
  axis.plot(cdf_x, cdf_y, label=label)
  axis.legend()

def generate_fig(outf, xlim_l, xlim_r, logx=True):
  if logx:
    plt.xscale("log")
  plt.ylim([0, 1.04])
  plt.xlim([xlim_l, xlim_r])
  plt.xlabel("latency (us)")
  plt.yticks([0.5,0.9,1.00], ["50%", "90%", "100%"])
  plt.savefig(outf + ".png")
  # plt.savefig(outf + ".eps")

def cdf():
  if len(sys.argv) < 5:
    print("Usage: python3 plot-cdf.py cdf [bench] [cores] [test]")
    exit(1)

  test = "{}-{}-{}".format(sys.argv[2], sys.argv[3], sys.argv[4])
  outf = OUTPUT_DIR + "cdf-{}".format(test)
  _, axis = plt.subplots(figsize=FIGURE_SIZE)

  inf = INPUT_DIR + "{}/latency-rel".format(test)
  array = np.fromfile(inf, dtype=float, sep="\n")
  plot_cdf(array.tolist(), axis, test)
  
  generate_fig(outf, 0.1, 10000.0)

def compare_cdf():
  bench = sys.argv[2]
  cores = sys.argv[3]
  test = sys.argv[4]

  if "wo-cohort" in test:
    files = [
      ("{}-1core-{}/latency-acq".format(bench, test), "1core"),
      ("{}-2core-{}/latency-acq".format(bench, test), "2core"),
      ("{}-4core-{}/latency-acq".format(bench, test), "4core"),
      ("{}-8core-{}/latency-acq".format(bench, test), "8core"),
      ("{}-16core-{}/latency-acq".format(bench, test), "16core"),
    ]

  elif "mgc" in test:
    files = [
      ("{}-16core-mgc1-cohort/latency-acq".format(bench), "mgc=1"),
      ("{}-16core-mgc2-cohort/latency-acq".format(bench), "mgc=2"),
      ("{}-16core-mgc4-cohort/latency-acq".format(bench), "mgc=4"),
      ("{}-16core-mgc8-cohort/latency-acq".format(bench), "mgc=8"),
      ("{}-16core-mgc16-cohort/latency-acq".format(bench), "mgc=16"),
      # ("{}-16core-wo-cohort/latency-acq".format(bench), "wo-cohort"),
      ("{}-16core-cohort-ts/latency-acq".format(bench), "timestamp"),
    ]
    test = "cohort"
  
  elif "release" in test:
    files = [
      ("{}-16core-mgc1-cohort/latency-rel".format(bench), "mgc=1"),
      ("{}-16core-mgc2-cohort/latency-rel".format(bench), "mgc=2"),
      ("{}-16core-mgc4-cohort/latency-rel".format(bench), "mgc=4"),
      ("{}-16core-mgc8-cohort/latency-rel".format(bench), "mgc=8"),
      ("{}-16core-mgc16-cohort/latency-rel".format(bench), "mgc=16"),
      # ("{}-16core-wo-cohort/latency-rel".format(bench), "wo-cohort"),
      ("{}-16core-cohort-ts/latency-rel".format(bench), "timestamp"),
    ]

  elif "batch" in test:
    files = [
      ("{}-16core-1batch/latency-acq".format(bench), "1 batch"),
      ("{}-16core-2batch/latency-acq".format(bench), "2 batch"),
      ("{}-16core-4batch/latency-acq".format(bench), "4 batch"),
      ("{}-16core-8batch/latency-acq".format(bench), "8 batch"),
      ("{}-16core-16batch/latency-acq".format(bench), "16 batch"),
    ]
    test = "batch-num"

  outf = "{}/cdf-{}-{}".format(OUTPUT_DIR, bench, test)
  _, axis = plt.subplots(figsize=FIGURE_SIZE)

  for file in files:
    inf = INPUT_DIR + file[0]
    array = np.fromfile(inf, dtype=float, sep="\n")
    plot_cdf(array.tolist(), axis, file[1])
  
  generate_fig(outf, 0.1, 10000.0)
  # generate_fig(outf, 0, 15, False)

def main():
  if function == "cdf":
    cdf()
    return
  if function == "compare_cdf":
    compare_cdf()
    return

if __name__=="__main__":
  main()