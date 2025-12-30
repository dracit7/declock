
cat tmp | grep periodic | awk '
{
  match($0, /epoch[0-9]+/)
  epoch = substr($0, RSTART+5, RLENGTH-5)
  match($0, /throughput [0-9]+\.[0-9]+ Mops/)
  throughput = substr($0, RSTART+11, RLENGTH-16)
  throughput_sum[epoch] += throughput
}
END {
  for (epoch in throughput_sum) {
    print epoch, throughput_sum[epoch]
  }
}
' | sort -n | awk '{print $1 " " $2}'