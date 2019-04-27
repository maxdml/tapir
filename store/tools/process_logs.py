#TODO: update this script to comply with new output
# See https://github.com/andromeda/tunicast/blob/master/data/sosp/tapir/process_logs.py
import sys

start, end = -1.0, -1.0

duration = float(sys.argv[2]) * 1e6
warmup = duration/3.0

tLatency = []
sLatency = []
fLatency = []

tExtra = 0.0
sExtra = 0.0
fExtra = 0.0

xLatency = []

for line in open(sys.argv[1]):
  if line.startswith('#') or line.strip() == "":
    continue

  line = line.strip().split()
  if not line[0].isdigit() or len(line) < 5:
    continue
  if line[1] != 'total':
    continue

  if start == -1:
    start = float(line[2]) + warmup
    end = start + warmup

  fts = float(line[2])

  if fts < start:
    continue

  if fts > end:
    break

  latency = int(line[4])
  status = int(line[5])
  ttype = -1
  try:
    ttype = int(line[6])
    extra = int(line[7])
  except:
    extra = 0

  if status == 1 and ttype == 2:
    xLatency.append(latency)

  tLatency.append(latency)
  tExtra += extra

  if status == 1:
    sLatency.append(latency)
    sExtra += extra
  else:
    fLatency.append(latency)
    fExtra += extra

if len(tLatency) == 0:
  print "Zero completed transactions.."
  sys.exit()

tLatency.sort()
sLatency.sort()
fLatency.sort()

print "Transactions(All/Success): ", len(tLatency), len(sLatency)
print "Abort Rate: ", (float)(len(tLatency)-len(sLatency))/len(tLatency)
print "Throughput (All/Success): ", len(tLatency)/(end-start)*1e6, len(sLatency)/(end-start)*1e6
print "Average Latency (all): ", sum(tLatency)/float(len(tLatency))
print "Median  Latency (all): ", tLatency[len(tLatency)/2]
print "99%tile Latency (all): ", tLatency[(len(tLatency) * 99)/100]
print "Average Latency (success): ", sum(sLatency)/float(len(sLatency))
print "Median  Latency (success): ", sLatency[len(sLatency)/2]
print "99%tile Latency (success): ", sLatency[(len(sLatency) * 99)/100]
print "Extra (all): ", tExtra
print "Extra (success): ", sExtra
if len(xLatency) > 0:
  print "X Transaction Latency: ", sum(xLatency)/float(len(xLatency))
if len(fLatency) > 0:
  print "Average Latency (failure): ", sum(fLatency)/float(len(tLatency)-len(sLatency))
  print "Extra (failure): ", fExtra
