import os
import re
import sys


def get_9s(percentile):
    """
    Method that gets the amount of 9s in a percentile
    Args:
        percentile: The percentile to get the amount of 9s for
    Returns:
        The amount of 9s of the percentile, so 0.4->0, 0.92->1, 0.994->2, etc.
    """
    counter = 1
    # Limit to 20 nines, probably won't ever see higher amount than this due to exponential growth
    while counter < 20:
        if percentile + 1/(10 ** counter) < 1.0:
            break
        counter += 1

    return counter - 1


def check_skip(previous, percentile, slack):
    """
    Method that checks if a percentile should be skipped
    Args:
        previous: Previous percentile
        percentile: Percentile to skip or not
        slack: Amount of distance between the percentiles (0-1)
    Returns:
        True if percentile is valid, False if it should be skipped
    """
    prev_9s = get_9s(previous)
    # print(f"prev: {previous}, perc: {percentile}, prev_9s: {prev_9s}")
    return percentile > previous + slack * (1 / (10 ** (prev_9s + 1)))


def parse_hgrm(filename, lower=0.01, upper=0.99991):
    """
    Method that parses a .hgrm file
    Args:
        filename: Name of the file to parse
        lower: Lower percentile limit (range 0-1)
        upper: Upper percentile limit (range 0-1)
        slack: Minimum distance between 2 consecutive percentiles (range 0-1)
    Returns:
        Tuple of 1. list of latencies, 2. list of corresponding percentiles
    """
    latencies = []
    percentiles = []

    # Groups:
    # 1. Latency (format float xy.00000)
    # 2. Percentile (format: float 0.90xyz)
    # 3. Count (format: integer)
    pattern = '^(?: )+([0-9]+\.[0-9]+)(?: )([0-1]\.[0-9]+) +([0-9]+)(?: )*'

    # Previous percentile
    previous = 0.0

    for line in open(filename, 'r'):
        line = line.rstrip('\r\n')  # remove newline
        result = re.match(pattern, line)
        if result:
            # Got match
            (latency, percentile, _) = result.group(1, 2, 3)
            f_percentile = float(percentile)
            if (f_percentile > lower and f_percentile < upper and check_skip(previous, f_percentile, 0.5)):
                previous = f_percentile
                latencies.append(float(latency))
                percentiles.append(f_percentile * 100.0)

    return latencies, percentiles


def main(args):
    if len(args) > 0:
        parse_hgrm(args[0])
    else:
        print(os.getcwd())
        file = "benchmarks/nexmark/3-node/9-million/snapshot-9M"
        l, p = parse_hgrm(file)
        print(l)
        print(p)


if __name__ == "__main__":
    main(sys.argv[1:])
