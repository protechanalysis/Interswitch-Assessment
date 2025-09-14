import sys
from collections import defaultdict, Counter
import heapq

def parse_input():
    """Parse input from stdin and return a list of (colleague, [bands]) tuples."""
    first_line = sys.stdin.readline()
    try:
        line_count = int(first_line.strip())
    except ValueError:
        print("Invalid input for number of lines.")
        return []

    records = []
    for _ in range(line_count):
        line = sys.stdin.readline().strip()
        if not line or ':' not in line:
            continue
        name, bands_string = line.split(':', 1)
        bands = [band.strip() for band in bands_string.split(',') if band.strip()]
        records.append((name.strip(), bands))
    return records

### Problem 1
def top_two_bands(records):
    """Problem 1: Find top 2 most liked bands (including ties).
    If there is a tie, output all bands with counts equal to the top 2 frequencies."""
    band_counts = Counter()
    for _, bands in records:
        band_counts.update(bands)

    if not band_counts:
        return

    two_largest = heapq.nlargest(2, set(band_counts.values()))
    if len(two_largest) == 1:
        second_highest = two_largest[0]
    else:
        second_highest = two_largest[1]

    top_bands = [band for band, count in band_counts.items() if count >= second_highest]

    for band in top_bands:
        print(band)


### Problem 2
def bands_to_colleagues(records):
    """Output each band followed by the colleagues who like it."""
    band_to_colleagues = defaultdict(list)
    for name, bands in records:
        for band in bands:
            band_to_colleagues[band].append(name)

    for band in sorted(band_to_colleagues.keys()):
        colleagues = sorted(set(band_to_colleagues[band]))
        print(f"{band}: {', '.join(colleagues)}")


if __name__ == "__main__":
    records = parse_input()
    top_two_bands(records)
    bands_to_colleagues(records)
