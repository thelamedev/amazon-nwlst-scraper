import sys
from pathlib import Path

log_path = Path("./")
log_file_path = sys.argv[1] if len(sys.argv) > 1 else "logs"


xpath_timings = []
page_load_timings = []
operation_timings = []

with open(log_path / log_file_path, "r") as f:
    while True:
        line = f.readline()
        if not line:
            break
        if "XPath evaluation took" in line:
            xpath_timings.append(float(line.split("XPath evaluation took ")[1].split("s")[0].strip()))

        elif "Page load took" in line:
            page_load_timings.append(float(line.split("Page load took ")[1].split("s")[0].strip()))

        elif "time_elapsed" in line:
            operation_timings.append(float(line.split("time_elapsed: ")[1].split("s")[0].strip()))

avg_xpath_timings = sum(xpath_timings) / len(xpath_timings)
xpath_timings_range = min(xpath_timings), max(xpath_timings)
median_xpath_timings = sorted(xpath_timings)[len(xpath_timings) // 2]
xpath_p95 = sorted(xpath_timings)[int(len(xpath_timings) * 0.95)]

avg_page_load_timings = sum(page_load_timings) / len(page_load_timings)
page_load_timings_range = min(page_load_timings), max(page_load_timings)
median_page_load_timings = sorted(page_load_timings)[len(page_load_timings) // 2]
page_load_p95 = sorted(page_load_timings)[int(len(page_load_timings) * 0.95)]

avg_operation_timings = sum(operation_timings) / len(operation_timings)
operation_timings_range = min(operation_timings), max(operation_timings)
median_operation_timings = sorted(operation_timings)[len(operation_timings) // 2]
operation_p95 = sorted(operation_timings)[int(len(operation_timings) * 0.95)]

print("Timings:")
print(
    f"\tXPath | Mean: {avg_xpath_timings:.2f}s | Range: {xpath_timings_range} | Median: {median_xpath_timings:.2f}s | P95: {xpath_p95:.2f}s"
)
print(
    f"\tPage Load | Mean: {avg_page_load_timings:.2f}s | Range: {page_load_timings_range} | Median: {median_page_load_timings:.2f}s | P95: {page_load_p95:.2f}s"
)
print(
    f"\tOperation | Mean: {avg_operation_timings:.2f}s | Range: {operation_timings_range} | Median: {median_operation_timings:.2f}s | P95: {operation_p95:.2f}s"
)
