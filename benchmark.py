import asyncio
import time
import httpx
import matplotlib.pyplot as plt
from rich.console import Console
from rich.table import Table
from rich.progress import track

console = Console()
BASE_URL = "http://localhost:8000"  # change if needed

# ---------- Helpers ----------
async def measure_time(client, url, params=None):
    start = time.perf_counter()
    resp = await client.get(url, params=params or {})
    elapsed = (time.perf_counter() - start) * 1000  # ms
    return elapsed, resp.status_code, resp.json()


# ---------- Benchmarks ----------
async def benchmark_offset(client, total_rows=1_000_000, limit=50, runs=10):
    times = []
    step = total_rows // runs  # spread across dataset
    for i in track(range(runs), description="[cyan]Benchmarking Offset..."):
        offset = i * step
        t, status, _ = await measure_time(
            client, f"{BASE_URL}/api/v1", {"offset": offset, "limit": limit}
        )
        times.append((offset, t))
    return times


async def benchmark_cursor(client, runs=10, limit=50):
    times = []
    after_id = None
    for i in track(range(runs), description="[yellow]Benchmarking Cursor..."):
        params = {"limit": limit}
        if after_id:
            params["after_id"] = after_id
        t, status, data = await measure_time(client, f"{BASE_URL}/api/v3", params=params)

        # fetch the new end cursor (last row id)
        after_id = data.get("page_info", {}).get("end_cursor")
        times.append((after_id or 0, t))  # use actual ID on x-axis
    return times



# ---------- Main ----------
async def main():
    async with httpx.AsyncClient(timeout=60) as client:
        offset_times = await benchmark_offset(client, runs=10)
        cursor_times = await benchmark_cursor(client, runs=10)

    # Console summary
    table = Table(title="Pagination Benchmark Results (ms)")
    table.add_column("Method", style="bold")
    table.add_column("Iterations", justify="right")
    table.add_column("Avg Time (ms)", justify="right")
    table.add_column("Max Time (ms)", justify="right")

    def summarize(name, data):
        xs, ys = zip(*data)
        avg = sum(ys) / len(ys)
        return name, len(data), round(avg, 2), round(max(ys), 2)

    for name, data in [
        ("Offset", offset_times),
        ("Cursor", cursor_times),
    ]:
        method, count, avg, max_ = summarize(name, data)
        table.add_row(method, str(count), str(avg), str(max_))

    console.print(table)

    # Plot results
    plt.figure(figsize=(10, 6))
    plt.plot([x for x, _ in offset_times], [t for _, t in offset_times], marker="o", label="Offset")
    plt.plot([x for x, _ in offset_times], [t for _, t in cursor_times], marker="o", label="Cursor")


    plt.title("Pagination Benchmark: Duration vs Iterations")
    plt.xlabel("Offset / Page / Iteration")
    plt.ylabel("Duration (ms)")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.savefig("pagination_benchmark.png")
    plt.show()


if __name__ == "__main__":
    asyncio.run(main())
