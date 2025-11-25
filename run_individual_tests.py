#!/usr/bin/env python
"""Run each test file individually to capture all errors"""
import subprocess
import sys
import os

test_files = [
    "tests/test_version.py",
    "tests/test_smoke.py",
    "tests/test_compiler.py",
    "tests/test_executor.py",
    "tests/test_profiler.py",
    "tests/test_query_parser.py",
]

results = {}

for test_file in test_files:
    print(f"\n{'='*80}")
    print(f"Running: {test_file}")
    print("=" * 80)

    result = subprocess.run(
        [sys.executable, "-m", "pytest", test_file, "-v", "--tb=short"],
        capture_output=True,
        text=True,
        cwd=".",
    )

    results[test_file] = {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }

    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    print(f"Return code: {result.returncode}")

# Write summary
print(f"\n{'='*80}")
print("SUMMARY")
print("=" * 80)

with open("individual_test_results.txt", "w", encoding="utf-8") as f:
    f.write("INDIVIDUAL TEST RESULTS\n")
    f.write("=" * 80 + "\n\n")

    for test_file, result in results.items():
        status = "PASS" if result["returncode"] == 0 else "FAIL"
        print(f"{test_file}: {status}")
        f.write(f"\n{'='*80}\n")
        f.write(f"{test_file}: {status}\n")
        f.write("=" * 80 + "\n")
        f.write(result["stdout"])
        if result["stderr"]:
            f.write("\nSTDERR:\n")
            f.write(result["stderr"])
        f.write("\n")

print("\nDetailed results written to individual_test_results.txt")
