#!/usr/bin/env python
"""Test runner script to capture detailed test results"""
import subprocess
import sys


def run_tests():
    """Run pytest and capture all output"""
    print("=" * 80)
    print("RUNNING FULL TEST SUITE")
    print("=" * 80)

    # Run pytest with verbose output
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short"],
        capture_output=True,
        text=True,
        cwd=".",
    )

    print("\nSTDOUT:")
    print(result.stdout)

    print("\nSTDERR:")
    print(result.stderr)

    print(f"\nReturn Code: {result.returncode}")
    print("=" * 80)

    # Write to file
    with open("test_results_full.txt", "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("FULL TEST SUITE RESULTS\n")
        f.write("=" * 80 + "\n\n")
        f.write("STDOUT:\n")
        f.write(result.stdout)
        f.write("\n\nSTDERR:\n")
        f.write(result.stderr)
        f.write(f"\n\nReturn Code: {result.returncode}\n")

    print("\nResults written to test_results_full.txt")
    return result.returncode


if __name__ == "__main__":
    sys.exit(run_tests())
