#!/usr/bin/env python3
"""
Pre-commit preparation script.

Verifies the adt_py310 conda environment is active, installs the package,
runs the test suite, and (if all tests pass) runs pre-commit on all files.
"""

import subprocess
import sys


def run(cmd: list[str], description: str) -> subprocess.CompletedProcess:
    """Run a shell command, printing a header and exiting on failure."""
    print(f"\n{'='*60}")
    print(f"  {description}")
    print(f"  $ {' '.join(cmd)}")
    print(f"{'='*60}\n")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"\n[FAILED] {description} exited with code {result.returncode}.")
        sys.exit(result.returncode)
    print(f"\n[OK] {description}")
    return result


def run_pre_commit() -> None:
    """Run pre-commit, retrying once if it exits non-zero.

    Some hooks (black, ruff, autoflake) auto-fix files on the first pass and
    exit with a non-zero code to signal that changes were made. A second run
    confirms everything is clean. If pre-commit still fails on the second pass,
    the script exits with an error.
    """
    cmd = ["pre-commit", "run", "--all-files"]
    description = "pre-commit run --all-files"

    print(f"\n{'='*60}")
    print(f"  {description}")
    print(f"  $ {' '.join(cmd)}")
    print(f"{'='*60}\n")

    first = subprocess.run(cmd)
    if first.returncode == 0:
        print(f"\n[OK] {description}")
        return

    print(
        "\n[INFO] pre-commit made fixes on the first pass — re-running to confirm clean state...\n"
    )
    second = subprocess.run(cmd)
    if second.returncode != 0:
        print(
            f"\n[FAILED] {description} exited with code {second.returncode} after retry."
        )
        sys.exit(second.returncode)
    print(f"\n[OK] {description}")


def check_conda_env(expected: str = "adt_py310") -> None:
    """Exit with an error if the active conda environment is not the expected one."""
    import os

    active = os.environ.get("CONDA_DEFAULT_ENV", "")
    if active != expected:
        print(
            f"[ERROR] Expected conda environment '{expected}', "
            f"but current environment is '{active or '(none)'}'.\n"
            f"Please run:  conda activate {expected}"
        )
        sys.exit(1)
    print(f"[OK] Conda environment '{active}' is active.")


def main() -> None:
    """Entry point: validate environment, install, test, and run pre-commit."""
    check_conda_env("adt_py310")
    run([sys.executable, "-m", "pip", "install", "."], "pip install .")
    run([sys.executable, "-m", "pytest", "tests/"], "pytest tests/")
    run_pre_commit()
    print("\nAll steps passed. Your changes are ready to commit!")


if __name__ == "__main__":
    main()
