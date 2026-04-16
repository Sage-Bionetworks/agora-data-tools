#!/usr/bin/env python3
"""
Pre-commit preparation script.

Verifies the expected virtual environment is active, installs the package,
runs the test suite, and (if all tests pass) runs pre-commit on all files.

Developer config (recommended):
  Copy dev_config.yaml.example to dev_config.yaml at the repo root,
  fill in your package manager and environment name, then just run:
    python scripts/prepare_commit.py

CLI overrides (optional, take precedence over config file):
  python scripts/prepare_commit.py -p conda   -e adt_py310
  python scripts/prepare_commit.py -p venv    -e .venv
  python scripts/prepare_commit.py -p poetry  -e my-project
  python scripts/prepare_commit.py -p pipenv  -e my-project

  python scripts/prepare_commit.py --package-manager conda   --env-name adt_py310
  python scripts/prepare_commit.py --package-manager venv    --env-name .venv
  python scripts/prepare_commit.py --package-manager poetry  --env-name my-project
  python scripts/prepare_commit.py --package-manager pipenv  --env-name my-project
"""

import argparse
import os
import pathlib
import subprocess
import sys

import yaml

_REPO_ROOT = pathlib.Path(__file__).parent.parent
_CONFIG_FILE = _REPO_ROOT / "dev_config.yaml"
_CONFIG_EXAMPLE = _REPO_ROOT / "dev_config.yaml.example"


def load_dev_config() -> dict[str, str]:
    """Read package_manager and env_name from dev_config.yaml, if it exists."""
    if not _CONFIG_FILE.exists():
        return {}
    with open(_CONFIG_FILE) as f:
        data = yaml.safe_load(f) or {}
    return {
        k: str(v).strip()
        for k, v in data.items()
        if k in ("package_manager", "env_name")
    }


# ---------------------------------------------------------------------------
# Environment checkers
# ---------------------------------------------------------------------------


def check_conda_env(env_name: str) -> None:
    """Exit with an error if the active conda environment is not the expected one."""
    active = os.environ.get("CONDA_DEFAULT_ENV", "")
    if active != env_name:
        print(
            f"[ERROR] Expected conda environment '{env_name}', "
            f"but current environment is '{active or '(none)'}'.\n"
            f"Please run:  conda activate {env_name}"
        )
        sys.exit(1)
    print(f"[OK] Conda environment '{active}' is active.")


def check_venv_env(env_name: str) -> None:
    """Exit with an error if the active venv/virtualenv does not match env_name."""
    active_path = os.environ.get("VIRTUAL_ENV", "")
    active_name = os.path.basename(active_path) if active_path else ""
    if active_name != env_name:
        print(
            f"[ERROR] Expected venv '{env_name}', "
            f"but current environment is '{active_name or '(none)'}'.\n"
            f"Please activate your virtual environment before running this script."
        )
        sys.exit(1)
    print(f"[OK] venv '{active_name}' is active.")


def check_poetry_env(env_name: str) -> None:
    """Exit with an error if the active Poetry environment does not contain env_name."""
    active_path = os.environ.get("VIRTUAL_ENV", "")
    active_name = os.path.basename(active_path) if active_path else ""
    if env_name not in active_name:
        print(
            f"[ERROR] Expected Poetry environment containing '{env_name}', "
            f"but current environment is '{active_name or '(none)'}'.\n"
            f"Please run:  poetry shell"
        )
        sys.exit(1)
    print(f"[OK] Poetry environment '{active_name}' is active.")


def check_pipenv_env(env_name: str) -> None:
    """Exit with an error if the active Pipenv environment does not contain env_name."""
    active_path = os.environ.get("VIRTUAL_ENV", "")
    active_name = os.path.basename(active_path) if active_path else ""
    if env_name not in active_name:
        print(
            f"[ERROR] Expected Pipenv environment containing '{env_name}', "
            f"but current environment is '{active_name or '(none)'}'.\n"
            f"Please run:  pipenv shell"
        )
        sys.exit(1)
    print(f"[OK] Pipenv environment '{active_name}' is active.")


PACKAGE_MANAGER_CHECKERS = {
    "conda": check_conda_env,
    "venv": check_venv_env,
    "virtualenv": check_venv_env,
    "poetry": check_poetry_env,
    "pipenv": check_pipenv_env,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate your dev environment, install the package, run tests, and run pre-commit.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n".join(
            [
                "dev config (recommended one-time setup):",
                f"  Copy {_CONFIG_EXAMPLE.name} to {_CONFIG_FILE.name} at the repo root,",
                "  fill in your values, then run this script with no arguments.",
                "",
                "CLI examples (override config file):",
                "  %(prog)s -p conda   -e adt_py310   (--package-manager conda   --env-name adt_py310)",
                "  %(prog)s -p venv    -e .venv        (--package-manager venv    --env-name .venv)",
                "  %(prog)s -p poetry  -e my-project   (--package-manager poetry  --env-name my-project)",
                "  %(prog)s -p pipenv  -e my-project   (--package-manager pipenv  --env-name my-project)",
            ]
        ),
    )
    parser.add_argument(
        "-p",
        "--package-manager",
        default=None,
        choices=list(PACKAGE_MANAGER_CHECKERS.keys()),
        metavar="MANAGER",
        help=f"Package manager used for your dev environment. Supported: {', '.join(PACKAGE_MANAGER_CHECKERS)}. "
        f"If omitted, read from {_CONFIG_FILE.name} at the repo root.",
    )
    parser.add_argument(
        "-e",
        "--env-name",
        default=None,
        metavar="NAME",
        help=f"Name of the virtual environment to verify is active. "
        f"If omitted, read from {_CONFIG_FILE.name} at the repo root.",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point: validate environment, install, test, and run pre-commit."""
    args = parse_args()
    dev_config = load_dev_config()

    package_manager = args.package_manager or dev_config.get("package_manager")
    env_name = args.env_name or dev_config.get("env_name")

    missing = [
        name
        for name, val in [
            ("--package-manager", package_manager),
            ("--env-name", env_name),
        ]
        if not val
    ]
    if missing:
        print(
            f"[ERROR] Missing required value(s): {', '.join(missing)}.\n"
            f"Either pass them as CLI arguments or create {_CONFIG_FILE} from the example:\n"
            f"  cp {_CONFIG_EXAMPLE} {_CONFIG_FILE}\n"
            f"Then fill in your package_manager and env_name."
        )
        sys.exit(1)

    if package_manager not in PACKAGE_MANAGER_CHECKERS:
        print(
            f"[ERROR] Unsupported package manager '{package_manager}'. "
            f"Supported: {', '.join(PACKAGE_MANAGER_CHECKERS)}."
        )
        sys.exit(1)

    checker = PACKAGE_MANAGER_CHECKERS[package_manager]
    checker(env_name)

    run([sys.executable, "-m", "pip", "install", "."], "pip install .")
    run([sys.executable, "-m", "pytest", "tests/"], "pytest tests/")
    run_pre_commit()
    print("\nAll steps passed. Your changes are ready to commit!")


if __name__ == "__main__":
    main()
