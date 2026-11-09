"""Shared argument parsing for the job entry points (src/jobs/*.py)."""

from __future__ import annotations

import argparse


def parse_args(*extra: tuple[str, dict]) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--bundle-root", required=True)
    p.add_argument("--env", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--sandbox-schema", default="")
    p.add_argument("--landing-root", default="")
    for flag, kwargs in extra:
        p.add_argument(flag, **kwargs)
    args, _ = p.parse_known_args()
    return args
