from __future__ import annotations

import argparse
import logging
import random

import numpy as np

from agenda.config import load_config
from agenda.pipeline import Pipeline
from agenda.utils.logging import init_logging


def _set_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)


def main() -> None:
    parser = argparse.ArgumentParser(description="Agenda 2026 pipeline")

    subparsers = parser.add_subparsers(dest="command", required=True)

    def _add_config_arg(p: argparse.ArgumentParser) -> None:
        p.add_argument("--config", default="config.yaml", help="Path to config YAML")

    validate_p = subparsers.add_parser("validate-config", help="Validate config")
    _add_config_arg(validate_p)

    ingest_p = subparsers.add_parser("ingest", help="Run ingestion")
    _add_config_arg(ingest_p)
    ingest_p.add_argument("target", nargs="?", default=None)

    build_p = subparsers.add_parser("build", help="Run build steps")
    _add_config_arg(build_p)
    build_p.add_argument("target", nargs="?", default=None)

    model_p = subparsers.add_parser("model", help="Run model steps")
    _add_config_arg(model_p)
    model_p.add_argument("target", nargs="?", default=None)

    policy_p = subparsers.add_parser("policy", help="Run policy steps")
    _add_config_arg(policy_p)
    policy_p.add_argument("target", nargs="?", default=None)

    render_p = subparsers.add_parser("render", help="Render figures")
    _add_config_arg(render_p)
    render_p.add_argument("target", nargs="?", default=None)

    paper_p = subparsers.add_parser("paper", help="Build paper outputs")
    _add_config_arg(paper_p)
    all_p = subparsers.add_parser("all", help="Run full pipeline")
    _add_config_arg(all_p)

    args = parser.parse_args()

    cfg = load_config(args.config)
    init_logging(cfg.paths.logs_dir)
    _set_seed(cfg.project.seed)

    pipe = Pipeline(cfg)

    logging.getLogger(__name__).info("starting command %s", args.command)

    if args.command == "validate-config":
        pipe.validate_config()
    elif args.command == "ingest":
        pipe.ingest(args.target)
    elif args.command == "build":
        pipe.build(args.target)
    elif args.command == "model":
        pipe.model(args.target)
    elif args.command == "policy":
        pipe.policy(args.target)
    elif args.command == "render":
        pipe.render(args.target)
    elif args.command == "paper":
        pipe.paper()
    elif args.command == "all":
        pipe.run_all()


if __name__ == "__main__":
    main()
