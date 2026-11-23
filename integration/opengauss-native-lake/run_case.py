#!/usr/bin/env python3
from lake_harness import Blocked, context_from, parser, print_result, run_case

def main():
    args_parser = parser("Run one native lake case; 0 pass, 1 fail, 2 blocked/unknown/unimplemented")
    args_parser.add_argument("--id", required=True)
    args = args_parser.parse_args()
    try:
        result, path = run_case(args.id, context_from(args))
        return print_result(result, path)
    except Blocked as exc:
        args_parser.exit(2, f"blocked: {exc}\n")

if __name__ == "__main__":
    raise SystemExit(main())
