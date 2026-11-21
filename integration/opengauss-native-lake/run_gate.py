#!/usr/bin/env python3
from lake_harness import Blocked, context_from, parser, print_result, run_gate

def main():
    args_parser = parser("Run every required case in a gate; blocked/skip never pass")
    args_parser.add_argument("--gate", required=True)
    args = args_parser.parse_args()
    try:
        result, path = run_gate(args.gate, context_from(args))
        return print_result(result, path)
    except Blocked as exc:
        args_parser.exit(2, f"blocked: {exc}\n")

if __name__ == "__main__":
    raise SystemExit(main())
