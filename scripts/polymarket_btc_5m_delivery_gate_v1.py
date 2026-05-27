#!/usr/bin/env python3
from __future__ import annotations

import argparse
import dataclasses
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
KEY_SCRIPTS = [
    "scripts/polymarket_btc_5m_delivery_gate_v1.py",
    "scripts/aws-spot-consensus-01.py",
    "scripts/polymarket_btc_5m_decision_engine_v1.py",
    "scripts/test_polymarket_btc_5m_decision_engine_v1.py",
    "scripts/run_polymarket_btc_5m_canonical_signal_dry_run_v1.py",
    "scripts/replay_polymarket_btc_5m_canonical_samples_v1.py",
    "scripts/join_polymarket_btc_5m_teacher_compatible_samples_v1.py",
    "scripts/compare_aws_mainline02_to_local_teacher_set_v1.py",
    "scripts/sync_aws_polymarket_btc_5m_runtime_to_ssd_v1.py",
    "scripts/trade_polymarket_crypto_5m_btc_live_v1.py",
]
KEY_SHELL_SCRIPTS = [
    "scripts/start_aws_high_fidelity_orderbook_diagnostic_v1.sh",
    "scripts/start_aws_native_robust01_shadow_dry_run_v1.sh",
]
AWS_HOST = "18.201.244.97"
AWS_USER = "ubuntu"
AWS_KEY = Path.home() / ".ssh/grandmatrix-ireland-key.pem"


class GateFailure(Exception):
    pass


def run(cmd: list[str], *, cwd: Path = ROOT, timeout: int = 60) -> str:
    result = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, timeout=timeout)
    if result.returncode != 0:
        raise GateFailure(
            f"command failed: {' '.join(cmd)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return (result.stdout + result.stderr).strip()


def check_py_compile() -> str:
    return run(["python3", "-m", "py_compile", *KEY_SCRIPTS])


def check_shell_syntax() -> str:
    outputs = []
    for script in KEY_SHELL_SCRIPTS:
        run(["bash", "-n", script])
        outputs.append(script)
    return "shell syntax ok: " + ", ".join(outputs)


def check_decision_tests() -> str:
    return run(["python3", "scripts/test_polymarket_btc_5m_decision_engine_v1.py"])


def check_no_strategy_config_defaults() -> str:
    sys.path.insert(0, str(ROOT / "scripts"))
    from polymarket_btc_5m_decision_engine_v1 import StrategyConfig  # type: ignore

    offenders = []
    for field in dataclasses.fields(StrategyConfig):
        if field.default is not dataclasses.MISSING or field.default_factory is not dataclasses.MISSING:
            offenders.append(field.name)
    if offenders:
        raise GateFailure(f"StrategyConfig has hidden defaults: {', '.join(offenders)}")
    return "StrategyConfig has no behavior defaults"


def check_strategy_profiles_explicit() -> str:
    sys.path.insert(0, str(ROOT / "scripts"))
    from polymarket_btc_5m_decision_engine_v1 import (  # type: ignore
        STRATEGY_PROFILE_NAMES,
        StrategyConfig,
        strategy_profile,
    )

    required = {field.name for field in dataclasses.fields(StrategyConfig)}
    summary = {}
    for profile in STRATEGY_PROFILE_NAMES:
        config = strategy_profile(profile)
        values = dataclasses.asdict(config)
        missing = sorted(name for name in required if values.get(name, None) is None and name not in {
            "up_min_coinbase_delta_usd",
            "up_min_aligned_book_imbalance",
        })
        if missing:
            raise GateFailure(f"{profile} missing explicit values: {', '.join(missing)}")
        summary[profile] = values
    return json.dumps(summary, ensure_ascii=False, sort_keys=True)


def scan_forbidden_patterns() -> str:
    targets = [ROOT / path for path in KEY_SCRIPTS]
    forbidden_literals = [
        "--max-price-to-" + "beat-observed-second",
        "max_price_to_" + "beat_observed_second",
    ]
    patterns = {
        "max_observed_first_second default": re.compile(r"max_observed_first_second\s*:\s*float\s*="),
        "threshold default": re.compile(r"threshold_usd\s*:\s*float\s*="),
        "entry_start default": re.compile(r"entry_start_second\s*:\s*float\s*="),
        "entry_end default": re.compile(r"entry_end_second\s*:\s*float\s*="),
        "entry_cap default": re.compile(r"entry_cap\s*:\s*float\s*="),
        "cvd default": re.compile(r"min_aligned_cvd_15s\s*:\s*float\s*="),
        "max observed argparse default": re.compile(r"--max-observed-first-second[^\n]*default\s*="),
        "price_to_beat argparse default": re.compile(r"--max-price-to-beat[^\n]*default\s*="),
        "threshold argparse default": re.compile(r"--threshold-usd[^\n]*default\s*="),
    }
    hits = []
    for path in targets:
        text = path.read_text()
        for literal in forbidden_literals:
            if literal in text:
                hits.append(f"{path.relative_to(ROOT)}: forbidden literal: {literal}")
        for label, pattern in patterns.items():
            for match in pattern.finditer(text):
                line_no = text.count("\n", 0, match.start()) + 1
                hits.append(f"{path.relative_to(ROOT)}:{line_no}: {label}")
    if hits:
        raise GateFailure("forbidden hidden defaults found:\n" + "\n".join(hits))
    return "no forbidden hidden strategy defaults found"


def scan_trading_risks() -> str:
    live_path = ROOT / "scripts/trade_polymarket_crypto_5m_btc_live_v1.py"
    text = live_path.read_text()
    findings = []
    duration_match = re.search(
        r'--duration-seconds["\'][^\n]*default\s*=\s*([0-9]+(?:\.[0-9]+)?)',
        text,
    )
    if duration_match and float(duration_match.group(1)) > 0:
        findings.append("live/dry-run duration-seconds has a positive default")
    if re.search(r"ENTRY_LATENCY_MS\s*=\s*(?!0\b)\d+", text):
        findings.append("ENTRY_LATENCY_MS is non-zero")
    if re.search(r"--max-observed-first-second[^\n]*default\s*=\s*10(?:\.0)?", text):
        findings.append("max observed argparse still defaults to 10")
    if "max_locked" in text.lower() and "skip" in text.lower():
        findings.append("possible max_locked skip path still needs manual review")
    if findings:
        raise GateFailure("trading risk scan failed:\n" + "\n".join(findings))
    return "trading risk scan passed for hidden delay/observation defaults"


def check_teacher_compatible_launcher() -> str:
    path = ROOT / "scripts/start_aws_teacher_compatible_shadow_v1.sh"
    text = path.read_text()
    findings = []
    if "--include-pm-orderbook" in text:
        findings.append("teacher-compatible launcher must not embed Polymarket orderbook inside signal-source samples")
    if "run_polymarket_btc_5m_canonical_signal_dry_run_v1.py" not in text:
        findings.append("teacher-compatible launcher does not start canonical dry-run")
    if "join_polymarket_btc_5m_teacher_compatible_samples_v1.py" not in text:
        findings.append("teacher-compatible launcher does not start teacher-compatible joiner")
    if "--samples-csv \"$TEACHER_SAMPLES\"" not in text:
        findings.append("dry-run is not wired to teacher-compatible samples")
    if findings:
        raise GateFailure("teacher-compatible launcher scan failed:\n" + "\n".join(findings))
    return "teacher-compatible launcher uses separate signal/orderbook collectors and dry-run consumes joined samples"


def check_high_fidelity_orderbook_diagnostic_launcher() -> str:
    path = ROOT / "scripts/start_aws_high_fidelity_orderbook_diagnostic_v1.sh"
    text = path.read_text()
    findings = []
    required = [
        "collect_polymarket_btc_5m_orderbook_only_v1.py",
        "orderbook_diagnostics",
        "--snapshot-interval-seconds \"$SNAPSHOT_INTERVAL_SECONDS\"",
        "--market-refresh-seconds \"$MARKET_REFRESH_SECONDS\"",
        "--raw-gz \"$RAW_GZ\"",
        "--snapshots-csv \"$SNAPSHOTS_CSV\"",
        "--disable-snapshot-jsonl-gz",
    ]
    for snippet in required:
        if snippet not in text:
            findings.append(f"missing required diagnostic launcher snippet: {snippet}")
    forbidden = [
        "--disable-raw-events",
        "run_polymarket_btc_5m_canonical_signal_dry_run_v1.py \\",
        "trade_polymarket_crypto_5m_btc_live_v1.py",
        "join_polymarket_btc_5m_teacher_compatible_samples_v1.py \\",
        "--samples-csv \"$TEACHER_SAMPLES\"",
    ]
    for snippet in forbidden:
        if snippet in text:
            findings.append(f"forbidden diagnostic launcher snippet: {snippet}")
    if 'RUN_DURATION_SECONDS="${RUN_DURATION_SECONDS:-129600}"' not in text:
        findings.append("diagnostic duration must default to 129600 seconds")
    if 'SNAPSHOT_INTERVAL_SECONDS="${SNAPSHOT_INTERVAL_SECONDS:-0.25}"' not in text:
        findings.append("diagnostic snapshot interval must default to 0.25 seconds")
    if 'MARKET_REFRESH_SECONDS="${MARKET_REFRESH_SECONDS:-0.5}"' not in text:
        findings.append("diagnostic market refresh must default to 0.5 seconds")
    if findings:
        raise GateFailure("high-fidelity diagnostic launcher scan failed:\n" + "\n".join(findings))
    return "high-fidelity orderbook diagnostic launcher saves raw events/snapshots and does not start dry-run/live"


def check_aws_health() -> str:
    if not AWS_KEY.exists():
        raise GateFailure(f"AWS key missing: {AWS_KEY}")
    ssh = [
        "ssh",
        "-i",
        str(AWS_KEY),
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=8",
        f"{AWS_USER}@{AWS_HOST}",
    ]
    remote_py = r"""
import os
import subprocess
import sys
from pathlib import Path


def iter_cmds():
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            raw = (Path("/proc") / name / "cmdline").read_bytes()
        except OSError:
            continue
        args = [part.decode("utf-8", "replace") for part in raw.split(b"\0") if part]
        if args:
            yield name, args


def arg_after(args, flag):
    for index, item in enumerate(args[:-1]):
        if item == flag:
            return args[index + 1]
    return ""


robust_dryrun = []
robust_collector = []
diag = []
other_dryrun = []
for pid, args in iter_cmds():
    cmd = " ".join(args)
    if "run_polymarket_btc_5m_canonical_signal_dry_run_v1.py" in cmd:
        if "aws_native_robust01" in cmd:
            robust_dryrun.append((pid, args, cmd))
        else:
            other_dryrun.append((pid, args, cmd))
    if "collect_polymarket_btc_5m_signal_sources_v1.py" in cmd and "aws_native_robust01" in cmd:
        robust_collector.append((pid, args, cmd))
    if "collect_polymarket_btc_5m_orderbook_only_v1.py" in cmd and "orderbook_diagnostics" in cmd:
        diag.append((pid, args, cmd))

if robust_dryrun:
    if not robust_collector:
        print("missing AWS native robust-01 signal collector process", file=sys.stderr)
        sys.exit(41)
    _, args, cmd = robust_dryrun[0]
    samples = arg_after(args, "--samples-csv")
    report = arg_after(args, "--report")
    if not samples or not report:
        print("cannot parse robust dry-run output paths", file=sys.stderr)
        sys.exit(42)
    if not Path(samples).is_file() or Path(samples).stat().st_size <= 0:
        print(f"robust samples csv missing or empty: {samples}", file=sys.stderr)
        sys.exit(43)
    if not Path(report).is_file() or Path(report).stat().st_size <= 0:
        print(f"robust dry-run report missing or empty: {report}", file=sys.stderr)
        sys.exit(44)
    print("robust_collector=" + robust_collector[0][2])
    print("robust_dryrun=" + cmd)
    print("samples_csv=" + samples)
    print("report=" + report)
    subprocess.run(["ls", "-lh", samples, report], check=False)
    subprocess.run(["df", "-h", "/"], check=False)
    sys.exit(0)

if not diag:
    print("missing AWS high-fidelity orderbook diagnostic process", file=sys.stderr)
    sys.exit(31)
if other_dryrun:
    print("unexpected AWS dry-run still running: " + other_dryrun[0][2], file=sys.stderr)
    sys.exit(32)
_, args, cmd = diag[0]
raw = arg_after(args, "--raw-gz")
snapshots = arg_after(args, "--snapshots-csv")
if not raw or not snapshots:
    print("cannot parse diagnostic output paths", file=sys.stderr)
    sys.exit(33)
if not Path(raw).is_file() or Path(raw).stat().st_size <= 0:
    print(f"diagnostic raw gz missing or empty: {raw}", file=sys.stderr)
    sys.exit(34)
if not Path(snapshots).is_file() or Path(snapshots).stat().st_size <= 0:
    print(f"diagnostic snapshots csv missing or empty: {snapshots}", file=sys.stderr)
    sys.exit(35)
print("diagnostic=" + cmd)
print("raw_gz=" + raw)
print("snapshots_csv=" + snapshots)
subprocess.run(["ls", "-lh", raw, snapshots], check=False)
subprocess.run(["df", "-h", "/"], check=False)
"""
    return run([*ssh, f"bash -lc {shlex.quote('python3 -c ' + shlex.quote(remote_py))}"], timeout=30)


def main() -> int:
    parser = argparse.ArgumentParser(description="Polymarket BTC 5M pre-delivery gate")
    parser.add_argument("--aws-health", action="store_true", help="lightly verify AWS runtime process state and output files")
    args = parser.parse_args()

    if args.aws_health:
        checks = [("aws_health_process_output", check_aws_health)]
    else:
        checks = [
            ("py_compile", check_py_compile),
            ("shell_syntax", check_shell_syntax),
            ("decision_tests", check_decision_tests),
            ("no_strategy_config_defaults", check_no_strategy_config_defaults),
            ("strategy_profiles_explicit", check_strategy_profiles_explicit),
            ("forbidden_pattern_scan", scan_forbidden_patterns),
            ("trading_risk_scan", scan_trading_risks),
            ("teacher_compatible_launcher", check_teacher_compatible_launcher),
            ("high_fidelity_orderbook_diagnostic_launcher", check_high_fidelity_orderbook_diagnostic_launcher),
        ]

    report = []
    try:
        for name, fn in checks:
            output = fn()
            report.append({"check": name, "status": "PASS", "output": output})
    except Exception as exc:
        report.append({"check": name if "name" in locals() else "unknown", "status": "FAIL", "output": str(exc)})
        print(json.dumps({"status": "FAIL", "checks": report}, ensure_ascii=False, indent=2))
        return 1

    print(json.dumps({"status": "PASS", "checks": report}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
