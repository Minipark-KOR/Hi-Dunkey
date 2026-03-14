#!/usr/bin/env python3
"""Collector canonical 이름 변경 자동화 도구.

기본 동작은 dry-run이며, --apply를 주어야 실제 파일을 수정합니다.
수정 대상:
- constants/collector_names.py: canonical 상수 값(old -> new)
- config/config.yaml: collectors.<old> 키를 collectors.<new>로 이동
- constants/domains.py: DOMAIN_CONFIG 내 collector_name 값(old -> new)
"""

from __future__ import annotations

import argparse
import importlib
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any


ROOT = Path(__file__).resolve().parent.parent
COLLECTOR_NAMES_FILE = ROOT / "constants" / "collector_names.py"
DOMAINS_FILE = ROOT / "constants" / "domains.py"
CONFIG_FILE = ROOT / "config" / "config.yaml"


def _load_yaml_module():
    try:
        return importlib.import_module("yaml")
    except ModuleNotFoundError as exc:
        raise RuntimeError("PyYAML이 필요합니다. 'pip install pyyaml' 후 다시 실행하세요.") from exc


def normalize(name: str) -> str:
    return name.strip().lower().replace("-", "_")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def update_collector_names_py(text: str, old: str, new: str) -> Tuple[str, List[str]]:
    """collector_names.py의 상수 값 중 old 값을 new로 변경."""
    changes: List[str] = []

    pattern = re.compile(r'^(\s*[A-Z0-9_]+\s*=\s*)(["\'])([^"\']+)(["\']\s*)$', re.MULTILINE)

    def repl(match: re.Match[str]) -> str:
        prefix, q1, value, suffix = match.group(1), match.group(2), match.group(3), match.group(4)
        if normalize(value) != normalize(old):
            return match.group(0)
        changes.append(f"collector_names.py: {value} -> {new}")
        return f"{prefix}{q1}{new}{q1}{suffix}"

    new_text = pattern.sub(repl, text)
    return new_text, changes


def update_config_yaml(text: str, old: str, new: str) -> Tuple[str, List[str]]:
    """config.yaml의 collectors 섹션 키를 old -> new로 이동."""
    changes: List[str] = []
    yaml = _load_yaml_module()
    data = yaml.safe_load(text) or {}
    collectors = data.setdefault("collectors", {})

    old_key = old
    if old_key not in collectors and normalize(old) in collectors:
        old_key = normalize(old)

    if old_key in collectors:
        if new in collectors and new != old_key:
            raise ValueError(f"config.yaml 충돌: collectors.{new} 키가 이미 존재합니다.")
        collectors[new] = collectors.pop(old_key)
        changes.append(f"config.yaml: collectors.{old_key} -> collectors.{new}")

    new_text = yaml.safe_dump(data, allow_unicode=True, sort_keys=False)
    return new_text, changes


def update_domains_py(text: str, old: str, new: str) -> Tuple[str, List[str]]:
    """domains.py의 collector_name 값(old -> new)을 문자열 치환으로 변경."""
    changes: List[str] = []

    # collector_name: "old" 형태만 변경
    pattern = re.compile(
        r'(["\']collector_name["\']\s*:\s*["\'])' + re.escape(old) + r'(["\'])'
    )
    count = 0

    def repl(match: re.Match[str]) -> str:
        nonlocal count
        count += 1
        return f"{match.group(1)}{new}{match.group(2)}"

    new_text = pattern.sub(repl, text)
    if count:
        changes.append(f"domains.py: collector_name '{old}' -> '{new}' ({count}곳)")
    return new_text, changes


def main() -> int:
    parser = argparse.ArgumentParser(description="수집기 canonical 이름 변경 자동화")
    parser.add_argument("old_name", help="기존 canonical 이름")
    parser.add_argument("new_name", help="새 canonical 이름")
    parser.add_argument("--apply", action="store_true", help="실제 파일 변경 적용")
    args = parser.parse_args()

    old_name = normalize(args.old_name)
    new_name = normalize(args.new_name)

    if not old_name or not new_name:
        print("❌ 이름은 비어 있을 수 없습니다.")
        return 1
    if old_name == new_name:
        print("⚠️ old_name과 new_name이 동일합니다. 변경할 내용이 없습니다.")
        return 0

    files = {
        COLLECTOR_NAMES_FILE: read_text(COLLECTOR_NAMES_FILE),
        CONFIG_FILE: read_text(CONFIG_FILE),
        DOMAINS_FILE: read_text(DOMAINS_FILE),
    }

    updated: Dict[Path, str] = dict(files)
    all_changes: List[str] = []

    updated[COLLECTOR_NAMES_FILE], changes = update_collector_names_py(updated[COLLECTOR_NAMES_FILE], old_name, new_name)
    all_changes.extend(changes)

    updated[CONFIG_FILE], changes = update_config_yaml(updated[CONFIG_FILE], old_name, new_name)
    all_changes.extend(changes)

    updated[DOMAINS_FILE], changes = update_domains_py(updated[DOMAINS_FILE], old_name, new_name)
    all_changes.extend(changes)

    if not all_changes:
        print("변경 대상이 없습니다.")
        return 0

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"\n=== rename_collector ({mode}) ===")
    print(f"old: {old_name}")
    print(f"new: {new_name}")
    print("변경 예정:")
    for line in all_changes:
        print(f"- {line}")

    if not args.apply:
        print("\nℹ️ 실제 반영하려면 --apply를 사용하세요.")
        return 0

    for path, text in updated.items():
        if text != files[path]:
            write_text(path, text)

    print("\n✅ 변경 적용 완료")
    return 0


if __name__ == "__main__":
    sys.exit(main())
