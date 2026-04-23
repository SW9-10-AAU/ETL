import os
from datetime import date

YES_VALUES = {"y", "yes", "1", "true"}
NO_VALUES = {"n", "no", "0", "false"}


def parse_env_bool(var_name: str) -> bool | None:
    value = os.getenv(var_name)
    if value is None:
        return None

    normalized = value.strip().lower()
    if normalized in YES_VALUES:
        return True
    if normalized in NO_VALUES:
        return False

    raise ValueError(
        f"Invalid value for {var_name}: '{value}'. Use one of: y/yes/1/true or n/no/0/false."
    )


def _parse_prompt_answer(prompt_text: str, default_yes: bool) -> bool:
    prompt_suffix = "[Y/n]" if default_yes else "[y/N]"
    answer = input(f"{prompt_text} {prompt_suffix}: ").strip().lower()
    if answer == "":
        return default_yes
    if answer in YES_VALUES:
        return True
    if answer in NO_VALUES:
        return False
    return default_yes


def should_run_step(env_var: str, prompt_text: str, default_yes: bool = True) -> bool:
    override = parse_env_bool(env_var)
    if override is not None:
        return override

    return _parse_prompt_answer(prompt_text, default_yes)


def should_run_step_with_fallback(
    env_var: str,
    fallback_env_var: str,
    prompt_text: str,
    default_yes: bool = True,
) -> bool:
    override = parse_env_bool(env_var)
    if override is not None:
        return override

    fallback = parse_env_bool(fallback_env_var)
    if fallback is not None:
        return fallback

    return _parse_prompt_answer(prompt_text, default_yes)


def parse_prompt_date(value: str) -> date:
    normalized = value.strip()
    for sep in ("-", "/"):
        parts = normalized.split(sep)
        if len(parts) != 3:
            continue

        first, second, third = [part.strip() for part in parts]
        if len(first) == 4:
            return date(int(first), int(second), int(third))
        if len(third) == 4:
            return date(int(third), int(second), int(first))

    raise ValueError(
        "Invalid date format. Use YYYY-MM-DD, YYYY/MM/DD, DD-MM-YYYY, or DD/MM/YYYY."
    )


def prompt_optional_date_range(
    prompt_text: str,
    default_start: date | None = None,
    default_end: date | None = None,
) -> tuple[date | None, date | None]:
    default_hint = ""
    if default_start or default_end:
        start_hint = default_start.isoformat() if default_start else "*"
        end_hint = default_end.isoformat() if default_end else "*"
        default_hint = f" (Press Enter for default {start_hint} to {end_hint})"

    answer = input(
        f"{prompt_text} (format: DD/MM/YYYY - DD/MM/YYYY, blank for all discovered days){default_hint}: "
    ).strip()

    if not answer:
        return default_start, default_end

    separators = [" - ", " to ", ","]
    start_raw = ""
    end_raw = ""
    for separator in separators:
        if separator in answer:
            start_raw, end_raw = [chunk.strip() for chunk in answer.split(separator, 1)]
            break

    if not start_raw or not end_raw:
        raise ValueError(
            "Invalid date range. Use two dates separated by ' - ' (example: 01/12/2025 - 05/03/2026)."
        )

    start = parse_prompt_date(start_raw)
    end = parse_prompt_date(end_raw)
    if start > end:
        raise ValueError("Start date cannot be after end date.")

    return start, end
