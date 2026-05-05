import os
from datetime import date, timedelta

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


def _format_prompt_date(value: date | None) -> str:
    return value.strftime("%d/%m/%Y") if value else "*"


def prompt_optional_date_range(
    prompt_text: str,
    default_start: date | None = None,
    default_end: date | None = None,
    available_start: date | None = None,
    available_end: date | None = None,
) -> tuple[date | None, date | None]:
    default_hint = ""
    if default_start or default_end:
        start_hint = _format_prompt_date(default_start)
        end_hint = _format_prompt_date(default_end)
        default_hint = f" (Press Enter for default {start_hint} to {end_hint})"

    available_hint = ""
    example_hint = ""
    if available_start or available_end:
        available_start_hint = _format_prompt_date(available_start)
        available_end_hint = _format_prompt_date(available_end)
        available_days = None
        if available_start and available_end:
            available_days = (available_end - available_start).days + 1
        available_days_hint = (
            f" ({available_days} days)" if available_days is not None else ""
        )
        available_hint = (
            f"\n    Available: {available_start_hint} - {available_end_hint}"
            f"{available_days_hint}."
        )

        if available_start:
            suggested_start = available_start
            suggested_end = suggested_start + timedelta(days=29)
            if available_end and suggested_end > available_end:
                suggested_end = available_end
            suggested_days = (suggested_end - suggested_start).days + 1
            example_hint = (
                "\n    Example (recommended max 30 days): "
                f"{_format_prompt_date(suggested_start)}"
                f" - {_format_prompt_date(suggested_end)}"
                f" ({suggested_days} days)"
            )

    answer = input(
        f"{prompt_text} (format: DD/MM/YYYY - DD/MM/YYYY, blank for all discovered days)"
        f"{available_hint}{example_hint}{default_hint}: "
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
