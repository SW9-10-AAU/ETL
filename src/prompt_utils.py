import os

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
