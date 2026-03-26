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


def should_run_step(env_var: str, prompt_text: str) -> bool:
    override = parse_env_bool(env_var)
    if override is not None:
        return override

    answer = input(f"{prompt_text} [Y/n]: ").strip().lower()
    if answer in NO_VALUES:
        return False

    return True


def should_run_step_with_fallback(
    env_var: str, fallback_env_var: str, prompt_text: str
) -> bool:
    override = parse_env_bool(env_var)
    if override is not None:
        return override

    fallback = parse_env_bool(fallback_env_var)
    if fallback is not None:
        return fallback

    answer = input(f"{prompt_text} [Y/n]: ").strip().lower()
    if answer in NO_VALUES:
        return False

    return True
