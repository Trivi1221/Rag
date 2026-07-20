import os
import re
import typing
from typing import Any, TextIO

from yaml import SafeLoader

_env_replace_matcher = re.compile(r"\$\{(\w|_)+:?.*}")


@typing.no_type_check  # pyaml does not have good hints, everything is Any
def load_yaml_with_envvars(
    stream: TextIO, environ: dict[str, Any] = os.environ
) -> dict[str, Any]:
    """Load yaml file with environment variable expansion.

    The pattern ${VAR} or ${VAR:default} will be replaced with
    the value of the environment variable.
    """
    loader = SafeLoader(stream)

    def resolve_env_expression(expression: str, recursion_depth: int = 0) -> str:
        if recursion_depth > 10:
            raise ValueError(
                f"Environment variable expression recursion limit reached: {expression}"
            )

        if not expression.startswith("${") or not expression.endswith("}"):
            return expression

        value = expression.removeprefix("${").removesuffix("}")
        split = value.split(":", 1)
        env_vars = split[0].strip()
        env_value: str | None = None
        env_var = ""
        for env_var in env_vars.split(","):
            env_var = env_var.strip()
            env_value = environ.get(env_var) or env_value
            if env_value is not None:
                break

        resolved_value = env_value
        default = None if len(split) == 1 else split[1]

        if resolved_value is None and default is None:
            raise ValueError(
                f"Environment variable {env_var} is not set and not default was provided"
            )

        result = resolved_value or default
        if isinstance(result, str) and result.startswith("${") and result.endswith("}"):
            return resolve_env_expression(result, recursion_depth + 1)
        return result

    def load_env_var(_, node) -> str:
        """Extract the matched value, expand env variable, and replace the match."""
        return resolve_env_expression(str(node.value))

    loader.add_implicit_resolver("env_var_replacer", _env_replace_matcher, None)
    loader.add_constructor("env_var_replacer", load_env_var)

    try:
        return loader.get_single_data()
    finally:
        loader.dispose()
