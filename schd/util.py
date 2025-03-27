from typing import Union

def ensure_bool(s: Union[bool, int, float, str]) -> bool:
    if isinstance(s, bool):
        return s
    if isinstance(s, (int, float)):
        return bool(s)
    if isinstance(s, str):
        truthy_values = {"true", "1", "yes", "on"}
        falsy_values = {"false", "0", "no", "off"}
        lower_s = s.strip().lower()
        if lower_s in truthy_values:
            return True
        if lower_s in falsy_values:
            return False
        raise ValueError(f"Cannot convert string '{s}' to bool")
    raise TypeError(f"Unsupported type: {type(s)}")
