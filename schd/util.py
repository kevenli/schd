import distutils

def ensure_bool(s):
    if s is None:
        return s
    
    if isinstance(s, bool):
        return s
    
    return bool(distutils.util.strtobool(s))
