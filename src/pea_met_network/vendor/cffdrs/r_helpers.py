# R helper functions (vendored from github.com/cffdrs/cffdrs_py)


def safe_div(num, den, default=0.0):
    """Safe division that returns default when denominator is zero."""
    if den == 0:
        return default
    return num / den
