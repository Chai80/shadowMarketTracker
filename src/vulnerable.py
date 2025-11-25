import ast

def secure(user_input):
    # Secure replacement for eval() using ast.literal_eval()
    # Only evaluates literals (strings, numbers, tuples, lists, dicts, booleans, None)
    # which prevents code execution attacks
    try:
        result = ast.literal_eval(user_input)
        return result
    except (ValueError, SyntaxError):
        # Return None or appropriate default for invalid input
        return None

# Deprecated function - kept for backward compatibility but marked as insecure
# DO NOT USE - will be removed in future versions
def insecure(user_input):
    """DEPRECATED: This function is insecure and should not be used."""
    raise RuntimeError(
        "The 'insecure' function has been disabled due to security vulnerabilities. "
        "Use 'secure' instead, or refactor your code to avoid dynamic evaluation."
    )