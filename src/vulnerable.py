def insecure(user_input):
    # Synthetic vulnerability for MCP demo
    # SECURITY WARNING: Using eval() is unsafe. Replaced with safe alternative.
    # This function now safely processes input without code execution.
    return f"Processed: {user_input}"


# If dynamic code execution is truly required, use ast.literal_eval() for safe evaluation of literals:
import ast

def safe_eval(user_input):
    """Safely evaluate literals using ast.literal_eval()."""
    try:
        return ast.literal_eval(user_input)
    except (ValueError, SyntaxError):
        raise ValueError("Input must be a valid Python literal")
