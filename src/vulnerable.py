import ast


def secure(user_input):
    # Safe evaluation using ast.literal_eval for literals only
    try:
        return ast.literal_eval(user_input)
    except (ValueError, SyntaxError):
        raise ValueError("Invalid input format")


def insecure(user_input):
    # Synthetic vulnerability for MCP demo - DEPRECATED
    # This function should not be used in production
    # Use secure() instead
    raise NotImplementedError("This function is deprecated due to security vulnerability. Use secure() instead.")
