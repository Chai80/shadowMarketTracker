import ast


def insecure(user_input):
    # Synthetic vulnerability for MCP demo
    # Replaced eval() with ast.literal_eval() for safe evaluation of literals only
    try:
        # Only evaluates literals (strings, numbers, tuples, lists, dicts, booleans, None)
        # This prevents execution of arbitrary code
        return ast.literal_eval(user_input)
    except (ValueError, SyntaxError):
        # Return the original input if it's not a valid literal
        # In a real application, you might want to handle this differently
        return user_input
