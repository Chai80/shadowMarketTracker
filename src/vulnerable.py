import ast

def insecure(user_input):
    # Synthetic vulnerability for MCP demo
    # eval() is dangerous - replaced with safe literal evaluation
    try:
        # Only evaluate literals (strings, numbers, tuples, lists, dicts, booleans, None)
        result = ast.literal_eval(user_input)
        return result
    except (ValueError, SyntaxError):
        # Return None for invalid inputs instead of executing arbitrary code
        return None