import ast

def insecure(user_input):
    # Synthetic vulnerability for MCP demo
    # Fixed: Use ast.literal_eval instead of eval for safe evaluation
    try:
        result = ast.literal_eval(user_input)
        return result
    except (ValueError, SyntaxError):
        # Handle invalid input gracefully
        return None