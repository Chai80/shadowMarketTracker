import ast

def insecure(user_input):
    # Synthetic vulnerability for MCP demo
    # Safely evaluate the user input using ast.literal_eval instead of eval
    try:
        # ast.literal_eval only evaluates literals (strings, numbers, tuples, lists, dicts, booleans, None)
        # and is much safer than eval
        result = ast.literal_eval(user_input)
        return result
    except (ValueError, SyntaxError):
        # Return None or handle the error appropriately if the input is not a valid literal
        return None