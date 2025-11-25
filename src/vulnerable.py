import ast

def insecure(user_input):
    # Safely evaluate literal expressions only
    # Removed eval() which allows arbitrary code execution
    try:
        # Only allow literal values (numbers, strings, lists, dicts, etc.)
        result = ast.literal_eval(user_input)
        return result
    except (ValueError, SyntaxError):
        # Return None for invalid inputs instead of executing code
        return None