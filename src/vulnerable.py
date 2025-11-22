import ast


def secure(user_input):
    # Secure implementation using literal_eval for safe evaluation
    try:
        # Use ast.literal_eval for safe evaluation of literals only
        result = ast.literal_eval(user_input.strip())
        return result
    except (ValueError, SyntaxError):
        # Return None or appropriate default for invalid input
        return None


def insecure(user_input):
    # Synthetic vulnerability for MCP demo - DO NOT USE IN PRODUCTION
    # This function demonstrates unsafe use of eval() which can execute arbitrary code
    eval(user_input)