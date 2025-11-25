import ast

def insecure(user_input):
    """
    Process user input safely without using eval().
    
    Args:
        user_input (str): User-provided input to process
    
    Returns:
        The result of processing the input safely
    """
    try:
        # Parse the expression safely using ast.literal_eval
        # This only allows literal expressions (numbers, strings, lists, dicts, etc.)
        result = ast.literal_eval(user_input)
        return result
    except (ValueError, SyntaxError):
        # If the input is not a valid literal, return it as a string
        # or handle according to your application's needs
        return user_input
