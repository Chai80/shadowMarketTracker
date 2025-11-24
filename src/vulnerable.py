import ast
import operator

# Whitelist of allowed operations for safe evaluation
ALLOWED_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}


def safe_eval(node_or_string):
    """
    Safely evaluate a mathematical expression.
    Only allows basic arithmetic operations and literals.
    """
    if isinstance(node_or_string, str):
        node = ast.parse(node_or_string, mode='eval')
    else:
        node = node_or_string
    
    def _eval(node):
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        elif isinstance(node, ast.Constant):  # Python 3.8+
            return node.value
        elif isinstance(node, ast.Num):  # Python < 3.8 compatibility
            return node.n
        elif isinstance(node, ast.BinOp) and type(node.op) in ALLOWED_OPERATORS:
            left = _eval(node.left)
            right = _eval(node.right)
            return ALLOWED_OPERATORS[type(node.op)](left, right)
        elif isinstance(node, ast.UnaryOp) and type(node.op) in ALLOWED_OPERATORS:
            operand = _eval(node.operand)
            return ALLOWED_OPERATORS[type(node.op)](operand)
        else:
            raise ValueError(f"Unsafe operation: {type(node).__name__}")
    
    return _eval(node)


def insecure(user_input):
    """
    Process user input safely without using eval().
    """
    try:
        # Use safe_eval instead of eval to prevent code injection
        result = safe_eval(user_input.strip())
        return result
    except (SyntaxError, ValueError, TypeError) as e:
        raise ValueError(f"Invalid mathematical expression: {e}")
