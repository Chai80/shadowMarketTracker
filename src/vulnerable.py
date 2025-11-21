import ast
import operator

# Supported operators for safe evaluation
ALLOWED_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}


def safe_eval(node):
    """Safely evaluate a simple arithmetic expression."""
    if isinstance(node, ast.Constant):  # Python 3.8+
        return node.value
    elif isinstance(node, ast.Num):  # Python < 3.8 fallback
        return node.n
    elif isinstance(node, ast.BinOp) and type(node.op) in ALLOWED_OPERATORS:
        left = safe_eval(node.left)
        right = safe_eval(node.right)
        return ALLOWED_OPERATORS[type(node.op)](left, right)
    elif isinstance(node, ast.UnaryOp) and type(node.op) in ALLOWED_OPERATORS:
        operand = safe_eval(node.operand)
        return ALLOWED_OPERATORS[type(node.op)](operand)
    else:
        raise ValueError("Unsafe expression")


def insecure(user_input):
    # Synthetic vulnerability for MCP demo
    # Replaced eval() with safe expression evaluation
    try:
        # Parse the expression into an AST
        tree = ast.parse(user_input.strip(), mode='eval')
        # Safely evaluate only simple arithmetic
        result = safe_eval(tree.body)
        return result
    except (SyntaxError, ValueError, TypeError):
        raise ValueError("Invalid or unsafe expression")