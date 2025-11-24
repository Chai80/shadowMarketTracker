import ast
import operator

# Supported operators for safe evaluation
def _safe_eval(node):
    if isinstance(node, ast.Constant):  # Python 3.8+
        return node.value
    elif isinstance(node, ast.BinOp):
        left = _safe_eval(node.left)
        right = _safe_eval(node.right)
        if isinstance(node.op, ast.Add):
            return operator.add(left, right)
        elif isinstance(node.op, ast.Sub):
            return operator.sub(left, right)
        elif isinstance(node.op, ast.Mult):
            return operator.mul(left, right)
        elif isinstance(node.op, ast.Div):
            return operator.truediv(left, right)
        elif isinstance(node.op, ast.Pow):
            return operator.pow(left, right)
    elif isinstance(node, ast.UnaryOp):
        operand = _safe_eval(node.operand)
        if isinstance(node.op, ast.USub):
            return operator.neg(operand)
        elif isinstance(node.op, ast.UAdd):
            return operator.pos(operand)
    raise ValueError("Unsafe expression")

def insecure(user_input):
    """
    Safe evaluation of mathematical expressions.
    Replaces eval() to prevent code injection.
    """
    try:
        # Parse the expression into an AST
        tree = ast.parse(user_input.strip(), mode='eval')
        # Evaluate only safe operations
        return _safe_eval(tree.body)
    except (SyntaxError, ValueError) as e:
        raise ValueError(f"Invalid expression: {e}")
