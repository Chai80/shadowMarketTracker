def insecure(user_input):
    # Synthetic vulnerability for MCP demo
    # Fixed: Replaced eval() with safe expression evaluation
    import ast
    import operator
    
    # Define allowed operations for safe evaluation
    operators = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.Pow: operator.pow,
        ast.USub: operator.neg,
        ast.UAdd: operator.pos,
    }
    
    def eval_expr(expr):
        """
        Safely evaluates a mathematical expression.
        Only allows basic arithmetic operations and numbers.
        """
        try:
            return eval_(ast.parse(expr, mode='eval').body)
        except (ValueError, SyntaxError):
            raise ValueError(f"Invalid expression: {expr}")
    
    def eval_(node):
        if isinstance(node, ast.Constant):  # Python 3.8+
            return node.value
        elif isinstance(node, ast.Num):  # Python < 3.8
            return node.n
        elif isinstance(node, ast.BinOp):
            left = eval_(node.left)
            right = eval_(node.right)
            if type(node.op) in operators:
                return operators[type(node.op)](left, right)
            else:
                raise ValueError(f"Unsupported operation: {type(node.op).__name__}")
        elif isinstance(node, ast.UnaryOp):
            operand = eval_(node.operand)
            if type(node.op) in operators:
                return operators[type(node.op)](operand)
            else:
                raise ValueError(f"Unsupported unary operation: {type(node.op).__name__}")
        else:
            raise ValueError(f"Unsupported expression node: {type(node).__name__}")
    
    # Only evaluate if it's a mathematical expression
    if user_input.strip():
        return eval_expr(user_input)
    else:
        raise ValueError("Empty input")