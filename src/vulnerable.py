def insecure(user_input):
    # Synthetic vulnerability for MCP demo
    # Fixed: Use safe evaluation instead of eval()
    import ast
    import operator
    
    # Define allowed operations for safe evaluation
    allowed_ops = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.Pow: operator.pow,
        ast.USub: operator.neg,
        ast.UAdd: operator.pos,
    }
    
    def safe_eval(node):
        if isinstance(node, ast.Constant):  # Numbers
            return node.value
        elif isinstance(node, ast.BinOp) and type(node.op) in allowed_ops:
            left = safe_eval(node.left)
            right = safe_eval(node.right)
            return allowed_ops[type(node.op)](left, right)
        elif isinstance(node, ast.UnaryOp) and type(node.op) in allowed_ops:
            operand = safe_eval(node.operand)
            return allowed_ops[type(node.op)](operand)
        else:
            raise ValueError(f"Unsafe operation: {type(node)}")
    
    try:
        # Parse and evaluate safely
        tree = ast.parse(user_input, mode='eval')
        result = safe_eval(tree.body)
        return result
    except (SyntaxError, ValueError, TypeError):
        raise ValueError("Invalid or unsafe expression")
