def secure(user_input):
    # Secure implementation using safe evaluation
    # Only allow safe mathematical expressions
    import ast
    import operator
    
    # Define allowed operators
    allowed_ops = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.Pow: operator.pow,
        ast.USub: operator.neg,
        ast.UAdd: operator.pos,
    }
    
    def eval_node(node):
        if isinstance(node, ast.Constant):  # Numbers
            return node.value
        elif isinstance(node, ast.BinOp) and type(node.op) in allowed_ops:
            left = eval_node(node.left)
            right = eval_node(node.right)
            return allowed_ops[type(node.op)](left, right)
        elif isinstance(node, ast.UnaryOp) and type(node.op) in allowed_ops:
            operand = eval_node(node.operand)
            return allowed_ops[type(node.op)](operand)
        else:
            raise ValueError("Unsafe expression")
    
    try:
        tree = ast.parse(user_input, mode='eval')
        return eval_node(tree.body)
    except (SyntaxError, ValueError, TypeError):
        raise ValueError("Invalid or unsafe expression")