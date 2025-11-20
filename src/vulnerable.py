import ast


def insecure(user_input):
    """Safely evaluate a literal expression from user input.

    The original implementation used ``eval`` which executes arbitrary
    Python code and is a severe security risk.  ``ast.literal_eval`` only
    parses literals (strings, numbers, tuples, lists, dicts, booleans and
    ``None``) and therefore cannot execute code.  If the input is not a
    valid literal a ``ValueError`` is raised.

    Parameters
    ----------
    user_input : str
        The string supplied by the user.

    Returns
    -------
    Any
        The Python object represented by the literal.

    Raises
    ------
    ValueError
        If the input is not a valid Python literal.
    """
    try:
        return ast.literal_eval(user_input)
    except (ValueError, SyntaxError) as exc:
        raise ValueError("Invalid input for evaluation") from exc
