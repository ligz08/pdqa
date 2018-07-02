def make_log_msg(title, status, detail=None, extra=None, sep='::'):
    """
    Make a message that looks like this:
    Title::status::detail::extra
    Any None will be skipped.
    """
    strings = [title, status, detail, extra]
    return sep.join(filter(None, strings))
