def _reload():
    """
    Reload modules during development. Note that this
    will not reload any changes made to this function
    or anywhere else in this __init__.py file.
    Add modules to reload as you create them.
    When you add modules or make any changes in this file, you need
    to run these two lines of code in any script where you are using
    this library. They must run be after you import your library but
    before you do `from ... import ...` statements.
        >>> import importlib
        >>> importlib.reload(ais_disabling)
    """
    import importlib

    import ais_disabling
    from ais_disabling import config, utils, model_utils, figure_utils, threshold_models

    importlib.reload(ais_disabling)
    importlib.reload(utils)
    importlib.reload(config)
    importlib.reload(model_utils)
    importlib.reload(figure_utils)
    importlib.reload(threshold_models)
