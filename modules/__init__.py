import os

# autoload all files
__all__ = []
for module in os.listdir(os.path.dirname(__file__)):
    if module == '__init__.py' or module[-3:] != '.py':
        continue
    __import__('modules.'+module[:-3], globals(), locals())
    __all__.append(module[:-3])
del module
del os