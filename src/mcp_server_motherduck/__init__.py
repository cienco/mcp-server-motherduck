try:
    from ._version import __version__
except Exception:
    __version__ = "0.0.0"

# Non importare build_application qui; l'HTTP adapter usa app_http:app
# Se proprio serve compatibilità:
try:
    from .server import build_application  # noqa: F401
except Exception:
    def build_application():
        return None
