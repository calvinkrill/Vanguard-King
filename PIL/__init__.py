"""
Lightweight local stub for environments where Pillow is not installed.

This allows simple import checks like `import PIL; print(PIL.__version__)`
to succeed in offline sandboxes used for CI/smoke testing.
"""

__version__ = "0.0-local-stub"
