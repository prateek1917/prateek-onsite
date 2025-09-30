from typing import Callable, Dict

_REGISTRY: Dict[str, Callable] = {}

def register(ref: str, fn: Callable):
    if ref in _REGISTRY and _REGISTRY[ref] is not fn:
        raise ValueError(f"Function ref '{ref}' already registered to a different function.")
    _REGISTRY[ref] = fn

def get(ref: str) -> Callable:
    return _REGISTRY[ref]

def has(ref: str) -> bool:
    return ref in _REGISTRY
