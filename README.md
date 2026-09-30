# ip-motion-sticker

Core infrastructure for a reproducible motion-sticker pipeline. This first
increment deliberately contains no image processing or APNG encoding. It
establishes the contracts those stages will rely on:

- strict, versioned manifest and checkpoint validation;
- deterministic SHA-256 hashing for files, bytes, and JSON values;
- atomic, integrity-checked checkpoint persistence; and
- an explicit pipeline state machine with guarded transitions.

## Development

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```

The library uses only the Python standard library at runtime.
