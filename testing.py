
import os, re
imports = set()
for root, dirs, files in os.walk('backend'):
    dirs[:] = [d for d in dirs if d != '__pycache__']
    for f in files:
        if f.endswith('.py'):
            with open(os.path.join(root, f), encoding='utf-8', errors='ignore') as fh:
                for line in fh:
                    line = line.strip()
                    if line.startswith('import ') or line.startswith('from '):
                        imports.add(line)
for i in sorted(imports): print(i)