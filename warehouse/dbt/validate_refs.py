#!/usr/bin/env python3
"""Lightweight dbt graph validator.

Checks (without needing dbt installed) that every {{ ref('x') }} resolves to a
model file in the project and every {{ source('s','t') }} is declared in a
sources yml. Also detects cycles in the ref graph. This is a fast sanity gate
for the StreamSense warehouse models.
"""
import os
import re
import sys

MODELS_DIR = os.path.join(os.path.dirname(__file__), "models")

ref_re = re.compile(r"{{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*}}")
source_re = re.compile(r"{{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*}}")


def find_models():
    models = {}
    for root, _, files in os.walk(MODELS_DIR):
        for f in files:
            if f.endswith(".sql"):
                name = f[:-4]
                models[name] = os.path.join(root, f)
    return models


def find_sources():
    """Parse sources.yml files for declared name/table pairs (no yaml dep)."""
    declared = set()
    for root, _, files in os.walk(MODELS_DIR):
        for f in files:
            if not f.endswith((".yml", ".yaml")):
                continue
            text = open(os.path.join(root, f)).read()
            # crude block parse: track current source name and table entries
            current_source = None
            for line in text.splitlines():
                m = re.match(r"\s*-\s*name:\s*(\S+)", line)
                indent = len(line) - len(line.lstrip())
                if m:
                    # heuristic: source names are less indented than tables
                    if indent <= 4:
                        current_source = m.group(1)
                    elif current_source:
                        declared.add((current_source, m.group(1)))
    return declared


def build_dep_graph(models):
    graph = {}
    for name, path in models.items():
        text = open(path).read()
        graph[name] = set(ref_re.findall(text))
    return graph


def detect_cycle(graph):
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {n: WHITE for n in graph}

    def visit(n, stack):
        color[n] = GRAY
        for dep in graph.get(n, ()):
            if dep not in graph:
                continue
            if color[dep] == GRAY:
                return stack + [dep]
            if color[dep] == WHITE:
                r = visit(dep, stack + [dep])
                if r:
                    return r
        color[n] = BLACK
        return None

    for n in graph:
        if color[n] == WHITE:
            r = visit(n, [n])
            if r:
                return r
    return None


def main():
    models = find_models()
    sources = find_sources()
    graph = build_dep_graph(models)

    errors = []

    for name, path in models.items():
        text = open(path).read()
        for ref in ref_re.findall(text):
            if ref not in models:
                errors.append(f"{name}: ref('{ref}') does not resolve to a model")
        for s, t in source_re.findall(text):
            if (s, t) not in sources:
                errors.append(f"{name}: source('{s}','{t}') not declared in any sources.yml")

    cycle = detect_cycle(graph)
    if cycle:
        errors.append("dependency cycle detected: " + " -> ".join(cycle))

    print(f"Models found      : {len(models)} ({', '.join(sorted(models))})")
    print(f"Sources declared  : {len(sources)} ({', '.join(sorted('.'.join(s) for s in sources))})")
    print(f"Ref edges         : {sum(len(v) for v in graph.values())}")

    if errors:
        print("\n❌ Validation failed:")
        for e in errors:
            print("  -", e)
        sys.exit(1)

    print("\n✅ All ref() and source() references resolve; no cycles.")


if __name__ == "__main__":
    main()
