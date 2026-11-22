#!/usr/bin/env python3
"""Snapshot the OLTP, OLAP, Cache, Events and streaming ORM APIs."""

import argparse
import ast
import difflib
import importlib
import inspect
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import textwrap
import tomllib
import typing

ROOT = Path(__file__).resolve().parents[2]
MODULES = ("", "cache", "client", "errors", "events", "events.sink",
           "expr", "query", "stream", "table", "validation")
# The root also re-exports management and gateway APIs, outside this snapshot.
ROOT_EXCLUSIONS = {
    "AwsCredentials", "AwsCredentialsOptions", "AwsCredentialsProvider", "Cluster",
    "ClusterOptions", "ClusterPolicy", "ClusterStatus", "ClusterNode", "ClusterIntent",
    "WorkerResources", "ScaleOptions", "AutoscaleOptions", "serve", "ServeOptions",
    "StartNodeRequest", "KubernetesProvider", "KubernetesProviderOptions", "ProviderError",
    "ProviderLaunch", "ProviderNode", "Gateway", "GatewayContext", "HTTPResponse",
    "S3", "GatewayOptions", "GatewayRequest", "GatewayMiddleware", "GatewayContextValues",
    "S3Options", "HeaderPair",
}
PYTHON_NAMES = {
    "__eq__": "eq", "__ne__": "ne", "__lt__": "lt", "__le__": "le",
    "__gt__": "gt", "__ge__": "ge", "__and__": "and", "__or__": "or",
    "__invert__": "not", "__iter__": "iterator", "__next__": "next",
    "__enter__": "enter", "__exit__": "dispose", "__bool__": "bool",
}


def camel_case(name):
    if "_" not in name or name.startswith("_"):
        return name
    words = (name.lower() if name.isupper() else name).split("_")
    return words[0] + "".join(word[:1].upper() + word[1:] for word in words[1:])


def annotation(value):
    if value is inspect.Signature.empty:
        return None
    return value if isinstance(value, str) else inspect.formatannotation(value)


def entry(module, symbol, kind, params=None, returns=None, **metadata):
    return {"module": module, "symbol": symbol, "kind": kind,
            "params": [] if params is None else params, "returns": returns, **metadata}


def native_name(symbol, canonical):
    return {"nativeSymbol": symbol} if symbol != canonical else {}


def python_signatures(module, symbol, kind, function, *, receiver=False, **metadata):
    # Keep runtime defaults as well as overloads, which can specify narrower types.
    overloads = typing.get_overloads(function)
    for overload in [function, *overloads]:
        sig = inspect.signature(overload)
        params = []
        for param in list(sig.parameters.values())[int(receiver):]:
            params.append({
                "name": param.name,
                "kind": param.kind.name.lower(),
                "type": annotation(param.annotation),
                **({"default": repr(param.default)} if param.default is not param.empty else {}),
            })
        yield entry(module, symbol, kind, params, annotation(sig.return_annotation),
                    **({"overload": True} if overload is not function else {}),
                    **({"async": True} if inspect.iscoroutinefunction(overload) else {}),
                    **metadata)


def instance_fields(constructor):
    # Unannotated fields such as error diagnostics are assigned on the instance.
    if not inspect.isfunction(constructor) or constructor.__code__.co_filename == "<string>":
        return {}
    tree = ast.parse(textwrap.dedent(inspect.getsource(constructor)))
    fields = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for target in targets:
            if (isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name)
                    and target.value.id == "self" and not target.attr.startswith("_")):
                fields[target.attr] = ast.unparse(node.annotation) if isinstance(node, ast.AnnAssign) else None
    return fields


def python_class(module, native, cls):
    name = camel_case(native)
    yield entry(module, name, "class", bases=[annotation(base) for base in
                vars(cls).get("__orig_bases__", cls.__bases__) if base is not object],
                **native_name(native, name))
    constructor = cls.__dict__.get("__init__")
    if constructor is not None:
        symbol = name + ".constructor"
        yield from python_signatures(module, symbol, "constructor", constructor,
                                     receiver=True, **native_name(native + ".__init__", symbol))
    assigned = instance_fields(constructor)
    fields = dict(assigned)
    fields.update({key: annotation(value) for key, value in inspect.get_annotations(cls).items()
                   if not key.startswith("_")})
    for member, value in vars(cls).items():
        if member.startswith("_") and member not in PYTHON_NAMES:
            continue
        canonical = PYTHON_NAMES.get(member, camel_case(member))
        if member == "__enter__" and name in {"BatchStream", "StreamSnapshot"}:
            canonical = "open"
        symbol = name + "." + canonical
        spelling = native_name(native + "." + member, symbol)
        if isinstance(value, (classmethod, staticmethod)):
            yield from python_signatures(module, symbol, "staticMethod", value.__func__,
                                         receiver=isinstance(value, classmethod), **spelling)
        elif isinstance(value, property):
            yield entry(module, symbol, "property", returns=annotation(inspect.signature(value.fget).return_annotation),
                        readonly=value.fset is None, **spelling)
            if value.fset is not None:
                yield from python_signatures(module, symbol, "setter", value.fset, receiver=True, **spelling)
        elif inspect.isfunction(value):
            yield from python_signatures(module, symbol, "method", value, receiver=True, **spelling)
        elif member not in fields or member in assigned:
            yield entry(module, symbol, "staticProperty", returns=annotation(type(value)), value=repr(value),
                        **spelling)
    for member, field_type in fields.items():
        symbol = name + "." + camel_case(member)
        value = vars(cls).get(member, inspect.Signature.empty)
        default = {"value": repr(value)} if value is not inspect.Signature.empty and not inspect.isdatadescriptor(value) else {}
        yield entry(module, symbol, "property", returns=field_type,
                    **default, **native_name(native + "." + member, symbol))


def python_symbols(source):
    sys.path.insert(0, str(source))
    try:
        for suffix in MODULES:
            module = importlib.import_module("nabedra" + ("." + suffix if suffix else ""))
            annotations = inspect.get_annotations(module)
            names = getattr(module, "__all__", None)
            if names is None:
                # Include local definitions, annotated type aliases and local constants.
                names = [name for name, value in vars(module).items() if not name.startswith("_") and (
                    getattr(value, "__module__", None) == module.__name__ or name in annotations
                )]
                tree = ast.parse(inspect.getsource(module))
                names.extend(target.id for node in tree.body if isinstance(node, ast.Assign)
                             for target in node.targets if isinstance(target, ast.Name) and not target.id.startswith("_"))
            for symbol in sorted(set(names) - (ROOT_EXCLUSIONS if not suffix else set())):
                name, value = camel_case(symbol), getattr(module, symbol)
                native = native_name(symbol, name)
                if inspect.isclass(value) and value.__name__ == symbol:
                    yield from python_class(module.__name__, symbol, value)
                elif inspect.isfunction(value):
                    yield from python_signatures(module.__name__, name, "function", value, **native)
                elif inspect.isclass(value):
                    yield entry(module.__name__, name, "alias", returns=value.__name__, **native)
                elif isinstance(value, (str, int, float, bool)):
                    yield entry(module.__name__, name, "constant", returns=annotation(type(value)),
                                value=repr(value), **native)
                else:
                    yield entry(module.__name__, name, "type", returns=annotation(value), **native)
    finally:
        sys.path.pop(0)


def typescript_symbols(package):
    tsc = package / "node_modules/typescript/bin/tsc"
    if not tsc.is_file():
        raise ValueError(f"TypeScript compiler missing. Run npm ci in {package}")
    with tempfile.TemporaryDirectory(prefix="nabedra-declarations-") as output:
        subprocess.run(["node", str(tsc), "-p", "tsconfig.json", "--declaration",
                        "--emitDeclarationOnly", "--outDir", output], cwd=package, check=True)
        result = subprocess.check_output(
            ["node", str(package / "tests/fixtures/api-manifest.mjs"), output,
             json.dumps(MODULES), json.dumps(sorted(ROOT_EXCLUSIONS))],
            cwd=package, text=True,
        )
    return json.loads(result)


def collect(root, allowlist):
    orm = root / "orm"
    with (orm / "python/pyproject.toml").open("rb") as file:
        python_version = tomllib.load(file)["project"]["version"]
    typescript_version = json.loads((orm / "typescript/package.json").read_text())["version"]
    if python_version != typescript_version:
        raise ValueError(f"ORM versions differ: Python {python_version}, TypeScript {typescript_version}")
    return {
        "schema": 1,
        "version": python_version,
        "allowlist": allowlist,
        "python": sorted(python_symbols(orm / "python/src"), key=symbol_key),
        "typescript": sorted(typescript_symbols(orm / "typescript"), key=symbol_key),
    }


def symbol_key(item):
    return f"{item['module']}:{item['symbol']}"


def parity(manifest, *, report=False):
    symbols = {language: {symbol_key(item) for item in manifest[language]}
               for language in ("python", "typescript")}
    valid = True
    for language, other in (("python", "typescript"), ("typescript", "python")):
        gaps = symbols[language] - symbols[other]
        allowed = manifest["allowlist"].get(language, {})
        for key in sorted(gaps):
            reason = allowed.get(key)
            if not isinstance(reason, str) or not reason.strip():
                print(f"{language} only: {key} (unallowed)", file=sys.stderr)
                valid = False
            elif report:
                print(f"{language} only: {key} ({reason})")
        for key in sorted(allowed.keys() - gaps):
            print(f"Stale {language} allowlist entry: {key}", file=sys.stderr)
            valid = False
    return valid


def render(manifest):
    # One signature per line gives diffs a stable, readable unit.
    header = {key: value for key, value in manifest.items() if key not in {"python", "typescript"}}
    sections = [json.dumps(header, indent=2, sort_keys=True).removesuffix("\n}")]
    for language in ("python", "typescript"):
        rows = ",\n".join("    " + json.dumps(item, ensure_ascii=False) for item in manifest[language])
        sections.append(f'  "{language}": [\n{rows}\n  ]')
    return ",\n".join(sections) + "\n}\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("generate", "check", "parity"))
    args = parser.parse_args(argv)
    path = ROOT / "orm/api-manifest.json"
    try:
        existing = path.read_text(encoding="utf-8")
        baseline = json.loads(existing)
        manifest = collect(ROOT, baseline["allowlist"])
        matched = parity(manifest, report=args.command == "parity")
        if args.command == "parity":
            return 0 if matched else 1

        generated = render(manifest)
        if args.command == "generate":
            if matched:
                path.write_text(generated, encoding="utf-8")
            return 0 if matched else 1

        if existing != generated:
            sys.stderr.writelines(difflib.unified_diff(
                existing.splitlines(keepends=True), generated.splitlines(keepends=True),
                fromfile="orm/api-manifest.json", tofile="regenerated",
            ))
            return 1
        if matched:
            print(f"API manifest matches. ORM versions: {manifest['version']}.")
        return 0 if matched else 1
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
