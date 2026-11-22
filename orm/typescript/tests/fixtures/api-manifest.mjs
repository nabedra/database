import { readFileSync } from "node:fs"
import { resolve } from "node:path"
import ts from "typescript"

const [, , directory, modules, exclusions] = process.argv
const { exports } = JSON.parse(readFileSync("package.json", "utf8"))
const exclude = new Set(JSON.parse(exclusions))
const entryPoints = JSON.parse(modules).map(suffix => {
  const path = suffix ? "./" + suffix.replaceAll(".", "/") : "."
  return ["nabedra" + (suffix ? "." + suffix : ""), resolve(directory, exports[path].types.replace(/^\.\/dist\//, ""))]
})

const program = ts.createProgram(entryPoints.map(([, path]) => path), {
  module: ts.ModuleKind.NodeNext,
  types: [],
})
const checker = program.getTypeChecker()
const entries = []
const text = node => node?.getText() ?? null
const has = (node, flag) => (ts.getCombinedModifierFlags(node) & flag) !== 0
const names = new Map([
  ["[Symbol.asyncIterator]", "iterator"],
  ["[Symbol.dispose]", "dispose"],
  ["[Symbol.asyncDispose]", "dispose"],
])

function add(module, symbol, kind, node, returns = text(node.type), metadata = {}) {
  const params = (node.parameters ?? []).map(param => ({
    name: text(param.name),
    kind: param.dotDotDotToken ? "rest" : ts.isIdentifier(param.name) ? "positional" : "destructured",
    type: text(param.type),
    ...(param.questionToken ? { optional: true } : {}),
  }))
  const typeParams = node.typeParameters?.map(text)
  entries.push({ module, symbol, kind, params, returns, ...(typeParams ? { typeParams } : {}), ...metadata })
}

function addMember(module, owner, node) {
  if (has(node, ts.ModifierFlags.Private | ts.ModifierFlags.Protected) ||
      node.name?.kind === ts.SyntaxKind.PrivateIdentifier) return
  if (ts.isConstructorDeclaration(node)) {
    add(module, owner + ".constructor", "constructor", node)
    return
  }
  const name = text(node.name)
  const symbol = owner + "." + (names.get(name) ?? name ??
    (ts.isIndexSignatureDeclaration(node) ? "[index]" : "[call]"))
  const native = names.has(name) ? { nativeSymbol: owner + "." + name } : {}
  const isStatic = has(node, ts.ModifierFlags.Static)
  if (ts.isGetAccessorDeclaration(node) || ts.isSetAccessorDeclaration(node)) {
    add(module, symbol, ts.isGetAccessorDeclaration(node) ? "property" : "setter", node, text(node.type), native)
  } else if (ts.isMethodDeclaration(node) || ts.isMethodSignature(node) || !name) {
    add(module, symbol, isStatic ? "staticMethod" : "method", node, text(node.type),
      { ...native, ...(node.questionToken ? { optional: true } : {}) })
  } else {
    add(module, symbol, isStatic ? "staticProperty" : "property", node, text(node.type),
      { readonly: has(node, ts.ModifierFlags.Readonly),
        ...(node.questionToken ? { optional: true } : {}),
        ...(node.initializer ? { value: text(node.initializer) } : {}) })
  }
}

for (const [module, path] of entryPoints) {
  const source = program.getSourceFile(path)
  if (!source) throw new Error(`Missing declaration for ${module}: ${path}`)
  for (const exported of checker.getExportsOfModule(checker.getSymbolAtLocation(source))) {
    const name = exported.name
    if (module === "nabedra" && exclude.has(name)) continue
    const symbol = exported.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(exported) : exported
    if (!symbol.declarations?.length) throw new Error(`Unresolved export ${module}:${name}`)
    for (const node of symbol.declarations) {
      if (ts.isClassDeclaration(node) || ts.isInterfaceDeclaration(node)) {
        const kind = ts.isClassDeclaration(node) ? "class" : "interface"
        add(module, name, kind, node, null, { bases: (node.heritageClauses ?? []).map(text) })
        for (const member of node.members) addMember(module, name, member)
      } else if (ts.isFunctionDeclaration(node) || ts.isTypeAliasDeclaration(node)) {
        add(module, name, ts.isFunctionDeclaration(node) ? "function" : "type", node)
      } else if (ts.isVariableDeclaration(node)) {
        add(module, name, "constant", node, text(node.type), node.initializer ? { value: text(node.initializer) } : {})
      } else {
        throw new Error(`Unsupported export ${module}:${name} (${ts.SyntaxKind[node.kind]})`)
      }
    }
  }
}
process.stdout.write(JSON.stringify(entries))
