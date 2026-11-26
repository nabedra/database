// Messages that name a language's own types, rewritten so the Python and TypeScript SDK messages can be compared.
const wording = [
  [/exact (?:int|str|bigint|string) values/, "exact T values"],
  [/requires a float32 vector/, "float32 vector"],
  [/requires finite float32 values/, "float32 vector"],
  [/Row must be a mapping|Rows must be iterable/, "rows"],
  [/Primary key must name a (?:str or int|string or bigint) column/, "primary key type"],
  // An unknown option name is a TypeError in both. Python's interpreter words it, and 3.13 and later
  // add a suggestion, so the class and the option name are compared.
  [/^\S+\(\) got an unexpected keyword argument '([^']*)'(?:\. Did you mean '[^']*'\?)?$|^Unknown option '([^']*)'$/,
    "unknown option '$1$2'"],
]

export const normalize = (text) => wording.reduce((result, [pattern, replacement]) => result.replace(pattern, replacement), text)
