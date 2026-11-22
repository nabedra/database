"""Replay the Nabedra logic tests (server/tests/logic/*.nlt) through the Python ORM.

The format and the rendering rules are in server/tests/logic/README.md. This
replay must render results exactly as the Rust runner (server/tests/olap_logic.rs)
and the TypeScript replay do. The server-backed test runs against
NABEDRA_LOGIC_URL and skips with "not-run: NABEDRA_LOGIC_URL unset" otherwise.
The parser, renderer and sensitivity tests need no server.
"""
import decimal
import hashlib
import json
import os
import pathlib
import re
import unittest
import uuid

from nabedra import errors
from nabedra.client import _Transport

CORPUS = pathlib.Path(__file__).resolve().parents[3] / 'server' / 'tests' / 'logic'
MISSING_TABLE_ID = 'ffffffff-ffff-4fff-bfff-ffffffffffff'
CONTROL_RE = re.compile(r'[\\\x00-\x1f\x7f]')
UUID_RE = re.compile(r'[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}')
TABLE_NAME_RE = re.compile(r'[A-Za-z_][A-Za-z0-9_]{0,63}')


class FormatError(ValueError):
    pass


def parse(text):
    """Return {'name', 'engines', 'records'}; each record has kind, line, skip, command, expected."""
    lines = text.replace('\r\n', '\n').removesuffix('\n').split('\n')
    if not lines or not lines[0].startswith('# name:'):
        raise FormatError('line 1: the file must start with `# name: <name>`')
    name = lines[0][len('# name:'):].strip()
    engines = ['olap', 'oltp']
    records = []
    skip = None
    i = 1
    while i < len(lines):
        line = lines[i].rstrip()
        if not line or line.startswith('#'):
            i += 1
            continue

        def fail(message, at=i):
            raise FormatError(f'line {at + 1}: {message}')

        if line.startswith('require '):
            words = line[len('require '):].split()
            if records or skip is not None:
                fail('require must come before the first record')
            if not words or any(w not in ('olap', 'oltp') for w in words):
                fail('require takes olap and/or oltp')
            engines = words
            i += 1
            continue
        if line.startswith('skipif'):
            reason = line[len('skipif'):].strip()
            if not reason or skip is not None:
                fail('skipif needs a reason and one record')
            skip = (i, reason)
            i += 1
            continue
        words = line.split()
        if words == ['statement', 'ok']:
            kind = {'type': 'ok'}
        elif words[:2] == ['statement', 'error']:
            pattern = line[len('statement error'):].strip()
            if not pattern:
                fail('statement error needs a code or message substring')
            kind = {'type': 'error', 'pattern': pattern}
        elif words[0] == 'query' and len(words) >= 2:
            if words[1] not in ('rowsort', 'nosort'):
                fail(f'unknown sort mode {words[1]!r}')
            kind = {'type': 'query', 'sort': words[1], 'hash': False, 'label': None}
            for word in words[2:]:
                if word == 'hash' and not kind['hash']:
                    kind['hash'] = True
                elif word.startswith('label-') and kind['label'] is None and len(word) > 6:
                    kind['label'] = word[6:]
                else:
                    fail(f'unknown query option {word!r}')
        elif words == ['explain']:
            kind = {'type': 'explain'}
        else:
            fail(f'unknown directive {line!r}')
        if skip is not None and skip[0] + 1 != i:
            fail('skipif must be on the line right before its record')
        header = i
        i += 1
        start = i
        takes_result = kind['type'] in ('query', 'explain')
        while i < len(lines) and lines[i].strip() and not (takes_result and lines[i] == '----'):
            if lines[i] == '----':
                fail('statements have no ---- block')
            i += 1
        if i == start:
            fail('record has no operation', header)
        command = '\n'.join(lines[start:i])
        expected = None
        if i < len(lines) and lines[i] == '----':
            i += 1
            first = i
            while i < len(lines) and lines[i] != '':
                i += 1
            expected = lines[first:i]
        if expected is None and (kind['type'] == 'explain' or (kind['type'] == 'query' and kind['label'] is None)):
            fail('query needs a ---- block unless it has a label', header)
        if kind.get('hash') and expected is not None and len(expected) != 1:
            fail('hash expects one `N values hashing to <hex>` line', header)
        records.append({'kind': kind, 'line': header + 1, 'skip': skip[1] if skip else None,
                        'command': command, 'expected': expected})
        skip = None
    if skip is not None:
        raise FormatError('skipif at the end of the file has no record')
    return {'name': name, 'engines': engines, 'records': records}


def render_string(value):
    if value == '':
        return '(empty)'
    named = {'\\': '\\\\', '\t': '\\t', '\n': '\\n', '\r': '\\r'}
    return CONTROL_RE.sub(lambda m: named.get(m.group(), '\\x%02x' % ord(m.group())), value)


def format_float(sign, digits, exp):
    digits = digits.rstrip('0') or '0'
    if -7 < exp < 21:
        if exp >= 0:
            int_len = exp + 1
            if len(digits) > int_len:
                whole, frac = digits[:int_len], digits[int_len:]
            else:
                whole, frac = digits.ljust(int_len, '0'), '0'
            return f'{sign}{whole}.{frac}'
        return f'{sign}0.{"0" * (-exp - 1)}{digits}'
    rest = '.' + digits[1:] if len(digits) > 1 else ''
    return f'{sign}{digits[0]}{rest}e{exp}'


def canonical_float(value):
    if value == 0:
        return '0.0'
    sign = '-' if value < 0 else ''
    # repr gives the shortest round-trip digits; Decimal exposes them exactly.
    t = decimal.Decimal(repr(abs(value))).as_tuple()
    digits = ''.join(map(str, t.digits))
    return format_float(sign, digits, len(digits) + t.exponent - 1)


def render_value(value):
    if value is None:
        return 'NULL'
    if value is True:
        return 'true'
    if value is False:
        return 'false'
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return canonical_float(value)
    if isinstance(value, str):
        return render_string(value)
    raise FormatError(f'cannot render {value!r}')


def utf8_key(line):
    return line.encode('utf-8')


def ordered(rows, sort):
    return sorted(rows, key=lambda r: utf8_key('\t'.join(r))) if sort == 'rowsort' else list(rows)


def hash_line(rows, sort):
    values = [v for row in ordered(rows, sort) for v in row]
    digest = hashlib.sha256(''.join(v + '\n' for v in values).encode('utf-8')).hexdigest()
    return f'{len(values)} values hashing to {digest}'


class Translator:
    def __init__(self, prefix):
        self.prefix = prefix
        self.tables = {}
        self.long_names = {}

    def translate(self, command):
        try:
            payload = json.loads(command)
        except ValueError as error:
            raise FormatError(f'invalid operation JSON: {error}') from None
        if not isinstance(payload, dict):
            raise FormatError('operation must be a JSON object')
        op = payload.pop('op', None)
        if not isinstance(op, str):
            raise FormatError('operation needs a string `op`')
        table = payload.pop('table', None)
        if isinstance(table, str):
            payload['table_id'] = self.tables.get(table, MISSING_TABLE_ID)
        if op in ('table.create', 'table.describe') and isinstance(payload.get('name'), str):
            payload['name'] = self.physical(payload['name'])
        if op.startswith('query.') and 'query' not in payload:
            payload['query'] = {k: payload.pop(k, None) for k in ('select', 'filter', 'order_by', 'limit')}
        return op, payload

    def observe(self, op, data):
        if op == 'table.create' and isinstance(data, dict):
            name, table_id = data.get('name'), data.get('id')
            if isinstance(name, str) and isinstance(table_id, str):
                self.tables[self.logical(name)] = table_id

    def physical(self, name):
        # Preserve invalid identifiers so the request tests server validation.
        if not self.prefix or not TABLE_NAME_RE.fullmatch(name):
            return name
        if len(self.prefix) + len(name) <= 64:
            return self.prefix + name
        digest = hashlib.sha256(name.encode('ascii')).hexdigest()[:16]
        physical = self.prefix + name[:64 - len(self.prefix) - len(digest)] + digest
        self.long_names[physical] = name
        return physical

    def logical(self, name):
        return self.long_names.get(name, name.removeprefix(self.prefix))

    def descriptor_row(self, d):
        schema = ','.join(f'{c}:{t}' for c, t in sorted(d['schema'].items(), key=lambda kv: utf8_key(kv[0])))
        return [render_string(self.logical(d['name'])), d['key'], schema]

    def render(self, op, payload, data):
        def row(obj):
            if not isinstance(obj, dict):
                raise FormatError('result row is not an object')
            select = payload.get('query', {}).get('select') if op == 'query.collect' else None
            columns = select if isinstance(select, list) else sorted(obj, key=utf8_key)
            if len(columns) != len(obj) or any(c not in obj for c in columns):
                raise FormatError(f'wrong result columns: expected {columns!r}, got {obj!r}')
            return [render_value(obj[c]) for c in columns]

        if op in ('table.create', 'table.describe'):
            return [self.descriptor_row(data)]
        if op == 'table.list':
            rows = [self.descriptor_row(d) for d in data if d['name'].startswith(self.prefix)]
            return sorted(rows, key=lambda r: utf8_key(r[0]))
        if op == 'query.collect':
            return [row(r) for r in data]
        if op == 'table.get':
            return [] if data is None else [row(data)]
        if op == 'query.explain':
            text = re.sub(r'^scan (\S+) (?=\()', lambda m: f'scan {self.logical(m[1])} ', data)
            text = UUID_RE.sub('<uuid>', text)
            return [[render_string(line)] for line in text.split('\n')]
        if data is None:
            return []
        return [[render_value(data)]]


class Failure(AssertionError):
    pass


def run_file(path, parsed, execute, prefix):
    """execute(op, payload) returns data or raises errors.NabedraError. Returns (assertions, skipped, failures)."""
    tr = Translator(prefix)
    labels = {}
    assertions, skipped, failures = 0, [], []
    for record in parsed['records']:
        at = f'{path}:{record["line"]}'
        if record['skip'] is not None:
            skipped.append(f'{at} skipif {record["skip"]}')
            continue
        op, payload = tr.translate(record['command'])
        try:
            data, error = execute(op, payload), None
        except errors.NabedraError as caught:
            data, error = None, caught
        if error is None:
            tr.observe(op, data)
        assertions += 1
        kind = record['kind']
        if kind['type'] == 'ok':
            if error is not None:
                failures.append(f'{at}: expected success\n  actual error: {error.code}: {error}')
            continue
        if kind['type'] == 'error':
            pattern = kind['pattern']
            if error is None:
                failures.append(f'{at}: expected error {pattern!r}\n  actual: success {data!r}')
            elif error.code != pattern and pattern not in str(error):
                failures.append(f'{at}: expected error {pattern!r}\n  actual error: {error.code}: {error}')
            continue
        if error is not None:
            failures.append(f'{at}: expected success\n  actual error: {error.code}: {error}')
            continue
        try:
            rows = tr.render(op, payload, data)
        except FormatError as error:
            failures.append(f'{at}: {error}')
            continue
        sort = kind.get('sort', 'nosort')
        actual = [hash_line(rows, sort)] if kind.get('hash') else ['\t'.join(r) for r in ordered(rows, sort)]
        label = kind.get('label')
        if label is not None:
            if label not in labels:
                labels[label] = (record['line'], actual)
            elif labels[label][1] != actual:
                failures.append(f'{at}: label-{label} disagrees with line {labels[label][0]}\n'
                                f'  expected: {labels[label][1]!r}\n  actual: {actual!r}')
                continue
        expected = record['expected']
        if expected is not None and sort == 'rowsort' and not kind.get('hash'):
            expected = sorted(expected, key=utf8_key)
        if expected is not None and expected != actual:
            failures.append(f'{at}: wrong result\n  expected: {expected!r}\n  actual: {actual!r}')
    return assertions, skipped, failures


def corpus_files():
    return sorted(CORPUS.rglob('*.nlt'))


class FakeExecute:
    """Canned exchanges for the offline sensitivity tests."""

    def __init__(self, responses):
        self.responses = list(responses)

    def __call__(self, op, payload):
        expected_op, result = self.responses.pop(0)
        assert op == expected_op, (op, expected_op)
        if isinstance(result, errors.NabedraError):
            raise result
        return result


CANNED_FILE = '''# name: canned
require olap

statement ok
{"op":"table.create","name":"t","schema":{"id":"int64","v":"utf8","f":"float64"},"key":"id","format":"parquet"}

query nosort
{"op":"query.collect","table":"t","order_by":{"column":"v","desc":false}}
----
1.5\t2\ta
2.0\t1\tb
'''
DESCRIPTOR = {'id': '11111111-1111-4111-8111-111111111111', 'name': 't', 'key': 'id', 'format': 'parquet',
              'schema': {'id': 'int64', 'v': 'utf8', 'f': 'float64'}}


class LogicFormatTests(unittest.TestCase):
    def test_corpus_parses(self):
        files = corpus_files()
        self.assertGreaterEqual(len(files), 25)
        for path in files:
            with self.subTest(path=path.name):
                parse(path.read_text(encoding='utf-8'))

    def test_canonical_floats_match_rust(self):
        cases = [(0.0, '0.0'), (-0.0, '0.0'), (2.0, '2.0'), (-0.5, '-0.5'), (1.5, '1.5'), (100.0, '100.0'),
                 (0.001, '0.001'), (1e-7, '1e-7'), (1.5e-7, '1.5e-7'), (1e-6, '0.000001'),
                 (1e20, '100000000000000000000.0'), (1e21, '1e21'), (123456.789, '123456.789'),
                 (0.1 + 0.2, '0.30000000000000004'), (0.50000762939453125, '0.5000076293945312'),
                 (1.7976931348623157e308, '1.7976931348623157e308'),
                 (2.2250738585072014e-308, '2.2250738585072014e-308')]
        for value, text in cases:
            self.assertEqual(canonical_float(value), text, repr(value))

    def test_rendering_and_translation(self):
        self.assertEqual(render_string(''), '(empty)')
        self.assertEqual(render_string('a\tb\\c\nd'), 'a\\tb\\\\c\\nd')
        self.assertEqual(render_string('\x00x\x1f\x7f'), '\\x00x\\x1f\\x7f')
        self.assertEqual(render_value(True), 'true')
        self.assertEqual(render_value(3), '3')
        tr = Translator('p_')
        tr.observe('table.create', {'name': 'p_t', 'id': 'abc'})
        self.assertEqual(tr.translate('{"op":"query.count","table":"t","limit":3}'),
                         ('query.count', {'table_id': 'abc', 'query': {
                             'select': None, 'filter': None, 'order_by': None, 'limit': 3}}))
        self.assertEqual(tr.translate('{"op":"table.get","table":"x","key":1}')[1]['table_id'], MISSING_TABLE_ID)
        self.assertEqual(tr.translate('{"op":"table.describe","name":"t"}')[1]['name'], 'p_t')
        self.assertEqual(tr.render('query.explain', {}, 'scan p_t (11111111-1111-4111-8111-111111111111)'),
                         [['scan t (<uuid>)']])
        for invalid in ('', '9bad', 'x' * 65):
            command = json.dumps({'op': 'table.create', 'name': invalid})
            self.assertEqual(tr.translate(command)[1]['name'], invalid)
        long_name = 'x' * 64
        command = json.dumps({'op': 'table.create', 'name': long_name})
        physical = tr.translate(command)[1]['name']
        self.assertEqual(len(physical), 64)
        self.assertTrue(physical.startswith('p_'))
        self.assertEqual(tr.logical(physical), long_name)
        self.assertEqual(parse('# name: x\nquery nosort\n{}\n----\na\u2028b\n')['records'][0]['expected'],
                         ['a\u2028b'])
        with self.assertRaisesRegex(FormatError, 'wrong result columns'):
            tr.render('query.collect', {'query': {'select': ['id']}}, [{'id': 1, 'extra': 2}])

    def test_canned_correct_order_passes(self):
        execute = FakeExecute([('table.create', DESCRIPTOR),
                               ('query.collect', [{'id': 2, 'v': 'a', 'f': 1.5}, {'id': 1, 'v': 'b', 'f': 2.0}])])
        assertions, _, failures = run_file('canned.nlt', parse(CANNED_FILE), execute, '')
        self.assertEqual((assertions, failures), (2, []))

    def test_planted_sort_order_difference_fails(self):
        # The server answer is right; this replay planted a row-order difference.
        execute = FakeExecute([('table.create', DESCRIPTOR),
                               ('query.collect', [{'id': 1, 'v': 'b', 'f': 2.0}, {'id': 2, 'v': 'a', 'f': 1.5}])])
        _, _, failures = run_file('canned.nlt', parse(CANNED_FILE), execute, '')
        self.assertEqual(len(failures), 1, failures)
        self.assertIn('canned.nlt:7: wrong result', failures[0])

    def test_label_disagreement_and_error_mismatch_fail(self):
        text = ('# name: x\n\nquery rowsort label-a\n{"op":"query.count","table":"t"}\n\n'
                'query rowsort label-a\n{"op":"query.count","table":"t"}\n\n'
                'statement error not_found\n{"op":"query.count","table":"t"}\n')
        execute = FakeExecute([('query.count', 1), ('query.count', 2),
                               ('query.count', errors.ValidationError('bad', code='validation_error'))])
        _, _, failures = run_file('x.nlt', parse(text), execute, '')
        self.assertEqual(len(failures), 2, failures)

    def test_parser_rejects_malformed_files(self):
        for bad in ['statement ok\n{}\n', '# name: x\nquery nosort\n{}\n', '# name: x\nquery sideways\n{}\n----\n',
                    '# name: x\nstatement ok\n{}\n----\n', '# name: x\nskipif\nstatement ok\n{}\n',
                    '# name: x\nstatement ok\n\n', '# name: x\nquery nosort hash\n{}\n----\na\nb\n',
                    '# name: x\nstatement ok\n{}\n\nrequire olap\n', '# name: x\nbogus\n']:
            with self.subTest(bad=bad):
                with self.assertRaises(FormatError):
                    parse(bad)


class LogicReplayTests(unittest.TestCase):
    def test_replay_rust_transcripts(self):
        """Render the Rust runner's recorded server responses (NABEDRA_LOGIC_RECORD) through this replay."""
        root = os.environ.get('NABEDRA_LOGIC_TRANSCRIPTS')
        if not root:
            self.skipTest('not-run: NABEDRA_LOGIC_TRANSCRIPTS unset')
        total, failures = 0, []
        for path in corpus_files():
            rel = path.relative_to(CORPUS).as_posix()
            parsed = parse(path.read_text(encoding='utf-8'))
            if 'olap' not in parsed['engines']:
                continue
            log = json.loads((pathlib.Path(root) / (rel + '.json')).read_text(encoding='utf-8'))

            def execute(op, payload, log=log):
                entry = log.pop(0)
                self.assertEqual(entry['op'], op)
                if 'error' in entry:
                    raise errors.NabedraError(entry['error']['message'], code=entry['error']['code'])
                return json.loads(entry['data'])

            n, _, f = run_file('tests/logic/' + rel, parsed, execute, '')
            self.assertEqual(log, [], rel)
            total, failures = total + n, failures + f
        print(f'logic transcripts (python): {total - len(failures)} passed; {len(failures)} failed')
        self.assertGreater(total, 0)
        self.assertEqual(failures, [], '\n'.join(failures))

    def test_replay_corpus(self):
        url = os.environ.get('NABEDRA_LOGIC_URL')
        if not url:
            self.skipTest('not-run: NABEDRA_LOGIC_URL unset')
        token = os.environ.get('NABEDRA_LOGIC_TOKEN') or os.environ.get('NABEDRA_TOKEN')
        engine = os.environ.get('NABEDRA_LOGIC_ENGINE', 'olap')
        transport = _Transport(url, token)
        execute = lambda op, payload: transport.request(op, payload)  # noqa: E731
        total, skipped, failures = 0, [], []
        try:
            for path in corpus_files():
                rel = path.relative_to(CORPUS).as_posix()
                parsed = parse(path.read_text(encoding='utf-8'))
                if engine not in parsed['engines']:
                    skipped.append(f'{rel} requires {parsed["engines"]}')
                    continue
                prefix = 'lt' + uuid.uuid4().hex[:8] + '_'
                n, s, f = run_file('tests/logic/' + rel, parsed, execute, prefix)
                total, skipped, failures = total + n, skipped + s, failures + f
        finally:
            transport.close()
        for line in skipped:
            print('skipped:', line)
        print(f'logic replay (python): {total - len(failures)} passed; {len(failures)} failed; '
              f'{len(skipped)} skipped')
        self.assertGreater(total, 0, f'no logic records ran for engine {engine!r}')
        self.assertEqual(failures, [], '\n'.join(failures))


if __name__ == '__main__':
    unittest.main()
