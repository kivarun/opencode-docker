#!/usr/bin/env python3
"""Audit source provenance and a finite extraction corpus. No runtime imports or writes.

This checks the extracted table against an independent transcription and frozen
expectations. It does not implement a pipeline runner or prove production parity.
"""
from collections import Counter
from copy import deepcopy
from hashlib import sha256
from itertools import product
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def require(condition, message):
    if not condition:
        raise ValueError(message)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def read(name):
    return json.loads((ROOT / name).read_text(), object_pairs_hook=unique_object)


def matches(flags, condition):
    return all(flags[key] is value for key, value in condition.items())


def table_decision(flags, table):
    allowed = {rule['decision'] for rule in table['rules_in_priority_order']}
    for constraint in table['hard_constraints']:
        if matches(flags, constraint['when']):
            if 'only' in constraint:
                allowed.intersection_update(constraint['only'])
            else:
                allowed.difference_update(constraint['forbid'])
    for rule in table['rules_in_priority_order']:
        if rule['decision'] in allowed and matches(flags, rule['when']):
            return rule['decision']
    return None


def source_relations(flags):
    # FC1..FC6: architect 7.5 definitions plus explicit flag consistency.
    t = flags['requires_task_change']
    i = flags['issues_exist']
    m = flags['has_major_issue']
    e = flags['stage_cannot_continue_without_external_input']
    a = flags['acceptance_criteria_satisfied']
    predicates = [
        flags['has_only_minor_issues'] == (i and not m),
        flags['no_blocking_issues'] == (not m and not e and not t),
        flags['can_close_normally'] == (a and not i),
        not m or i,
        a or m,
        not e or not t,
    ]
    return [f'FC{index}' for index, valid in enumerate(predicates, 1) if not valid]


def source_decision(flags):
    # Independent direct transcription of architect 7.6--7.8.
    # HC1 and HC4 are exclusive requirements, including HC4's positive warning.
    t = flags['requires_task_change']
    l = flags['iteration_limit_reached']
    m = flags['has_major_issue']
    h = flags['has_only_minor_issues']
    s = flags['needs_stage_contract_change']
    p = flags['needs_pipeline_plan_change']
    e = flags['stage_cannot_continue_without_external_input']
    c = flags['can_close_normally']
    if t:
        return 'architectural_proposal'
    if e and not p and not s:
        return 'architectural_warning'
    if c:
        return 'close_stage'
    if l and h and not c and not m:
        return 'close_stage_ignore_minor'
    if not e and not c and not p and not s and not l:
        return 'rework_same_stage'
    if not e and m and not p and s and not l:
        return 'rework_change_stage_contract'
    if m and p:
        return 'rework_change_pipeline_plan'
    return None


def refs_in(value):
    if isinstance(value, dict):
        if set(value) == {'source', 'lines'}:
            yield value
        for child in value.values():
            yield from refs_in(child)
    elif isinstance(value, list):
        for child in value:
            yield from refs_in(child)


def apply_fixture(base, overlay):
    result = deepcopy(base)
    if 'state' in overlay:
        result['state'] = deepcopy(overlay['state'])
    if 'state_patch' in overlay:
        require(isinstance(result.get('state'), dict), 'state_patch requires a state')
        result['state'].update(overlay['state_patch'])
    for key in overlay.get('absent_state_fields', []):
        result['state'].pop(key, None)
    files = set(overlay.get('files_present', result.get('files_present', [])))
    files.update(overlay.get('files_add', []))
    files.difference_update(overlay.get('files_remove', []))
    result['files_present'] = sorted(files)
    return result


def main():
    documents = {p.name: read(p.name) for p in sorted(ROOT.glob('*.json'))}
    sources = documents['sources.json']
    table = documents['decision-table.json']
    vectors = documents['decision-vectors.json']
    scenarios = documents['scenarios.json']
    transitions = documents['state-transitions.json']['transitions']
    notes = documents['specification-notes.json']['notes']

    source_map = {}
    for source in sources['sources']:
        data = (ROOT / source['path']).read_bytes()
        require(sha256(data).hexdigest() == source['sha256'], f"Changed source {source['id']}")
        require(len(data) == source['bytes'], f"Wrong byte count for {source['id']}")
        require(len(data.splitlines()) == source['lines'], f"Wrong line count for {source['id']}")
        source_map[source['id']] = source
    reference_count = 0
    for name, document in documents.items():
        require(document['format_version'] == 1, f'{name}: format_version')
        for reference in refs_in(document):
            source = source_map[reference['source']]
            lo, hi = reference['lines']
            require(type(lo) is int and type(hi) is int and 1 <= lo <= hi <= source['lines'],
                    f'{name}: invalid source range {reference}')
            reference_count += 1

    fields = [field['name'] for field in table['input_fields']]
    require(len(fields) == len(set(fields)) == 11, 'Eleven distinct boolean fields required')
    require(fields == vectors['bit_order'], 'Vector bit order differs from table')
    priority = ['close_stage', 'close_stage_ignore_minor', 'rework_same_stage',
                'rework_change_stage_contract', 'rework_change_pipeline_plan',
                'architectural_proposal', 'architectural_warning']
    require([r['decision'] for r in table['rules_in_priority_order']] == priority,
            'Source priority changed')
    for item in table['rules_in_priority_order'] + table['hard_constraints']:
        require(set(item['when']) <= set(fields), f"Unknown field in {item['id']}")
        require(all(type(v) is bool for v in item['when'].values()), 'Nonboolean rule guard')

    stored = {row['input']: row for row in vectors['rows']}
    require(len(stored) == len(vectors['rows']), 'Repeated decision vector')
    counts = Counter()
    covered = set()
    for bits in product([False, True], repeat=len(fields)):
        flags = dict(zip(fields, bits))
        bit_string = ''.join('1' if bit else '0' for bit in bits)
        if source_relations(flags):
            counts['relation_rejected'] += 1
            require(bit_string not in stored, 'Inconsistent vector in admitted corpus')
            continue
        expected = source_decision(flags)
        require(table_decision(flags, table) == expected, f'Table transcription mismatch: {bit_string}')
        require(bit_string in stored, f'Missing frozen vector {bit_string}')
        row = stored[bit_string]
        require(row['id'] == 'DV-' + bit_string, f'Wrong vector identity {bit_string}')
        require(row['expected'] == expected, f'Golden expectation mismatch: {bit_string}')
        require(row['status'] == ('selected' if expected else 'uncovered'), f'Wrong status: {bit_string}')
        covered.add(bit_string)
        counts[expected or 'uncovered'] += 1
    require(set(stored) == covered, 'Unexpected frozen vector')
    require(counts['relation_rejected'] == vectors['relation_rejected'] == 1952, 'Rejected count')
    require(len(covered) == vectors['consistent_vectors'] == 96, 'Consistent count')
    require(counts['uncovered'] == vectors['uncovered'] == 14, 'Uncovered count')
    require(sum(counts[d] for d in priority) == vectors['selected'] == 82, 'Selected count')
    require(2 ** len(fields) == vectors['total_assignments'] == 2048, 'Assignment count')

    transition_ids = {t['id'] for t in transitions}
    note_ids = {n['id'] for n in notes}
    require(len(transition_ids) == len(transitions) == 16, 'Repeated/missing transition')
    require(len(note_ids) == len(notes) == 12, 'Repeated/missing specification note')
    cases = scenarios['cases']
    require(len({c['id'] for c in cases}) == len(cases), 'Repeated scenario ID')
    fixtures = {}
    for name, fixture in scenarios['fixtures'].items():
        parent = fixtures[fixture['extends']] if 'extends' in fixture else {}
        fixtures[name] = apply_fixture(parent, fixture)
    referenced_transitions = set()
    named_decisions = 0
    for case in cases:
        require(case['sources'] and case['expected'] and case['event'], f"Incomplete {case['id']}")
        require(set(case['transition_refs']) <= transition_ids, f"Unknown transition: {case['id']}")
        require(set(case['spec_notes']) <= note_ids, f"Unknown note: {case['id']}")
        referenced_transitions.update(case['transition_refs'])
        given = case['given']
        parent = fixtures[given['fixture']] if 'fixture' in given else {}
        expanded = apply_fixture(parent, given)
        state = expanded.get('state')
        assertions = case['expected'].get('assert', {})
        writes = assertions.get('state_subset', {})
        for field in assertions.get('state_unchanged', []):
            require(isinstance(state, dict) and field in state, f"Unknown unchanged field in {case['id']}")
            require(field not in writes or writes[field] == state[field], f"Contradictory assertions: {case['id']}")
        require(not set(assertions.get('must_update', [])) & set(assertions.get('files_unchanged', [])),
                f"Contradictory file assertions: {case['id']}")
        if case['kind'] in ('decision', 'decision_input'):
            flags = given['flags']
            missing = set(fields) - set(flags)
            nonboolean = {k for k, v in flags.items() if type(v) is not bool}
            expected = case['expected']
            if expected['status'] == 'invalid_shape':
                require(missing or nonboolean, f"Expected malformed facts: {case['id']}")
                require(set(expected.get('missing', [])) <= missing, 'Missing field witness')
                require(set(expected.get('non_boolean', [])) <= nonboolean, 'Nonboolean witness')
                continue
            require(not missing and not nonboolean and set(flags) == set(fields), f"Bad facts: {case['id']}")
            invalid = source_relations(flags)
            if expected['status'] == 'inconsistent_facts':
                require(set(expected['required_violations']) <= set(invalid), f"Missing violation: {case['id']}")
                continue
            require(not invalid, f"Inconsistent named scenario: {case['id']}")
            require(table_decision(flags, table) == expected['decision'], f"Named decision mismatch: {case['id']}")
            require(source_decision(flags) == expected['decision'], f"Independent decision mismatch: {case['id']}")
            counters = given['counters']
            require(flags['iteration_limit_reached'] == (counters['STAGE_ITERATION'] >= counters['MAX_STAGE_ITERATIONS']),
                    f"Counter boundary mismatch: {case['id']}")
            named_decisions += 1
    require(referenced_transitions == transition_ids, 'A transition has no named scenario')
    for transition in transitions:
        require(set(transition['spec_notes']) <= note_ids, 'Unknown transition note')

    print('Source copies: 2/2 SHA-256, byte counts and line counts match')
    print(f'Source references: {reference_count} within archived source bounds')
    print('Decision enumeration: 2048 assignments; 1952 violate extracted relations')
    print('Admitted vectors: 96; selected 82; uncovered 14 (preserved, no fallback)')
    print('Selected distribution: ' + json.dumps({d: counts[d] for d in priority}))
    print(f'Named cases: {len(cases)} structurally checked; {named_decisions} decision expectations independently checked')
    print('Transition coverage: 16/16; notes: 10 open, 2 resolved by source precedence')
    print('No production execution, LLM run, transition simulator, or end-to-end parity claim')


if __name__ == '__main__':
    main()
