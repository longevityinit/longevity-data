import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  createModel,
  parseState,
  serializeState,
  initialState,
  updateState,
  visibleData,
  tooltipEntries,
  scheduleFrame,
} from '../../src/charts/headline-state.js';

const rows = [];
for (const sex of ['female', 'male']) {
  for (const [code, entity, kind, years] of [
    ['frontier', 'Best practice', 'summary', [1850, 1950, 2023]],
    ['OWID_WRL', 'World', 'summary', [1950, 2023]],
    ['p10', '10th percentile', 'summary', [1950, 2023]],
    ['SWE', 'Sweden', 'country', [1751, 1950, 2023]],
    ['DEU', 'Germany', 'country', [1950, 2023]],
    ['UN_EUR', 'Europe', 'summary', [1950, 2023]],
  ]) {
    for (const year of years)
      rows.push({ code, series: code, entity, sex, year, kind, value: sex === 'female' ? 80 : 75 });
  }
}
const model = createModel({ start_year: 1751, end_year: 2023, rows });
const comparable = (state) => ({ ...state, selectedSeries: [...state.selectedSeries] });

test('canonical URL round trip preserves selections, sex, years, and unrelated parameters', () => {
  const state = parseState(
    '?sex=male&lines=frontier,OWID_WRL,DEU,p10,UN_EUR&start=1900&end=2000',
    model,
  );
  const url = serializeState(
    state,
    'https://example.org/chart?theme=light&lines=old&country=SWE#data',
  );
  assert.equal(url.searchParams.get('sex'), 'm');
  assert.equal(url.searchParams.get('show'), 'best,world,DEU,p10,un-europe');
  assert.equal(url.searchParams.get('theme'), 'light');
  assert.equal(url.hash, '#data');
  assert.equal(url.searchParams.has('lines'), false);
  assert.equal(url.searchParams.has('country'), false);
  assert.deepEqual(comparable(parseState(url.search, model)), comparable(state));
});

test('explicit empty show, aliases, repeated selections, unknown codes, and legacy precedence', () => {
  assert.deepEqual(
    [...parseState('?show=&lines=SWE&country=DEU', model).selectedSeries],
    ['frontier', 'OWID_WRL'],
  );
  assert.deepEqual(
    [...parseState('?show=best,world,DEU&show=un-europe,DEU,nope', model).selectedSeries],
    ['frontier', 'OWID_WRL', 'DEU', 'UN_EUR'],
  );
  assert.ok(parseState('?country=SWE', model).selectedSeries.has('SWE'));
  assert.equal(parseState('?sex=invalid', model).sex, 'female');
});

test('URL wins over saved preferences; parameter-free embeds stay fixed', () => {
  const preferences = { search: '', embedded: false, saved: ['DEU'], suggestedCountry: 'SWE' };
  assert.deepEqual(
    [...initialState(preferences, model).selectedSeries],
    ['frontier', 'OWID_WRL', 'DEU'],
  );
  assert.deepEqual(
    [...initialState({ ...preferences, embedded: true }, model).selectedSeries],
    ['frontier', 'OWID_WRL', 'p10'],
  );
  assert.deepEqual(
    [...initialState({ ...preferences, search: '?show=world' }, model).selectedSeries],
    ['frontier', 'OWID_WRL'],
  );
  assert.ok(initialState({ ...preferences, saved: null }, model).selectedSeries.has('SWE'));
  assert.equal(initialState({ ...preferences, saved: [] }, model).selectedSeries.has('SWE'), false);
});

test('selection changes are immutable and clearing retains only the baseline', () => {
  const original = parseState('?show=best,world', model);
  const selected = updateState(original, { type: 'select', code: 'DEU', selected: true }, model);
  assert.equal(original.selectedSeries.has('DEU'), false);
  assert.equal(selected.selectedSeries.has('DEU'), true);
  const baseline = updateState(
    selected,
    { type: 'select', code: 'frontier', selected: false },
    model,
  );
  assert.equal(baseline.selectedSeries.has('frontier'), true);
  assert.deepEqual(
    [...updateState(selected, { type: 'clear' }, model).selectedSeries],
    ['frontier', 'OWID_WRL'],
  );
});

test('URL ranges clamp; reversed/equal/malformed ranges reset predictably', () => {
  assert.equal(parseState('?start=1000&end=9999', model).startYear, 1751);
  for (const query of ['?start=2000&end=1900', '?start=2000&end=2000', '?start=nope']) {
    const state = parseState(query, model);
    assert.equal(state.startYear, 1751);
    assert.equal(state.endYear, 2023);
  }
});

test('range actions never cross, clamp bounds, and reset without losing selection or sex', () => {
  let state = parseState('?sex=m&show=DEU&start=1900&end=2000', model);
  state = updateState(state, { type: 'year', edge: 'start', year: 2023 }, model);
  assert.equal(state.startYear, 1999);
  state = updateState(state, { type: 'year', edge: 'end', year: 1751 }, model);
  assert.equal(state.endYear, 2000);
  state = updateState(state, { type: 'resetRange' }, model);
  assert.equal(state.startYear, 1751);
  assert.equal(state.endYear, 2023);
  assert.equal(state.sex, 'male');
  assert.ok(state.selectedSeries.has('DEU'));
});

test('plot trims empty edges and expands again when an early country is selected', () => {
  let state = parseState('?show=best,world', model);
  assert.equal(visibleData(state, model).plotStart, 1850);
  state = updateState(state, { type: 'select', code: 'SWE', selected: true }, model);
  assert.equal(visibleData(state, model).plotStart, 1751);
  state = updateState(state, { type: 'year', edge: 'end', year: 1800 }, model);
  const view = visibleData(state, model);
  assert.ok(view.plotStart < view.plotEnd);
  assert.equal(view.available.length, 1);
});

test('missing tooltip values remain omitted in both modes, per the current product decision', () => {
  const entries = [
    { id: 'frontier', row: { value: 55 } },
    { id: 'OWID_WRL', row: { value: null } },
    { id: 'SWE' },
  ];
  for (const pinned of [false, true]) {
    assert.deepEqual(
      tooltipEntries(entries, { pinned, pointerY: 0, valueY: (x) => x }).map((item) => item.id),
      ['frontier'],
    );
  }
});

test('hover row limit prioritizes references and nearest comparisons; pinned shows all available', () => {
  const entries = [
    { id: 'frontier', row: { value: 100 } },
    { id: 'p10', row: { value: 50 } },
    ...Array.from({ length: 15 }, (_, i) => ({ id: `country${i}`, row: { value: i } })),
  ];
  const hover = tooltipEntries(entries, { pinned: false, pointerY: 0, valueY: (x) => x });
  assert.equal(hover.length, 10);
  assert.ok(hover.some((item) => item.id === 'frontier'));
  assert.ok(hover.some((item) => item.id === 'country0'));
  assert.equal(tooltipEntries(entries, { pinned: true }).length, 17);
});

test('rapid updates coalesce into one frame and a later frame can be scheduled', () => {
  const callbacks = [];
  let draws = 0;
  const schedule = scheduleFrame(
    () => draws++,
    (callback) => callbacks.push(callback),
  );
  schedule();
  schedule();
  schedule();
  assert.equal(callbacks.length, 1);
  callbacks.shift()();
  assert.equal(draws, 1);
  schedule();
  assert.equal(callbacks.length, 1);
});
