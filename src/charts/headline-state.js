export const BASELINE_SERIES = ['frontier', 'OWID_WRL'];
export const DEFAULT_SERIES = [...BASELINE_SERIES, 'p10'];
const URL_ALIASES = {
  best: 'frontier',
  world: 'OWID_WRL',
  'un-africa': 'UN_AFR',
  'un-asia': 'UN_ASI',
  'un-europe': 'UN_EUR',
  'un-latin-america-caribbean': 'UN_LAC',
  'un-northern-america': 'UN_NAM',
  'un-oceania': 'UN_OCE',
};
const URL_NAMES = new Map(Object.entries(URL_ALIASES).map(([alias, code]) => [code, alias]));

export function createModel(data) {
  const countries = [
    ...new Map(
      data.rows.filter((row) => row.kind === 'country').map((row) => [row.code, row.entity]),
    ),
  ].sort((a, b) => a[1].localeCompare(b[1]));
  const options = [
    ...new Map(data.rows.filter((row) => row.value != null).map((row) => [row.series, row.entity])),
  ].sort((a, b) => a[1].localeCompare(b[1]));
  return {
    data,
    countries,
    options,
    names: new Map(data.rows.map((row) => [row.code, row.entity])),
    countryCodes: new Set(countries.map(([code]) => code)),
    optionCodes: new Set(options.map(([code]) => code)),
    rowsBySex: new Map(
      ['female', 'male'].map((sex) => [sex, data.rows.filter((row) => row.sex === sex)]),
    ),
  };
}

function normalizeSelection(codes, model) {
  return new Set([...BASELINE_SERIES, ...codes.filter((code) => model.optionCodes.has(code))]);
}

export function hasUrlState(search) {
  const query = new URLSearchParams(search);
  return ['sex', 'show', 'lines', 'country', 'start', 'end'].some((key) => query.has(key));
}

export function parseState(search, model) {
  const query = new URLSearchParams(search);
  const { start_year: minimum, end_year: maximum } = model.data;
  const year = (key, fallback) =>
    /^\d{4}$/.test(query.get(key) || '')
      ? Math.max(minimum, Math.min(maximum, Number(query.get(key))))
      : fallback;
  let startYear = year('start', minimum);
  let endYear = year('end', maximum);
  if (startYear >= endYear) {
    startYear = minimum;
    endYear = maximum;
  }
  const key = query.has('show') ? 'show' : query.has('lines') ? 'lines' : null;
  const codes = key
    ? query
        .getAll(key)
        .flatMap((value) => value.split(','))
        .map((code) => (Object.hasOwn(URL_ALIASES, code) ? URL_ALIASES[code] : code))
    : [
        ...DEFAULT_SERIES,
        ...query
          .getAll('country')
          .flatMap((value) => value.split(','))
          .filter((code) => model.countryCodes.has(code)),
      ];
  return {
    sex: ['m', 'male'].includes(query.get('sex')) ? 'male' : 'female',
    selectedSeries: normalizeSelection(codes, model),
    startYear,
    endYear,
  };
}

export function serializeState(state, href) {
  const url = new URL(href);
  url.searchParams.set('sex', state.sex === 'male' ? 'm' : 'f');
  url.searchParams.set(
    'show',
    [...state.selectedSeries].map((code) => URL_NAMES.get(code) || code).join(','),
  );
  url.searchParams.set('start', state.startYear);
  url.searchParams.set('end', state.endYear);
  url.searchParams.delete('lines');
  url.searchParams.delete('country');
  return url;
}

export function initialState({ search, embedded, saved, suggestedCountry }, model) {
  const state = parseState(search, model);
  if (embedded || hasUrlState(search)) return state;
  state.selectedSeries = normalizeSelection(
    saved ?? [...DEFAULT_SERIES, ...(suggestedCountry ? [suggestedCountry] : [])],
    model,
  );
  return state;
}

// Every user-facing state change passes through this reducer; DOM values are inputs, not storage.
export function updateState(state, action, model) {
  const next = { ...state, selectedSeries: new Set(state.selectedSeries) };
  const { start_year: minimum, end_year: maximum } = model.data;
  switch (action.type) {
    case 'sex':
      next.sex = action.sex === 'male' ? 'male' : 'female';
      break;
    case 'select':
      if (model.optionCodes.has(action.code)) {
        if (action.selected) next.selectedSeries.add(action.code);
        else next.selectedSeries.delete(action.code);
      }
      break;
    case 'clear':
      next.selectedSeries = new Set(BASELINE_SERIES);
      break;
    case 'year': {
      if (!Number.isFinite(action.year)) return state;
      const year = Math.max(minimum, Math.min(maximum, Math.round(action.year)));
      if (action.edge === 'start') next.startYear = Math.min(year, next.endYear - 1);
      else next.endYear = Math.max(year, next.startYear + 1);
      break;
    }
    case 'resetRange':
      next.startYear = minimum;
      next.endYear = maximum;
      break;
    default:
      throw new Error(`Unknown state action: ${action.type}`);
  }
  next.selectedSeries = normalizeSelection([...next.selectedSeries], model);
  return next;
}

export function visibleData(state, model) {
  const rows = model.rowsBySex.get(state.sex);
  const available = rows.filter(
    (row) =>
      state.selectedSeries.has(row.series) &&
      row.value != null &&
      row.year >= state.startYear &&
      row.year <= state.endYear,
  );
  let plotStart = available.length
    ? Math.min(...available.map((row) => row.year))
    : state.startYear;
  let plotEnd = available.length ? Math.max(...available.map((row) => row.year)) : state.endYear;
  if (plotStart === plotEnd) {
    plotStart = Math.max(state.startYear, plotStart - 1);
    plotEnd = Math.min(state.endYear, plotEnd + 1);
  }
  return { rows, available, plotStart, plotEnd };
}

export function tooltipEntries(entries, { pinned, pointerY, valueY, limit = 10 }) {
  // Preserve the currently agreed behavior: missing values are omitted even when pinned.
  const available = entries.filter((item) => item.row?.value != null);
  if (pinned || available.length <= limit) return available;
  const references = available.filter(isReference);
  const nearby = available
    .filter((item) => !isReference(item))
    .sort(
      (a, b) => Math.abs(valueY(a.row.value) - pointerY) - Math.abs(valueY(b.row.value) - pointerY),
    );
  const ids = new Set(
    [...references, ...nearby.slice(0, Math.max(0, limit - references.length))].map(
      (item) => item.id,
    ),
  );
  return available.filter((item) => ids.has(item.id));
}

export function isReference(item) {
  return BASELINE_SERIES.includes(item.id) || /^p\d+$/.test(item.id);
}

export function scheduleFrame(callback, requestFrame = requestAnimationFrame) {
  let pending = false;
  return () => {
    if (pending) return;
    pending = true;
    requestFrame(() => {
      pending = false;
      callback();
    });
  };
}
