import { BASELINE_SERIES } from './headline-state.js';

export function mountControls({ model, getState, update, onTableToggle }) {
  const { data, countryCodes, options } = model;
  const baselineLines = new Set(BASELINE_SERIES);
  const countryList = document.getElementById('country-list');
  const countrySearch = document.getElementById('country-search');
  const pickerMedia = window.matchMedia('(max-width: 600px)');
  if (pickerMedia.matches) document.getElementById('country-picker').open = false;
  pickerMedia.addEventListener('change', (event) => {
    if (event.matches) document.getElementById('country-picker').open = false;
  });
  let pickerFilter = 'all';
  function updateSelection() {
    const state = getState();
    countryList.querySelectorAll('.picker-section').forEach((heading) => heading.remove());
    const seenSections = new Set();
    for (const label of countryList.querySelectorAll('label')) {
      const matchesFilter =
        pickerFilter === 'all' ||
        (pickerFilter === 'selected' && state.selectedSeries.has(label.dataset.code)) ||
        label.dataset.category === pickerFilter;
      label.hidden =
        !matchesFilter ||
        !label.textContent
          .toLocaleLowerCase()
          .includes(countrySearch.value.trim().toLocaleLowerCase());
      label.querySelector('input').checked = state.selectedSeries.has(label.dataset.code);
      if (!label.hidden && !seenSections.has(label.dataset.group)) {
        const heading = document.createElement('div');
        heading.className = 'picker-section';
        heading.textContent = label.dataset.group;
        label.before(heading);
        seenSections.add(label.dataset.group);
      }
    }
    const selectionCount = [...state.selectedSeries].filter(
      (code) => !baselineLines.has(code),
    ).length;
    document.getElementById('picker-heading-count').textContent =
      `${selectionCount} ${selectionCount === 1 ? 'comparison' : 'comparisons'}`;
    document.getElementById('clear-countries').disabled = selectionCount === 0;
    document.getElementById('country-empty').hidden = [
      ...countryList.querySelectorAll('label'),
    ].some((label) => !label.hidden);
  }
  // Sort the picker separately so existing series colors remain stable.
  const pickerRank = (code) => (/^p\d+$/.test(code) ? 0 : countryCodes.has(code) ? 2 : 1);
  const pickerOptions = [...options].sort(
    (a, b) =>
      pickerRank(a[0]) - pickerRank(b[0]) ||
      (pickerRank(a[0]) === 0
        ? Number(a[0].slice(1)) - Number(b[0].slice(1))
        : a[1].localeCompare(b[1])),
  );
  for (const [code, name] of pickerOptions) {
    if (baselineLines.has(code)) continue;
    const label = document.createElement('label');
    label.dataset.code = code;
    const input = document.createElement('input');
    input.type = 'checkbox';
    input.value = code;
    input.setAttribute('aria-label', name.replace(' (population-weighted)', ''));
    input.addEventListener('change', () => {
      update({ type: 'select', code, selected: input.checked });
    });
    const category = countryCodes.has(code)
      ? 'Country'
      : /^p\d+$/.test(code)
        ? 'Percentile'
        : code === 'frontier'
          ? 'Reference'
          : 'Group';
    label.dataset.category =
      category === 'Percentile' ? 'percentiles' : category === 'Country' ? 'countries' : 'regions';
    label.dataset.group =
      category === 'Percentile' ? 'Percentiles' : category === 'Country' ? 'Countries' : 'Regions';
    const optionName = document.createElement('span');
    optionName.className = 'option-name';
    optionName.textContent = name.replace(' (population-weighted)', '');
    const value = document.createElement('span');
    value.className = 'option-value';
    label.append(input, optionName, value);
    countryList.append(label);
  }
  countrySearch.addEventListener('input', updateSelection);
  document.querySelectorAll('[data-picker-filter]').forEach((button) => {
    button.addEventListener('click', () => {
      pickerFilter = button.dataset.pickerFilter;
      document
        .querySelectorAll('[data-picker-filter]')
        .forEach((item) => item.setAttribute('aria-pressed', String(item === button)));
      updateSelection();
      countryList.scrollTop = 0;
    });
  });
  document.getElementById('clear-countries').addEventListener('click', () => {
    update({ type: 'clear' });
  });
  const startControl = document.getElementById('start-year');
  const endControl = document.getElementById('end-year');
  for (const input of [startControl, endControl]) {
    input.min = data.start_year;
    input.max = data.end_year;
  }
  const presets = [data.start_year, 1850, 1950, 2000, data.end_year].filter(
    (year, i, all) => year >= data.start_year && year <= data.end_year && all.indexOf(year) === i,
  );
  const marks = document.getElementById('year-marks');
  presets.forEach((year) => {
    const option = document.createElement('option');
    option.value = year;
    marks.append(option);
  });
  function moveNearestHandle(year) {
    const control =
      Math.abs(year - getState().startYear) <= Math.abs(year - getState().endYear)
        ? startControl
        : endControl;
    update({ type: 'year', edge: control === startControl ? 'start' : 'end', year });
    control.focus({ preventScroll: true });
  }
  document.getElementById('timeline').addEventListener('click', (event) => {
    // Handle drags and preset buttons have their own handlers.
    if (event.target.closest('input, button')) return;
    const bounds = event.currentTarget.getBoundingClientRect();
    if (event.clientY - bounds.top > 36) return;
    const fraction = Math.max(0, Math.min(1, (event.clientX - bounds.left) / bounds.width));
    let year = Math.round(data.start_year + fraction * (data.end_year - data.start_year));
    const nearest = presets.reduce((a, b) => (Math.abs(b - year) < Math.abs(a - year) ? b : a));
    if ((Math.abs(nearest - year) * bounds.width) / (data.end_year - data.start_year) <= 8)
      year = nearest;
    moveNearestHandle(year);
  });
  presets.forEach((year) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'year-marker';
    button.textContent = year;
    button.dataset.year = year;
    button.setAttribute('aria-label', `Move nearest range handle to ${year}`);
    button.style.left = `${(100 * (year - data.start_year)) / (data.end_year - data.start_year)}%`;
    button.addEventListener('click', () => {
      moveNearestHandle(year);
    });
    document.getElementById('timeline').append(button);
  });
  // Snap on pointer release, but retain single-year keyboard adjustments.
  for (const control of [startControl, endControl]) {
    control.addEventListener('pointerup', () => {
      const year = Number(control.value);
      const nearest = presets.reduce((a, b) => (Math.abs(b - year) < Math.abs(a - year) ? b : a));
      const pixelsPerYear =
        document.getElementById('timeline').clientWidth / (data.end_year - data.start_year);
      if (Math.abs(nearest - year) * pixelsPerYear <= 8) {
        update({ type: 'year', edge: control === startControl ? 'start' : 'end', year: nearest });
      }
    });
  }
  for (const [id, isStart] of [
    ['start-value', true],
    ['end-value', false],
  ]) {
    const field = document.getElementById(id);
    function commitYear() {
      if (/^\d{4}$/.test(field.value.trim())) {
        const year = Math.max(data.start_year, Math.min(data.end_year, Number(field.value)));
        update({ type: 'year', edge: isStart ? 'start' : 'end', year });
      } else {
        field.value = isStart ? getState().startYear : getState().endYear;
      }
    }
    field.addEventListener('change', commitYear);
    field.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        event.preventDefault();
        commitYear();
      }
      if (event.key === 'Escape') field.value = isStart ? getState().startYear : getState().endYear;
    });
  }
  startControl.addEventListener('input', () =>
    update({ type: 'year', edge: 'start', year: Number(startControl.value) }),
  );
  endControl.addEventListener('input', () =>
    update({ type: 'year', edge: 'end', year: Number(endControl.value) }),
  );
  function resetRange() {
    update({ type: 'resetRange' });
  }
  document.getElementById('view-data').addEventListener('click', (event) => {
    const table = document.getElementById('chart-data');
    table.hidden = !table.hidden;
    event.currentTarget.setAttribute('aria-expanded', String(!table.hidden));
    onTableToggle();
  });
  document.getElementById('timeline').addEventListener('dblclick', (event) => {
    if (event.target.closest('button')) return;
    event.preventDefault();
    resetRange();
  });
  for (const control of [startControl, endControl]) {
    control.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') {
        event.preventDefault();
        resetRange();
      }
    });
  }

  document.querySelectorAll('input[name="sex"]').forEach((input) => {
    input.addEventListener('change', () => update({ type: 'sex', sex: input.value }));
  });

  function render(state, view) {
    document.querySelector(`input[name="sex"][value="${state.sex}"]`).checked = true;
    updateSelection();
    startControl.value = state.startYear;
    endControl.value = state.endYear;
    const timeline = document.getElementById('timeline');
    timeline.style.setProperty(
      '--range-start',
      `${(100 * (state.startYear - data.start_year)) / (data.end_year - data.start_year)}%`,
    );
    timeline.style.setProperty(
      '--range-end',
      `${(100 * (state.endYear - data.start_year)) / (data.end_year - data.start_year)}%`,
    );
    // Keep preset labels readable even on a narrow chart; snapping still uses every preset.
    const markerWidth = timeline.clientWidth;
    const shownMarkers = [];
    for (const year of [data.start_year, data.end_year, 1950, 1850, 2000]) {
      const position = ((year - data.start_year) / (data.end_year - data.start_year)) * markerWidth;
      if (
        presets.includes(year) &&
        shownMarkers.every((other) => Math.abs(other - position) >= 44)
      ) {
        shownMarkers.push(position);
        timeline.querySelector(`[data-year="${year}"]`).hidden = false;
      } else if (presets.includes(year))
        timeline.querySelector(`[data-year="${year}"]`).hidden = true;
    }
    document.getElementById('start-value').value = state.startYear;
    document.getElementById('end-value').value = state.endYear;
    const endValues = new Map(
      view.rows.filter((row) => row.year === view.plotEnd).map((row) => [row.series, row.value]),
    );
    for (const label of countryList.querySelectorAll('label')) {
      const code = label.dataset.code;
      const value = label.querySelector('.option-value');
      const endValue = endValues.get(code);
      value.textContent = endValue == null ? '—' : endValue.toFixed(1);
      value.title =
        endValue == null
          ? `No data for ${view.plotEnd}`
          : `${view.plotEnd}: ${endValue.toFixed(1)} years`;
    }
  }
  return { render };
}
