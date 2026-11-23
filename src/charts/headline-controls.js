import { BASELINE_SERIES } from './headline-state.js';

export function mountControls({ model, getState, update, onTableOpen }) {
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
  let rangeStart = data.start_year;
  let rangeEnd = data.end_year;
  let presets = [data.start_year, 1850, 1950, 2000, data.end_year].filter(
    (year, i, all) => year >= data.start_year && year <= data.end_year && all.indexOf(year) === i,
  );
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
    let year = Math.round(rangeStart + fraction * (rangeEnd - rangeStart));
    const nearest = presets.reduce((a, b) => (Math.abs(b - year) < Math.abs(a - year) ? b : a));
    if ((Math.abs(nearest - year) * bounds.width) / (rangeEnd - rangeStart) <= 8) year = nearest;
    moveNearestHandle(year);
  });
  function renderPresets() {
    const marks = document.getElementById('year-marks');
    marks.replaceChildren();
    document.querySelectorAll('.year-marker').forEach((marker) => marker.remove());
    presets = [...new Set([rangeStart, 1800, 1850, 1900, 1950, 2000, rangeEnd])].filter(
      (year) => year >= rangeStart && year <= rangeEnd,
    );
    for (const year of presets) {
      const option = document.createElement('option');
      option.value = year;
      marks.append(option);
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'year-marker';
      button.textContent = year;
      button.dataset.year = year;
      button.setAttribute('aria-label', `Move nearest range handle to ${year}`);
      button.style.left = `${(100 * (year - rangeStart)) / (rangeEnd - rangeStart)}%`;
      button.addEventListener('click', () => moveNearestHandle(year));
      document.getElementById('timeline').append(button);
    }
  }
  // Snap on pointer release, but retain single-year keyboard adjustments.
  for (const control of [startControl, endControl]) {
    control.addEventListener('pointerup', () => {
      const year = Number(control.value);
      const nearest = presets.reduce((a, b) => (Math.abs(b - year) < Math.abs(a - year) ? b : a));
      const pixelsPerYear =
        document.getElementById('timeline').clientWidth / (rangeEnd - rangeStart);
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
        const year = Math.max(rangeStart, Math.min(rangeEnd, Number(field.value)));
        update({ type: 'year', edge: isStart ? 'start' : 'end', year });
        field.value = isStart ? getState().startYear : getState().endYear;
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
  const tabs = [...document.querySelectorAll('.data-tabs [role="tab"]')];
  function selectTab(tab, focus = false) {
    for (const item of tabs) {
      const selected = item === tab;
      item.setAttribute('aria-selected', String(selected));
      item.tabIndex = selected ? 0 : -1;
      document.getElementById(item.getAttribute('aria-controls')).hidden = !selected;
    }
    if (focus) tab.focus();
    if (tab.id === 'tab-table') onTableOpen();
  }
  tabs.forEach((tab, index) => {
    tab.addEventListener('click', () => selectTab(tab));
    tab.addEventListener('keydown', (event) => {
      let next;
      if (event.key === 'ArrowRight') next = (index + 1) % tabs.length;
      if (event.key === 'ArrowLeft') next = (index + tabs.length - 1) % tabs.length;
      if (event.key === 'Home') next = 0;
      if (event.key === 'End') next = tabs.length - 1;
      if (next !== undefined) {
        event.preventDefault();
        selectTab(tabs[next], true);
      }
    });
  });
  for (const [buttonId, dialogId] of [
    ['view-data', 'chart-data'],
    ['download-data', 'download-dialog'],
  ]) {
    const button = document.getElementById(buttonId);
    const dialog = document.getElementById(dialogId);
    button.addEventListener('click', () => {
      if (dialog.open) return;
      dialog.showModal();
      if (dialogId === 'chart-data') selectTab(tabs[0]);
    });
    dialog.querySelector('.dialog-close').addEventListener('click', () => dialog.close());
    dialog.addEventListener('click', (event) => {
      if (event.target !== dialog) return;
      const box = dialog.getBoundingClientRect();
      if (
        event.clientX < box.left ||
        event.clientX > box.right ||
        event.clientY < box.top ||
        event.clientY > box.bottom
      )
        dialog.close();
    });
    // Keep Escape local to the modal rather than also dismissing a pinned chart tooltip.
    dialog.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') event.stopPropagation();
    });
  }
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
    // Availability uses the full selected series, so zooming never traps the handles.
    const years = view.rows
      .filter((row) => state.selectedSeries.has(row.series) && row.value != null)
      .map((row) => row.year);
    rangeStart = years.length ? Math.min(...years) : data.start_year;
    rangeEnd = years.length ? Math.max(...years) : data.end_year;
    if (rangeStart === rangeEnd) {
      rangeStart = Math.max(data.start_year, rangeStart - 1);
      rangeEnd = Math.min(data.end_year, rangeEnd + 1);
    }
    for (const input of [startControl, endControl]) {
      input.min = rangeStart;
      input.max = rangeEnd;
    }
    renderPresets();
    const shownStart = state.startYear;
    const shownEnd = state.endYear;
    startControl.value = shownStart;
    endControl.value = shownEnd;
    const timeline = document.getElementById('timeline');
    timeline.style.setProperty(
      '--range-start',
      `${(100 * (shownStart - rangeStart)) / (rangeEnd - rangeStart)}%`,
    );
    timeline.style.setProperty(
      '--range-end',
      `${(100 * (shownEnd - rangeStart)) / (rangeEnd - rangeStart)}%`,
    );
    // Keep preset labels readable even on a narrow chart; snapping still uses every preset.
    const markerWidth = timeline.clientWidth;
    const shownMarkers = [];
    for (const year of new Set([rangeStart, rangeEnd, 1950, 1900, 1850, 1800, 2000])) {
      const position = ((year - rangeStart) / (rangeEnd - rangeStart)) * markerWidth;
      if (
        presets.includes(year) &&
        shownMarkers.every((other) => Math.abs(other - position) >= 44)
      ) {
        shownMarkers.push(position);
        timeline.querySelector(`[data-year="${year}"]`).hidden = false;
      } else if (presets.includes(year))
        timeline.querySelector(`[data-year="${year}"]`).hidden = true;
    }
    for (const [id, year] of [
      ['start-value', shownStart],
      ['end-value', shownEnd],
    ]) {
      const field = document.getElementById(id);
      // A queued redraw must not replace an edit that has not been committed.
      if (document.activeElement !== field) field.value = year;
    }
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
