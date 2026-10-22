/*!
Tooltip positioning adapted from the MIT-licensed OWID Grapher revision
a139fcfde4ffdd658019cf48962d23a985414a95.
Sources:
https://github.com/owid/owid-grapher/blob/a139fcfde4ffdd658019cf48962d23a985414a95/packages/@ourworldindata/grapher/src/tooltip/TooltipCard.tsx
https://github.com/owid/owid-grapher/blob/a139fcfde4ffdd658019cf48962d23a985414a95/packages/@ourworldindata/grapher/src/lineCharts/LineChartTooltip.tsx

The MIT License (MIT)
Copyright (c) 2024 Our World in Data

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
/* Presentation only: summary series are calculated by Python. */
(async function () {
  const status = document.getElementById('status');
  const container = document.getElementById('chart');
  const referenceSeries = [
    {id: 'frontier', name: 'Best practice', color: '#ff5722'},
    {id: 'OWID_WRL', name: 'World life expectancy', color: '#2680ff'},
    {id: 'p10', name: '10th percentile (population-weighted)', color: '#e63e91', dash: '6 3'},
  ];
  try {
    const response = await fetch('headline.json', {cache: 'no-cache'});
    if (!response.ok) throw new Error(`Data request failed (${response.status})`);
    const data = await response.json();
    if (!Array.isArray(data.rows) || !data.rows.length) throw new Error('No observations available');
    const names = new Map(data.rows.map(r => [r.code, r.entity]));
    const countries = [...new Map(data.rows.filter(r => r.kind === 'country').map(r => [r.code, r.entity]))]
      .sort((a, b) => a[1].localeCompare(b[1]));
    const options = [...new Map(data.rows.filter(r => r.value != null).map(r => [r.series, r.entity]))]
      .sort((a, b) => a[1].localeCompare(b[1]));
    const optionCodes = new Set(options.map(([code]) => code));
    const countryCodes = new Set(countries.map(([code]) => code));
    const urlAliases = {
      best: 'frontier', world: 'OWID_WRL',
      'wb-high': 'OWID_HIC', 'wb-upper-middle': 'OWID_UMC',
      'wb-lower-middle': 'OWID_LMC', 'wb-low': 'OWID_LIC',
      'un-africa': 'UN_AFR', 'un-asia': 'UN_ASI', 'un-europe': 'UN_EUR',
      'un-latin-america-caribbean': 'UN_LAC', 'un-northern-america': 'UN_NAM',
      'un-oceania': 'UN_OCE',
    };
    const urlNames = new Map(Object.entries(urlAliases).map(([alias, code]) => [code, alias]));
    const embedded = window.self !== window.top;
    const params = new URLSearchParams(location.search);
    const hasUrlState = ['sex', 'show', 'lines', 'country', 'start', 'end'].some(key => params.has(key));
    let startYear = data.start_year, endYear = data.end_year;
    function readYears(query) {
      const parse = (key, fallback) => /^\d{4}$/.test(query.get(key) || '') ? Math.max(data.start_year, Math.min(data.end_year, Number(query.get(key)))) : fallback;
      startYear = parse('start', data.start_year); endYear = parse('end', data.end_year);
      if (startYear >= endYear) { startYear = data.start_year; endYear = data.end_year; }
    }
    readYears(params);
    function readUrlState() {
      const query = new URLSearchParams(location.search);
      readYears(query);
      const sex = ['m', 'male'].includes(query.get('sex')) ? 'male' : 'female';
      document.querySelector(`input[name="sex"][value="${sex}"]`).checked = true;
      const selectionKey = query.has('show') ? 'show' : 'lines';
      if (query.has(selectionKey)) return query.getAll(selectionKey)
        .flatMap(value => value.split(','))
        .map(code => Object.hasOwn(urlAliases, code) ? urlAliases[code] : code)
        .filter(code => optionCodes.has(code));
      return [...referenceSeries.map(s => s.id), ...query.getAll('country').flatMap(value => value.split(',')).filter(code => countryCodes.has(code))];
    }
    let saved = null;
    try {
      if (!embedded && !hasUrlState) {
      const value = embedded || hasUrlState ? null : localStorage.getItem('headline-lines');
      if (value !== null) {
        const parsed = JSON.parse(value);
        if (Array.isArray(parsed)) saved = parsed.filter(code => optionCodes.has(code));
      } else {
        const multiple = localStorage.getItem('headline-countries');
        if (multiple !== null) {
          const parsed = JSON.parse(multiple);
          if (Array.isArray(parsed)) saved = [...referenceSeries.map(s => s.id), ...parsed.filter(code => countryCodes.has(code))];
        }
        const legacy = localStorage.getItem('headline-country');
        if (saved === null && (legacy === '' || countryCodes.has(legacy))) saved = [...referenceSeries.map(s => s.id), ...(legacy ? [legacy] : [])];
      }
      }
    } catch (_) { /* Storage may be unavailable. */ }
    const selectedCountries = new Set(hasUrlState ? readUrlState() : saved || referenceSeries.map(s => s.id));
    if (saved === null && !embedded && !hasUrlState) {
      try {
        const regionNames = new Intl.DisplayNames(['en'], {type: 'region'});
        for (const language of navigator.languages || [navigator.language]) {
          const region = new Intl.Locale(language).region;
          if (!region) continue;
          const match = countries.find(([, name]) => name === regionNames.of(region));
          if (match) { selectedCountries.add(match[0]); break; }
        }
      } catch (_) { /* Manual selection remains available. */ }
    }
    const baselineLines = new Set(['frontier', 'OWID_WRL']);
    const countryList = document.getElementById('country-list');
    const countrySearch = document.getElementById('country-search');
    if (window.matchMedia('(max-width: 600px)').matches) document.getElementById('country-picker').open = false;
    let pickerFilter = 'all';
    function updateCountries() {
      for (const code of baselineLines) selectedCountries.add(code);
      const seenSections = new Set();
      for (const label of countryList.children) {
        const matchesFilter = pickerFilter === 'all'
          || (pickerFilter === 'selected' && selectedCountries.has(label.dataset.code))
          || label.dataset.category === pickerFilter;
        label.hidden = !matchesFilter || !label.textContent.toLocaleLowerCase().includes(countrySearch.value.trim().toLocaleLowerCase());
        label.querySelector('input').checked = selectedCountries.has(label.dataset.code);
        delete label.dataset.section;
        if (!label.hidden && !seenSections.has(label.dataset.group)) {
          label.dataset.section = label.dataset.group;
          seenSections.add(label.dataset.group);
        }
      }
      const selectionCount = [...selectedCountries].filter(code => !baselineLines.has(code)).length;
      document.getElementById('selection-count').textContent = `${selectionCount} comparisons selected`;
      document.getElementById('clear-countries').disabled = selectionCount === 0;
      document.getElementById('country-empty').hidden = [...countryList.children].some(label => !label.hidden);
    }
    function syncUrl() {
      const url = new URL(location.href);
      for (const code of baselineLines) selectedCountries.add(code);
      url.searchParams.set('sex', document.querySelector('input[name="sex"]:checked').value === 'male' ? 'm' : 'f');
      url.searchParams.set('show', [...selectedCountries].map(code => urlNames.get(code) || code).join(','));
      url.searchParams.delete('lines');
      url.searchParams.delete('country');
      url.searchParams.set('start', startYear);
      url.searchParams.set('end', endYear);
      history.replaceState(null, '', url);
    }
    function saveCountries() {
      syncUrl();
      try { if (!embedded) localStorage.setItem('headline-lines', JSON.stringify([...selectedCountries])); } catch (_) { /* Optional persistence. */ }
      updateCountries();
      render();
    }
    // Sort the picker separately so existing series colors remain stable.
    const incomeGroups = new Set(['OWID_HIC', 'OWID_UMC', 'OWID_LMC', 'OWID_LIC']);
    const pickerRank = code => /^p\d+$/.test(code) ? 0 : countryCodes.has(code) ? 2 : 1;
    const pickerOptions = [...options].sort((a, b) => pickerRank(a[0]) - pickerRank(b[0])
      || (pickerRank(a[0]) === 0 ? Number(a[0].slice(1)) - Number(b[0].slice(1)) : a[1].localeCompare(b[1])));
    for (const [code, name] of pickerOptions) {
      if (baselineLines.has(code) || incomeGroups.has(code)) continue;
      const label = document.createElement('label'); label.dataset.code = code;
      const input = document.createElement('input'); input.type = 'checkbox'; input.value = code;
      input.setAttribute('aria-label', name.replace(' (population-weighted)', ''));
      input.addEventListener('change', () => {
        if (input.checked) selectedCountries.add(code); else selectedCountries.delete(code);
        saveCountries();
      });
      const category = countryCodes.has(code) ? 'Country' : /^p\d+$/.test(code) ? 'Percentile' : code === 'frontier' ? 'Reference' : 'Group';
      label.dataset.category = category === 'Percentile' ? 'percentiles' : category === 'Country' ? 'countries' : 'regions';
      label.dataset.group = category === 'Percentile' ? 'Percentiles' : category === 'Country' ? 'Countries' : 'Regions';
      const optionName = document.createElement('span'); optionName.className = 'option-name'; optionName.textContent = name.replace(' (population-weighted)', '');
      label.append(input, optionName); countryList.append(label);
    }
    countrySearch.addEventListener('input', updateCountries);
    document.querySelectorAll('[data-picker-filter]').forEach(button => {
      button.addEventListener('click', () => {
        pickerFilter = button.dataset.pickerFilter;
        document.querySelectorAll('[data-picker-filter]').forEach(item => item.setAttribute('aria-pressed', String(item === button)));
        updateCountries(); countryList.scrollTop = 0;
      });
    });
    document.getElementById('clear-countries').addEventListener('click', () => {
      selectedCountries.clear();
      selectedCountries.add('frontier');
      selectedCountries.add('OWID_WRL');
      saveCountries();
    });
    function lineStyle(code) {
      const reference = referenceSeries.find(item => item.id === code);
      if (reference) return reference;
      const position = (.42 + options.findIndex(([id]) => id === code) * .618034) % 1;
      const color = d3.hsl(d3.interpolateRainbow(position));
      // Keep the original hue variety, darkening only pale yellow/green shades.
      if (color.h >= 55 && color.h <= 115) color.l = Math.min(color.l, .38);
      return {id: code, name: names.get(code), color: color.formatHex(), dash: /^p\d+$/.test(code) ? '6 3' : null};
    }
    updateCountries();
    const startControl = document.getElementById('start-year');
    const endControl = document.getElementById('end-year');
    for (const input of [startControl, endControl]) { input.min = data.start_year; input.max = data.end_year; }
    const presets = [data.start_year, 1950, 2000, data.end_year].filter((year, i, all) => year >= data.start_year && year <= data.end_year && all.indexOf(year) === i);
    const marks = document.getElementById('year-marks');
    presets.forEach(year => { const option = document.createElement('option'); option.value = year; marks.append(option); });
    function moveNearestHandle(year) {
      const control = Math.abs(year - startYear) <= Math.abs(year - endYear) ? startControl : endControl;
      if (control === startControl) startYear = Math.min(year, endYear - 1);
      else endYear = Math.max(year, startYear + 1);
      changeYears();
      control.focus({preventScroll: true});
    }
    document.getElementById('timeline').addEventListener('click', event => {
      // Handle drags and preset buttons have their own handlers.
      if (event.target.closest('input, button')) return;
      const bounds = event.currentTarget.getBoundingClientRect();
      if (event.clientY - bounds.top > 36) return;
      const fraction = Math.max(0, Math.min(1, (event.clientX - bounds.left) / bounds.width));
      let year = Math.round(data.start_year + fraction * (data.end_year - data.start_year));
      const nearest = presets.reduce((a, b) => Math.abs(b - year) < Math.abs(a - year) ? b : a);
      if (Math.abs(nearest - year) * bounds.width / (data.end_year - data.start_year) <= 8) year = nearest;
      moveNearestHandle(year);
    });
    presets.forEach(year => {
      const button = document.createElement('button');
      button.type = 'button'; button.className = 'year-marker'; button.textContent = year;
      button.setAttribute('aria-label', `Move nearest range handle to ${year}`);
      button.style.left = `${100 * (year - data.start_year) / (data.end_year - data.start_year)}%`;
      button.addEventListener('click', () => {
        moveNearestHandle(year);
      });
      document.getElementById('timeline').append(button);
    });
    // Snap on pointer release, but retain single-year keyboard adjustments.
    for (const control of [startControl, endControl]) {
      control.addEventListener('pointerup', () => {
        const year = Number(control.value);
        const nearest = presets.reduce((a, b) => Math.abs(b - year) < Math.abs(a - year) ? b : a);
        const pixelsPerYear = document.getElementById('timeline').clientWidth / (data.end_year - data.start_year);
        if (Math.abs(nearest - year) * pixelsPerYear <= 8) {
          if (control === startControl) startYear = Math.min(nearest, endYear - 1);
          else endYear = Math.max(nearest, startYear + 1);
          changeYears();
        }
      });
    }
    function changeYears() { syncUrl(); render(); }
    for (const [id, isStart] of [['start-value', true], ['end-value', false]]) {
      const field = document.getElementById(id);
      function commitYear() {
        if (/^\d{4}$/.test(field.value.trim())) {
          const year = Math.max(data.start_year, Math.min(data.end_year, Number(field.value)));
          if (isStart) startYear = Math.min(year, endYear - 1);
          else endYear = Math.max(year, startYear + 1);
          changeYears();
        } else {
          field.value = isStart ? startYear : endYear;
        }
      }
      field.addEventListener('change', commitYear);
      field.addEventListener('keydown', event => {
        if (event.key === 'Enter') { event.preventDefault(); commitYear(); }
        if (event.key === 'Escape') field.value = isStart ? startYear : endYear;
      });
    }
    startControl.addEventListener('input', () => { startYear = Math.min(Number(startControl.value), endYear - 1); changeYears(); });
    endControl.addEventListener('input', () => { endYear = Math.max(Number(endControl.value), startYear + 1); changeYears(); });
    function resetRange() {
      startYear = data.start_year; endYear = data.end_year; changeYears();
    }
    document.getElementById('timeline').addEventListener('dblclick', event => {
      if (event.target.closest('button')) return;
      event.preventDefault();
      resetRange();
    });
    for (const control of [startControl, endControl]) {
      control.addEventListener('keydown', event => {
        if (event.key === 'Escape') { event.preventDefault(); resetRange(); }
      });
    }
    const format = value => value == null ? '—' : value.toFixed(2);
    const countryCountSubtitle = d3.select('#country-count-subtitle');
    document.getElementById('source').textContent = `${data.source}, via Our World in Data`;
    const credits = document.getElementById('data-source-credits');
    for (const source of data.sources || []) {
      const entry = document.createElement('li');
      const link = document.createElement('a');
      link.href = source.originalChartUrl; link.textContent = source.title;
      entry.append(link, document.createTextNode(` — ${source.citation}; via Our World in Data.`));
      credits.append(entry);
    }
    function render() {
      const years = d3.range(startYear, endYear + 1);
      startControl.value = startYear; endControl.value = endYear;
      const timeline = document.getElementById('timeline');
      timeline.style.setProperty('--range-start', `${100 * (startYear - data.start_year) / (data.end_year - data.start_year)}%`);
      timeline.style.setProperty('--range-end', `${100 * (endYear - data.start_year) / (data.end_year - data.start_year)}%`);
      document.getElementById('start-value').value = startYear;
      document.getElementById('end-value').value = endYear;
      const series = [...selectedCountries].map(lineStyle);
      const tableHead = document.querySelector('#table-caption + thead tr');
      tableHead.replaceChildren();
      for (const name of ['Year', ...series.map(item => item.name)]) {
        const th = document.createElement('th'); th.scope = 'col'; th.textContent = name; tableHead.append(th);
      }
      let hoveredSeries = null;
      let focusedSeries = null;
      const sex = document.querySelector('input[name="sex"]:checked').value;
      const title = `${startYear === data.start_year && endYear === data.end_year ? 'A century of unequal progress' : 'Unequal progress'}: life expectancy at birth for ${sex === 'female' ? 'women' : 'men'}, ${startYear}–${endYear}`;
      document.getElementById('chart-title').textContent = title;
      document.title = `${title} · Our Longevity in Data`;
      const rows = data.rows.filter(r => r.sex === sex);
      if (!rows.some(r => r.value != null)) {
        status.hidden = false;
        countryCountSubtitle.text('0');
        container.replaceChildren();
        document.getElementById('values').replaceChildren();
        status.textContent = 'No observations available for this selection.';
        return;
      }
      const grouped = d3.group(rows, r => r.series);
      const latestCountries = rows.filter(r => r.kind === 'country' && r.year === data.end_year);
      const totalPopulation = d3.sum(latestCountries, r => r.population);
      const populationShares = new Map(latestCountries.map(r => [r.code, r.population / totalPopulation]));
      // Square-root scaling keeps small countries visible without overwhelming the summaries.
      const countryOpacity = share => .06 + .44 * Math.sqrt(Math.min(share, .2) / .2);
      const opacityKey = d3.select('#opacity-samples');
      opacityKey.selectAll('*').remove();
      for (const percent of [.1, 1, 5, 20]) {
        const sample = opacityKey.append('li');
        sample.append('span').attr('aria-hidden', 'true').style('opacity', countryOpacity(percent / 100));
        sample.append('b').style('font-weight', 'normal').text(`${percent}%`);
      }
      const summary = new Map(series.map(s => [s.id, new Map((grouped.get(s.id) || []).map(r => [r.year, r]))]));
      const width = Math.max(280, container.clientWidth), height = width < 600 ? 350 : 480;
      const mobile = width < 600;
      const margin = {left: 64, right: mobile ? 128 : 175, top: 24, bottom: 35};
      const rangeEditor = document.querySelector('.range-editor');
      const displayScale = container.clientWidth / width;
      rangeEditor.style.setProperty('--plot-left', `${margin.left * displayScale}px`);
      rangeEditor.style.setProperty('--plot-right', `${margin.right * displayScale}px`);
      const x = d3.scaleLinear().domain([startYear, endYear]).range([margin.left, width - margin.right]);
      const y = d3.scaleLinear().domain([0, Math.ceil(d3.max(data.rows, r => r.value) / 10) * 10]).range([height - margin.bottom, margin.top]);
      container.replaceChildren();
      const svg = d3.select(container).append('svg').attr('viewBox', `0 0 ${width} ${height}`)
        .attr('role', 'group').attr('aria-label', `${sex === 'female' ? 'Female' : 'Male'} life expectancy at birth, ${startYear}–${endYear}. Annual highlighted values are available in the table below.`);
      svg.append('g').attr('transform', `translate(${margin.left},0)`)
        .call(d3.axisLeft(y)
          .tickValues(d3.range(0, y.domain()[1] + 1, 10))
          .tickFormat(value => value === 0 ? '' : d3.format('d')(value))
          .tickSize(-(width - margin.left - margin.right)))
        .call(g => g.select('.domain').remove())
        .call(g => g.selectAll('.tick line').attr('stroke', '#edf0f2').attr('stroke-width', 1));
      // Leave at least 40px between four-digit year labels, using whole decades.
      const plotWidth = width - margin.left - margin.right;
      const minimumStep = (endYear - startYear) * 40 / plotWidth;
      const yearStep = [1, 2, 5, 10, 20, 30, 50, 100].find(step => step >= minimumStep) || 100;
      svg.append('g').attr('transform', `translate(0,${height - margin.bottom})`)
        .call(d3.axisBottom(x)
          .tickValues(d3.range(startYear, endYear + 1, yearStep))
          .tickFormat(d3.format('d')));
      svg.append('text')
        .attr('transform', `translate(16,${(margin.top + height - margin.bottom) / 2}) rotate(-90)`)
        .attr('text-anchor', 'middle').attr('font-size', 12).attr('fill', '#536372')
        .text('Life expectancy at birth (years)');
      if (startYear < 1950) svg.append('text').attr('x', (x(startYear) + x(Math.min(1950, endYear))) / 2)
        .attr('y', y(85)).attr('dominant-baseline', 'middle')
        .attr('text-anchor', 'middle').attr('font-size', 11)
        .attr('fill', '#65727d').text('Before 1950: limited country coverage');
      if (startYear <= 1950 && endYear >= 1950) svg.append('line').attr('x1', x(1950)).attr('x2', x(1950))
        .attr('y1', margin.top).attr('y2', height - margin.bottom)
        .attr('stroke', '#65727d').attr('stroke-width', 1.5)
        .attr('stroke-dasharray', '1 5').attr('stroke-linecap', 'round')
        .append('title').text('1950: broader country coverage begins');
      const line = d3.line().defined(r => r.value != null).x(r => x(r.year)).y(r => y(r.value));
      function points(values) {
        const byYear = new Map(values.map(r => [r.year, r]));
        return years.map(year => byYear.get(year) || {year, value: null});
      }
      for (const values of grouped.values()) {
        if (values[0].kind !== 'country' || selectedCountries.has(values[0].code)) continue;
        svg.append('path').attr('class', 'country-line').datum(points(values)).attr('d', line).attr('fill', 'none')
          .attr('stroke', '#75818b')
          .attr('stroke-opacity', countryOpacity(populationShares.get(values[0].code) || 0))
          .attr('stroke-width', 1)
          .append('title').text(values[0].entity);
      }
      for (const item of series) {
        svg.append('path').attr('class', 'summary-line').attr('data-series', item.id).datum(points(grouped.get(item.id) || [])).attr('d', line)
          .attr('fill', 'none').attr('stroke', item.color).attr('stroke-width', 2.7).attr('stroke-dasharray', item.dash || null)
          .on('pointerenter', event => { if (event.pointerType !== 'touch') { hoveredSeries = item.id; updateHighlight(); } })
          .on('pointerleave', () => { hoveredSeries = null; updateHighlight(); })
          .append('title').text(item.name);
      }
      const endpointCandidates = series.flatMap(item => {
        const last = (grouped.get(item.id) || []).filter(r => r.value != null && r.year >= startYear && r.year <= endYear)
          .sort((a, b) => a.year - b.year).at(-1);
        if (!last) return [];
        const words = item.name.replace(' (population-weighted)', '').split(' ');
        const lines = [];
        for (const word of words) {
          if (!lines.length || (lines.at(-1) + ' ' + word).length > (mobile ? 16 : 23)) lines.push(word);
          else lines[lines.length - 1] += ' ' + word;
        }
        return [{...item, last, lines, anchorY: y(last.value), labelHeight: Math.max(18, lines.length * 14)}];
      });
      const endpointLayer = svg.append('g');
      let labelPointer = null;
      const isReference = item => baselineLines.has(item.id) || /^p\d+$/.test(item.id);
      function visibleEndpointLabels(active) {
        const available = endpointCandidates.filter(item => !labelPointer
          || summary.get(item.id).get(labelPointer.year)?.value != null || item.id === active);
        const priority = item => isReference(item) || item.id === active;
        const distance = item => labelPointer
          ? Math.abs(y(summary.get(item.id).get(labelPointer.year)?.value ?? item.last.value) - labelPointer.y)
          : -item.last.value;
        const references = available.filter(priority);
        const nearby = available.filter(item => !priority(item)).sort((a, b) => distance(a) - distance(b));
        const limit = Math.max(mobile ? 4 : 10, references.length);
        const chosen = [...references, ...nearby.slice(0, Math.max(0, limit - references.length))]
          .map(item => ({...item})).sort((a, b) => a.anchorY - b.anchorY);
        let bottom = margin.top;
        for (const item of chosen) {
          item.labelY = Math.max(item.anchorY, bottom + item.labelHeight / 2);
          bottom = item.labelY + item.labelHeight / 2 + 6;
        }
        let top = height - margin.bottom;
        for (const item of [...chosen].reverse()) {
          item.labelY = Math.min(item.labelY, top - item.labelHeight / 2);
          top = item.labelY - item.labelHeight / 2 - 6;
        }
        return chosen;
      }
      function drawEndpointLabels(active) {
        const labelX = width - margin.right + 18;
        const labels = endpointLayer.selectAll('.line-label').data(visibleEndpointLabels(active), item => item.id)
          .join(enter => {
            const group = enter.append('g').attr('class', 'line-label').attr('tabindex', 0).attr('role', 'group');
            group.append('path').attr('class', 'label-connector').attr('fill', 'none').attr('stroke-width', 1).attr('stroke-opacity', .5);
            group.append('rect').attr('fill', 'transparent');
            group.append('text').attr('font-size', 12).attr('font-weight', 400).attr('dominant-baseline', 'middle');
            group.on('pointerenter', (event, item) => { if (event.pointerType !== 'touch') { hoveredSeries = item.id; updateHighlight(); } })
              .on('pointerleave', () => { hoveredSeries = null; updateHighlight(); })
              .on('focus', function (event, item) { focusedSeries = this.matches(':focus-visible') ? item.id : null; updateHighlight(); })
              .on('blur', () => { focusedSeries = null; updateHighlight(); });
            return group;
          });
        labels.attr('data-series', item => item.id).attr('aria-label', item => item.name);
        labels.select('path').attr('data-series', item => item.id).attr('stroke', item => item.color)
          .attr('d', item => `M${x(item.last.year)},${y(item.last.value)}L${labelX - 5},${item.labelY}`);
        labels.select('rect').attr('x', labelX - 4).attr('y', item => item.labelY - item.labelHeight / 2)
          .attr('width', margin.right - 16).attr('height', item => item.labelHeight);
        labels.select('text').attr('x', labelX).attr('y', item => item.labelY).attr('fill', item => item.color)
          .each(function (item) {
            const text = d3.select(this); text.selectAll('*').remove();
            item.lines.forEach((line, i) => text.append('tspan').attr('x', labelX)
              .attr('dy', i ? '1.1em' : `${-(item.lines.length - 1) * .55}em`).text(line));
            text.append('title').text(`${item.name}: ${format(item.last.value)} (${item.last.year})`);
          });
      }
      function updateHighlight() {
        const selectedSeries = hoveredSeries || focusedSeries;
        drawEndpointLabels(selectedSeries);
        svg.selectAll('.country-line').attr('opacity', selectedSeries ? .15 : 1);
        svg.selectAll('.summary-line, .line-label').attr('opacity', function () {
          return !selectedSeries || this.getAttribute('data-series') === selectedSeries ? 1 : .15;
        });
        svg.selectAll('.summary-line').attr('stroke-width', function () {
          return this.getAttribute('data-series') === selectedSeries ? 4 : 2.7;
        });

      }
      updateHighlight();
      const tooltip = d3.select(container).append('div').attr('id', 'tooltip').attr('hidden', true);
      const guide = svg.append('line').attr('y1', margin.top).attr('y2', height - margin.bottom)
        .attr('stroke', '#536372').attr('stroke-dasharray', '3 3').attr('visibility', 'hidden');
      let pinned = false;
      let lastHover = [margin.left, margin.top];
      const popupLimit = Math.min(320, height - margin.top - margin.bottom, window.innerHeight * .5);
      tooltip.style('--tooltip-limit', `${popupLimit}px`);
      function closeTooltip() {
        pinned = false;
        tooltip.attr('hidden', true).classed('is-pinned', false);
        guide.attr('visibility', 'hidden');
        labelPointer = null;
        drawEndpointLabels(hoveredSeries || focusedSeries);
      }
      function showTooltip(event, pin = false) {
        if (pinned && !pin) return;
        const [px, py] = event ? d3.pointer(event, svg.node()) : lastHover;
        if (px > width - margin.right || px < margin.left || py < margin.top || py > height - margin.bottom) {
          if (!pinned) closeTooltip();
          return;
        }
        lastHover = [px, py];
        pinned = pin;
        const year = Math.max(startYear, Math.min(endYear, Math.round(x.invert(px))));
        guide.attr('x1', x(year)).attr('x2', x(year)).attr('visibility', 'visible');
        labelPointer = {year, y: py};
        drawEndpointLabels(hoveredSeries || focusedSeries);
        tooltip.attr('hidden', null).classed('is-pinned', pin).attr('role', pin ? 'dialog' : 'tooltip')
          .attr('aria-label', `Life expectancy in ${year}`);
        tooltip.selectAll('*').remove();
        const heading = tooltip.append('div').attr('class', 'tooltip-heading');
        heading.append('span').text(year);
        if (pin) heading.append('button').attr('type', 'button').attr('aria-label', 'Close year values')
          .text('×').on('click', () => { closeTooltip(); svg.node().focus({preventScroll: true}); });
        const contents = tooltip.append('div').attr('class', 'tooltip-contents');
        const entries = series.map(item => ({...item, row: summary.get(item.id).get(year)}))
          .sort((a, b) => (b.row?.value ?? -Infinity) - (a.row?.value ?? -Infinity));
        function addRows(items) {
          contents.selectAll('*').remove();
          for (const item of items) {
            const entry = contents.append('div').attr('class', 'tooltip-row');
            entry.append('span').attr('class', 'tooltip-swatch').style('background', item.color);
            const label = entry.append('span').attr('class', 'tooltip-name')
              .text(item.name.replace(' (population-weighted)', ''));
            if (item.row?.winners?.length) label.append('small').text(item.row.winners.map(id => names.get(id) || id).join(', '));
            entry.append('strong').text(item.row?.value == null ? 'No data' : `${item.row.value.toFixed(1)} years`);
          }
        }
        const availableEntries = entries.filter(item => item.row?.value != null);
        let visibleEntries = availableEntries;
        if (!pin && availableEntries.length > 10) {
          const references = availableEntries.filter(isReference);
          const nearby = availableEntries.filter(item => !isReference(item)).sort((a, b) => {
            const distance = item => item.row?.value == null ? Infinity : Math.abs(y(item.row.value) - py);
            return distance(a) - distance(b);
          });
          const visibleIds = new Set([...references, ...nearby.slice(0, Math.max(0, 10 - references.length))]
            .map(item => item.id));
          // Preserve value order after choosing nearby lines and mandatory references.
          visibleEntries = availableEntries.filter(item => visibleIds.has(item.id));
        }
        addRows(visibleEntries);
        if (visibleEntries.length < availableEntries.length) {
          tooltip.append('div').attr('class', 'tooltip-hint')
            .text(`+${availableEntries.length - visibleEntries.length} more · Click or tap for all selected values`);
        }
        if (pin) {
          tooltip.style('left', `${Math.max(0, width - margin.right - Math.min(300, width))}px`)
            .style('top', `${margin.top}px`);
          contents.attr('tabindex', 0).attr('role', 'region').attr('aria-label', 'Scrollable selected values');
          heading.select('button').node().focus({preventScroll: true});
          return;
        }
        const [mouseX, mouseY] = event ? d3.pointer(event, container) : lastHover;
        const box = tooltip.node().getBoundingClientRect();
        let left = mouseX - box.width - 20;
        if (left < 0) left = mouseX + 20;
        let top = mouseY - 16;
        if (top + box.height > container.clientHeight) top = mouseY - box.height - 16;
        tooltip.style('left', `${Math.max(0, Math.min(container.clientWidth - box.width, left))}px`)
          .style('top', `${Math.max(0, Math.min(container.clientHeight - box.height, top))}px`);

      }
      svg.attr('tabindex', 0).on('pointermove', event => showTooltip(event))
        .on('pointerleave', () => { if (!pinned) closeTooltip(); })
        .on('click', event => showTooltip(event, true))
        .on('keydown', event => {
          if (event.key === 'Enter') { event.preventDefault(); showTooltip(null, true); }
        });
      d3.select(window).on('keydown.headlineTooltip', event => {
        if (event.key === 'Escape' && pinned) {
          closeTooltip(); svg.node().focus({preventScroll: true});
        }
      });
      const body = document.getElementById('values');
      body.replaceChildren();
      for (const year of [...years].reverse()) {
        const tr = document.createElement('tr');
        const th = document.createElement('th'); th.scope = 'row'; th.textContent = year; tr.append(th);
        for (const item of series) {
          const row = summary.get(item.id).get(year);
          const td = document.createElement('td');
          td.textContent = format(row?.value) + (row?.winners?.length ? ` (${row.winners.map(id => names.get(id) || id).join(', ')})` : '');
          tr.append(td);
        }
        body.append(tr);
      }
      document.getElementById('table-caption').textContent = `${sex === 'female' ? 'Female' : 'Male'} life expectancy at birth, in years`;
      countryCountSubtitle.text(new Set(rows.filter(r => r.kind === 'country').map(r => r.code)).size);
      status.hidden = true;
    }
    document.querySelectorAll('input[name="sex"]').forEach(input => input.addEventListener('change', () => { syncUrl(); render(); }));
    window.addEventListener('popstate', () => {
      selectedCountries.clear();
      readUrlState().forEach(code => selectedCountries.add(code));
      updateCountries();
      render();
    });
    // Serialize resolved preferences too, so copying this URL reproduces the view.
    syncUrl();
    let lastWidth;
    new ResizeObserver(entries => {
      const width = Math.round(entries[0].contentRect.width);
      if (width !== lastWidth) { lastWidth = width; render(); }
    }).observe(container);
    render();
  } catch (error) {
    container.replaceChildren();
    status.hidden = false;
    status.textContent = `Unable to display the chart: ${error.message}. Try reloading, or download the data below.`;
  }
})();
