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
    const embedded = window.self !== window.top;
    const params = new URLSearchParams(location.search);
    const hasUrlState = ['sex', 'lines', 'country', 'start', 'end'].some(key => params.has(key));
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
      const sex = query.get('sex') === 'male' ? 'male' : 'female';
      document.querySelector(`input[name="sex"][value="${sex}"]`).checked = true;
      if (query.has('lines')) return query.getAll('lines').flatMap(value => value.split(',')).filter(code => optionCodes.has(code));
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
    const countryList = document.getElementById('country-list');
    const countrySearch = document.getElementById('country-search');
    function updateCountries() {
      const entries = document.getElementById('selected-lines');
      entries.replaceChildren();
      for (const code of selectedCountries) {
        const li = document.createElement('li');
        const swatch = document.createElement('span'); swatch.className = 'selected-swatch'; swatch.style.background = lineStyle(code).color;
        const text = document.createElement('span'); text.textContent = names.get(code);
        const remove = document.createElement('button'); remove.type = 'button'; remove.textContent = '×';
        remove.setAttribute('aria-label', `Remove ${names.get(code)}`);
        remove.addEventListener('click', () => { selectedCountries.delete(code); saveCountries(); document.getElementById('country-count').focus(); });
        li.append(swatch, text, remove); entries.append(li);
      }
      for (const label of countryList.children) {
        label.hidden = !label.textContent.toLocaleLowerCase().includes(countrySearch.value.trim().toLocaleLowerCase());
        label.querySelector('input').checked = selectedCountries.has(label.dataset.code);
      }
      document.getElementById('country-empty').hidden = [...countryList.children].some(label => !label.hidden);
    }
    function syncUrl() {
      const url = new URL(location.href);
      url.searchParams.set('sex', document.querySelector('input[name="sex"]:checked').value);
      url.searchParams.set('lines', [...selectedCountries].join(','));
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
    for (const [code, name] of options) {
      const label = document.createElement('label'); label.dataset.code = code;
      const input = document.createElement('input'); input.type = 'checkbox'; input.value = code;
      input.addEventListener('change', () => {
        if (input.checked) selectedCountries.add(code); else selectedCountries.delete(code);
        saveCountries();
      });
      const category = countryCodes.has(code) ? 'Country' : /^p\d+$/.test(code) ? 'Percentile' : code === 'frontier' ? 'Reference' : 'Group';
      label.append(input, document.createTextNode(`${name} · ${category}`)); countryList.append(label);
    }
    countrySearch.addEventListener('input', updateCountries);
    document.getElementById('clear-countries').addEventListener('click', () => { selectedCountries.clear(); saveCountries(); });
    document.getElementById('country-picker').addEventListener('keydown', event => {
      if (event.key === 'Escape') { event.currentTarget.open = false; document.getElementById('country-count').focus(); }
    });
    function lineStyle(code) {
      const reference = referenceSeries.find(item => item.id === code);
      return reference || {id: code, name: names.get(code), color: d3.interpolateRainbow((.42 + options.findIndex(([id]) => id === code) * .618034) % 1), dash: /^p\d+$/.test(code) ? '6 3' : null};
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
    const countryLegendLabel = d3.select('#country-key');
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
        countryLegendLabel.text('One line per country or territory (0)');
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
      const width = Math.max(280, container.clientWidth), height = Math.max(width < 600 ? 350 : 480, series.length * 42 + 80);
      const margin = {left: 64, right: width < 600 ? 112 : 175, top: 24, bottom: 35};
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
        .call(g => g.selectAll('.tick line').attr('stroke', '#e7ebee'));
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
          .append('title').text(item.name);
      }
      // Separate endpoint labels vertically while preserving their value order.
      const labels = series.flatMap(item => {
        const last = (grouped.get(item.id) || []).filter(r => r.value != null && r.year >= startYear && r.year <= endYear).sort((a, b) => a.year - b.year).at(-1);
        return last ? [{...item, last, labelY: y(last.value)}] : [];
      }).sort((a, b) => a.labelY - b.labelY);
      const labelGap = width < 600 ? 32 : 25;
      labels.forEach((item, i) => {
        item.labelY = Math.max(item.labelY, i ? labels[i - 1].labelY + labelGap : margin.top);
      });
      if (labels.length) {
        const overflow = Math.max(0, labels.at(-1).labelY - (height - margin.bottom - 12));
        labels.forEach(item => { item.labelY -= overflow; });
      }
      const labelX = width - margin.right + 18;
      for (const item of labels) {
        svg.append('path').attr('class', 'label-connector').attr('data-series', item.id).attr('d', `M${x(item.last.year)},${y(item.last.value)}L${labelX - 5},${item.labelY}`)
          .attr('fill', 'none').attr('stroke', item.color).attr('stroke-width', 1).attr('stroke-opacity', .65);
        const button = svg.append('g').attr('class', 'line-label').attr('data-series', item.id)
          .attr('role', 'group').attr('tabindex', 0).attr('aria-label', item.name)
          .on('pointerenter', () => { hoveredSeries = item.id; updateHighlight(); })
          .on('pointerleave', () => { hoveredSeries = null; updateHighlight(); })
          .on('focus', function () {
            focusedSeries = this.matches(':focus-visible') ? item.id : null;
            updateHighlight();
          })
          .on('blur', () => { focusedSeries = null; updateHighlight(); });
        button.append('rect').attr('x', labelX - 4).attr('y', item.labelY - labelGap / 2)
          .attr('width', margin.right - 16).attr('height', labelGap).attr('fill', 'transparent');
        const text = button.append('text').attr('x', labelX).attr('y', item.labelY)
          .attr('fill', item.color).attr('font-size', width < 600 ? 10 : 12).attr('font-weight', 400)
          .attr('dominant-baseline', 'middle');
        const words = item.name.replace(' (population-weighted)', '').split(' ');
        const lines = [];
        for (const word of words) {
          if (!lines.length || (lines.at(-1) + ' ' + word).length > (width < 600 ? 16 : 23)) lines.push(word);
          else lines[lines.length - 1] += ' ' + word;
        }
        lines.forEach((line, i) => text.append('tspan').attr('x', labelX)
          .attr('dy', i ? '1.1em' : `${-(lines.length - 1) * .55}em`).text(line));
        text.append('title').text(`${item.name}: ${format(item.last.value)} (${item.last.year})`);
      }
      function updateHighlight() {
        const selectedSeries = hoveredSeries || focusedSeries;
        svg.selectAll('.country-line').attr('opacity', selectedSeries ? .15 : 1);
        svg.selectAll('.summary-line, .label-connector').attr('opacity', function () {
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
      svg.on('pointermove', event => {
        const [px, py] = d3.pointer(event, svg.node());
        if (px > width - margin.right || px < margin.left || py < margin.top || py > height - margin.bottom) {
          tooltip.attr('hidden', true); guide.attr('visibility', 'hidden'); return;
        }
        const year = Math.max(startYear, Math.min(endYear, Math.round(x.invert(px))));
        guide.attr('x1', x(year)).attr('x2', x(year)).attr('visibility', 'visible');
        tooltip.attr('hidden', null);
        tooltip.selectAll('*').remove();
        tooltip.append('div').attr('class', 'tooltip-heading').text(year);
        const entries = series.map(item => ({...item, row: summary.get(item.id).get(year)}))
          .sort((a, b) => (b.row?.value ?? -Infinity) - (a.row?.value ?? -Infinity));
        for (const item of entries) {
          const entry = tooltip.append('div').attr('class', 'tooltip-row');
          entry.append('span').attr('class', 'tooltip-swatch').style('background', item.color);
          const label = entry.append('span').attr('class', 'tooltip-name')
            .text(item.name.replace(' (population-weighted)', ''));
          if (item.row?.winners?.length) label.append('small').text(item.row.winners.map(id => names.get(id) || id).join(', '));
          entry.append('strong').text(item.row?.value == null ? '—' : `${item.row.value.toFixed(1)} years`);
        }
        const [mouseX, mouseY] = d3.pointer(event, container);
        const box = tooltip.node().getBoundingClientRect();
        let left = mouseX - box.width - 20;
        if (left < 0) left = mouseX + 20;
        let top = mouseY - 16;
        if (top + box.height > container.clientHeight) top = mouseY - box.height - 16;
        tooltip.style('left', `${Math.max(0, Math.min(container.clientWidth - box.width, left))}px`)
          .style('top', `${Math.max(0, Math.min(container.clientHeight - box.height, top))}px`);
      }).on('pointerleave', () => { tooltip.attr('hidden', true); guide.attr('visibility', 'hidden'); });
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
      countryLegendLabel.text(`One line per country or territory (${new Set(rows.filter(r => r.kind === 'country').map(r => r.code)).size})`);
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
