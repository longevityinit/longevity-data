/* Presentation only: summary series are calculated by Python. */
(async function () {
  const status = document.getElementById('status');
  const container = document.getElementById('chart');
  const series = [
    {id: 'frontier', name: 'Best practice', color: '#ff5722'},
    {id: 'OWID_HIC', name: 'High income', color: '#9454e8'},
    {id: 'OWID_WRL', name: 'World life expectancy', color: '#2680ff'},
    {id: 'OWID_LIC', name: 'Low income', color: '#00a6b2'},
    {id: 'p10', name: '10th percentile (population-weighted)', color: '#e63e91', dash: '6 3'},
    {id: 'p25', name: '25th percentile (population-weighted)', color: '#00b878', dash: '2 3'},
  ];
  try {
    const response = await fetch('headline.json');
    if (!response.ok) throw new Error(`Data request failed (${response.status})`);
    const data = await response.json();
    if (!Array.isArray(data.rows) || !data.rows.length) throw new Error('No observations available');
    const names = new Map(data.rows.map(r => [r.code, r.entity]));
    const years = d3.range(data.start_year, data.end_year + 1);
    const format = value => value == null ? '—' : value.toFixed(2);
    const legend = d3.select('#legend');
    function addSwatch(entry, color, width, dash) {
      entry.append('svg').attr('class', 'swatch').attr('viewBox', '0 0 36 12').attr('aria-hidden', 'true')
        .append('line').attr('x1', 0).attr('x2', 36).attr('y1', 6).attr('y2', 6)
        .attr('stroke', color).attr('stroke-width', width).attr('stroke-dasharray', dash || null);
    }
    for (const item of series) {
      const li = legend.append('li');
      addSwatch(li, item.color, 2.7, item.dash);
      li.append('span').text(item.name);
    }
    const countryLegend = legend.append('li').attr('class', 'country-legend');
    addSwatch(countryLegend, '#65727d', 2);
    const countryLegendLabel = countryLegend.append('span');
    document.getElementById('source').textContent = `${data.source}, via Our World in Data`;
    function render() {
      const sex = document.querySelector('input[name="sex"]:checked').value;
      const title = `A century of unequal progress: life expectancy at birth for ${sex === 'female' ? 'women' : 'men'}, ${data.start_year}–${data.end_year}`;
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
      const width = Math.max(280, container.clientWidth), height = width < 600 ? 350 : 480;
      const margin = {left: 64, right: 16, top: 24, bottom: 35};
      const x = d3.scaleLinear().domain([data.start_year, data.end_year]).range([margin.left, width - margin.right]);
      const y = d3.scaleLinear().domain([0, Math.ceil(d3.max(data.rows, r => r.value) / 10) * 10]).range([height - margin.bottom, margin.top]);
      container.replaceChildren();
      const svg = d3.select(container).append('svg').attr('viewBox', `0 0 ${width} ${height}`)
        .attr('role', 'img').attr('aria-label', `${sex === 'female' ? 'Female' : 'Male'} life expectancy at birth, ${data.start_year}–${data.end_year}. Annual highlighted values are available in the table below.`);
      svg.append('g').attr('transform', `translate(${margin.left},0)`)
        .call(d3.axisLeft(y)
          .tickValues(d3.range(0, y.domain()[1] + 1, 10))
          .tickFormat(value => value === 0 ? '' : d3.format('d')(value))
          .tickSize(-(width - margin.left - margin.right)))
        .call(g => g.select('.domain').remove())
        .call(g => g.selectAll('.tick line').attr('stroke', '#e7ebee'));
      // Leave at least 40px between four-digit year labels, using whole decades.
      const plotWidth = width - margin.left - margin.right;
      const yearStep = Math.max(10, Math.ceil((data.end_year - data.start_year) * 40 / plotWidth / 10) * 10);
      svg.append('g').attr('transform', `translate(0,${height - margin.bottom})`)
        .call(d3.axisBottom(x)
          .tickValues(d3.range(data.start_year, data.end_year + 1, yearStep))
          .tickFormat(d3.format('d')));
      svg.append('text')
        .attr('transform', `translate(16,${(margin.top + height - margin.bottom) / 2}) rotate(-90)`)
        .attr('text-anchor', 'middle').attr('font-size', 12).attr('fill', '#536372')
        .text('Life expectancy at birth (years)');
      svg.append('text').attr('x', (x(data.start_year) + x(1950)) / 2)
        .attr('y', y(85)).attr('dominant-baseline', 'middle')
        .attr('text-anchor', 'middle').attr('font-size', 11)
        .attr('fill', '#65727d').text('Before 1950: limited country coverage');
      svg.append('line').attr('x1', x(1950)).attr('x2', x(1950))
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
        if (values[0].kind !== 'country') continue;
        svg.append('path').datum(points(values)).attr('d', line).attr('fill', 'none')
          .attr('stroke', '#75818b')
          .attr('stroke-opacity', countryOpacity(populationShares.get(values[0].code) || 0))
          .attr('stroke-width', 1)
          .append('title').text(values[0].entity);
      }
      for (const item of series) {
        svg.append('path').datum(points(grouped.get(item.id) || [])).attr('d', line)
          .attr('fill', 'none').attr('stroke', item.color).attr('stroke-width', 2.7).attr('stroke-dasharray', item.dash || null)
          .append('title').text(item.name);
      }
      const tooltip = d3.select(container).append('div').attr('id', 'tooltip').attr('hidden', true);
      const guide = svg.append('line').attr('y1', margin.top).attr('y2', height - margin.bottom)
        .attr('stroke', '#536372').attr('stroke-dasharray', '3 3').attr('visibility', 'hidden');
      svg.on('pointermove', event => {
        const [px] = d3.pointer(event, svg.node());
        const year = Math.max(data.start_year, Math.min(data.end_year, Math.round(x.invert(px))));
        guide.attr('x1', x(year)).attr('x2', x(year)).attr('visibility', 'visible');
        tooltip.attr('hidden', null).style('left', `${Math.max(0, Math.min(width - 250, px + 12))}px`).style('top', '25px');
        tooltip.selectAll('*').remove();
        tooltip.append('strong').text(year);
        for (const item of series) {
          const row = summary.get(item.id).get(year);
          tooltip.append('div').text(`${item.name}: ${format(row?.value)} years`);
          if (row?.winners.length) tooltip.append('div').text(row.winners.map(id => names.get(id) || id).join(', '));
        }
      }).on('pointerleave', () => { tooltip.attr('hidden', true); guide.attr('visibility', 'hidden'); });
      const body = document.getElementById('values');
      body.replaceChildren();
      for (const year of [...years].reverse()) {
        const tr = document.createElement('tr');
        const th = document.createElement('th'); th.scope = 'row'; th.textContent = year; tr.append(th);
        for (const item of series) {
          const row = summary.get(item.id).get(year);
          const td = document.createElement('td');
          td.textContent = format(row?.value) + (row?.winners.length ? ` (${row.winners.map(id => names.get(id) || id).join(', ')})` : '');
          tr.append(td);
        }
        body.append(tr);
      }
      document.getElementById('table-caption').textContent = `${sex === 'female' ? 'Female' : 'Male'} life expectancy at birth, in years`;
      countryLegendLabel.text(`One line per country or territory (${new Set(rows.filter(r => r.kind === 'country').map(r => r.code)).size})`);
      status.hidden = true;
    }
    document.querySelectorAll('input[name="sex"]').forEach(input => input.addEventListener('change', render));
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
