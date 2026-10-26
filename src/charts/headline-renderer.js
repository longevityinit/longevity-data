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
import { isReference, tooltipEntries } from './headline-state.js';

const format = (value) => (value == null ? '—' : value.toFixed(2));

export function createRenderer({ d3, container, model }) {
  const { names, countryCodes, options } = model;
  const referenceSeries = [
    { id: 'frontier', name: 'Best practice', color: '#ff5722' },
    { id: 'OWID_WRL', name: 'World life expectancy', color: '#2680ff' },
    { id: 'p10', name: '10th percentile (population-weighted)', color: '#e63e91', dash: '6 3' },
  ];
  function lineStyle(code) {
    const reference = referenceSeries.find((item) => item.id === code);
    if (reference) return reference;
    const position = (0.42 + options.findIndex(([id]) => id === code) * 0.618034) % 1;
    const color = d3.hsl(d3.interpolateRainbow(position));
    // Keep the original hue variety, darkening only pale yellow/green shades.
    if (color.h >= 55 && color.h <= 115) color.l = Math.min(color.l, 0.38);
    return {
      id: code,
      name: names.get(code),
      color: color.formatHex(),
      dash: /^p\d+$/.test(code) ? '6 3' : null,
    };
  }
  const lineWidth = (code, emphasized = false) =>
    countryCodes.has(code) ? (emphasized ? 3 : 1.8) : emphasized ? 4 : 2.7;

  function draw({ view, series, grouped, summary }) {
    const { available, plotStart, plotEnd } = view;
    const years = d3.range(plotStart, plotEnd + 1);
    const interaction = { hoveredSeries: null, labelPointer: null };
    const width = Math.max(280, container.clientWidth),
      height = width < 600 ? 350 : 480;
    const mobile = width < 600;
    const margin = { left: 64, right: mobile ? 128 : 175, top: 24, bottom: 35 };
    const rangeEditor = document.querySelector('.range-editor');
    const displayScale = container.clientWidth / width;
    rangeEditor.style.setProperty('--plot-left', `${margin.left * displayScale}px`);
    rangeEditor.style.setProperty('--plot-right', `${margin.right * displayScale}px`);
    const x = d3
      .scaleLinear()
      .domain([plotStart, plotEnd])
      .range([margin.left, width - margin.right]);
    const [lowestValue, highestValue] = d3.extent(available, (row) => row.value);
    const valuePadding = Math.max(2, (highestValue - lowestValue) * 0.05);
    const y = d3
      .scaleLinear()
      .domain([
        Math.max(0, Math.floor((lowestValue - valuePadding) / 5) * 5),
        Math.ceil((highestValue + valuePadding) / 5) * 5,
      ])
      .range([height - margin.bottom, margin.top]);
    container.replaceChildren();
    const svg = d3
      .select(container)
      .append('svg')
      .attr('viewBox', `0 0 ${width} ${height}`)
      .attr('role', 'group')
      .attr('aria-labelledby', 'chart-subtitle')
      .attr('aria-details', 'chart-data');
    svg
      .append('g')
      .attr('transform', `translate(${margin.left},0)`)
      .call(
        d3
          .axisLeft(y)
          .ticks(6)
          .tickFormat((value) => (value === 0 ? '' : d3.format('d')(value)))
          .tickSize(-(width - margin.left - margin.right)),
      )
      .call((g) => g.select('.domain').remove())
      .call((g) =>
        g.selectAll('.tick line').attr('stroke', 'var(--chart-grid)').attr('stroke-width', 1),
      );
    // Leave at least 40px between four-digit year labels, using whole decades.
    const plotWidth = width - margin.left - margin.right;
    const minimumStep = ((plotEnd - plotStart) * 40) / plotWidth;
    const yearStep = [1, 2, 5, 10, 20, 25, 50, 100].find((step) => step >= minimumStep) || 100;
    svg
      .append('g')
      .attr('transform', `translate(0,${height - margin.bottom})`)
      .call(
        d3
          .axisBottom(x)
          .tickValues(d3.range(Math.ceil(plotStart / yearStep) * yearStep, plotEnd + 1, yearStep))
          .tickFormat(d3.format('d')),
      );
    svg
      .append('text')
      .attr('transform', `translate(16,${(margin.top + height - margin.bottom) / 2}) rotate(-90)`)
      .attr('text-anchor', 'middle')
      .attr('font-size', 12)
      .attr('fill', '#536372')
      .text('Life expectancy at birth (years)');
    if (plotStart <= 1950 && plotEnd >= 1950)
      svg
        .append('line')
        .attr('x1', x(1950))
        .attr('x2', x(1950))
        .attr('y1', margin.top)
        .attr('y2', height - margin.bottom)
        .attr('stroke', '#65727d')
        .attr('stroke-width', 1.5)
        .attr('stroke-dasharray', '1 5')
        .attr('stroke-linecap', 'round')
        .append('title')
        .text('1950: broader country coverage begins');
    const line = d3
      .line()
      .defined((r) => r.value != null)
      .x((r) => x(r.year))
      .y((r) => y(r.value));
    function points(values) {
      const byYear = new Map(values.map((r) => [r.year, r]));
      return years.map((year) => byYear.get(year) || { year, value: null });
    }
    for (const item of series) {
      svg
        .append('path')
        .attr('class', 'summary-line')
        .attr('data-series', item.id)
        .datum(points(grouped.get(item.id) || []))
        .attr('d', line)
        .attr('fill', 'none')
        .attr('stroke', item.color)
        .attr('stroke-width', lineWidth(item.id))
        .attr('stroke-dasharray', item.dash || null)
        .on('pointerenter', (event) => {
          if (event.pointerType !== 'touch') {
            interaction.hoveredSeries = item.id;
            updateHighlight();
          }
        })
        .on('pointerleave', () => {
          interaction.hoveredSeries = null;
          updateHighlight();
        })
        .append('title')
        .text(item.name);
    }
    const labels = createLabels({
      svg,
      series,
      grouped,
      summary,
      x,
      y,
      width,
      height,
      margin,
      mobile,
      plotStart,
      plotEnd,
      lineWidth,
      interaction,
    });
    const { drawEndpointLabels, updateHighlight } = labels;
    updateHighlight();
    createTooltip({
      svg,
      series,
      summary,
      x,
      y,
      width,
      height,
      margin,
      plotStart,
      plotEnd,
      interaction,
      drawEndpointLabels,
    });
  }

  function createTooltip({
    svg,
    series,
    summary,
    x,
    y,
    width,
    height,
    margin,
    plotStart,
    plotEnd,
    interaction,
    drawEndpointLabels,
  }) {
    const tooltip = d3.select(container).append('div').attr('id', 'tooltip').attr('hidden', true);
    const guide = svg
      .append('line')
      .attr('y1', margin.top)
      .attr('y2', height - margin.bottom)
      .attr('stroke', '#536372')
      .attr('stroke-dasharray', '3 3')
      .attr('visibility', 'hidden');
    let pinned = false;
    let lastHover = [margin.left, margin.top];
    const popupLimit = Math.min(320, height - margin.top - margin.bottom, window.innerHeight * 0.5);
    tooltip.style('--tooltip-limit', `${popupLimit}px`);
    function closeTooltip() {
      pinned = false;
      tooltip.attr('hidden', true).classed('is-pinned', false);
      guide.attr('visibility', 'hidden');
      interaction.labelPointer = null;
      drawEndpointLabels(interaction.hoveredSeries);
    }
    function showTooltip(event, pin = false) {
      if (pinned && !pin) return;
      const [px, py] = event ? d3.pointer(event, svg.node()) : lastHover;
      if (
        px > width - margin.right ||
        px < margin.left ||
        py < margin.top ||
        py > height - margin.bottom
      ) {
        if (!pinned) closeTooltip();
        return;
      }
      lastHover = [px, py];
      pinned = pin;
      const year = Math.max(plotStart, Math.min(plotEnd, Math.round(x.invert(px))));
      guide.attr('x1', x(year)).attr('x2', x(year)).attr('visibility', 'visible');
      interaction.labelPointer = { year, y: py };
      drawEndpointLabels(interaction.hoveredSeries);
      tooltip
        .attr('hidden', null)
        .classed('is-pinned', pin)
        .attr('role', pin ? 'dialog' : 'tooltip')
        .attr('aria-label', `Life expectancy in ${year}`);
      tooltip.selectAll('*').remove();
      const heading = tooltip.append('div').attr('class', 'tooltip-heading');
      heading.append('span').text(year);
      if (pin)
        heading
          .append('button')
          .attr('type', 'button')
          .attr('aria-label', 'Close year values')
          .text('×')
          .on('click', () => {
            closeTooltip();
          });
      const contents = tooltip.append('div').attr('class', 'tooltip-contents');
      const entries = series
        .map((item) => ({ ...item, row: summary.get(item.id).get(year) }))
        .sort((a, b) => (b.row?.value ?? -Infinity) - (a.row?.value ?? -Infinity));
      function addRows(items) {
        contents.selectAll('*').remove();
        for (const item of items) {
          const entry = contents.append('div').attr('class', 'tooltip-row');
          entry.append('span').attr('class', 'tooltip-swatch').style('background', item.color);
          const label = entry
            .append('span')
            .attr('class', 'tooltip-name')
            .text(item.name.replace(' (population-weighted)', ''));
          if (item.row?.winners?.length)
            label
              .append('small')
              .text(item.row.winners.map((id) => names.get(id) || id).join(', '));
          entry
            .append('strong')
            .text(item.row?.value == null ? 'No data' : `${item.row.value.toFixed(1)} years`);
        }
      }
      const availableEntries = entries.filter((item) => item.row?.value != null);
      const visibleEntries = tooltipEntries(entries, { pinned: pin, pointerY: py, valueY: y });
      addRows(visibleEntries);
      if (visibleEntries.length < availableEntries.length) {
        tooltip
          .append('div')
          .attr('class', 'tooltip-hint')
          .text(
            `+${availableEntries.length - visibleEntries.length} more · Click or tap for all selected values`,
          );
      }
      if (pin) {
        tooltip
          .style('left', `${Math.max(0, width - margin.right - Math.min(300, width))}px`)
          .style('top', `${margin.top}px`);
        contents
          .attr('tabindex', 0)
          .attr('role', 'region')
          .attr('aria-label', 'Scrollable selected values');
        heading.select('button').node().focus({ preventScroll: true });
        return;
      }
      const [mouseX, mouseY] = event ? d3.pointer(event, container) : lastHover;
      const box = tooltip.node().getBoundingClientRect();
      let left = mouseX - box.width - 20;
      if (left < 0) left = mouseX + 20;
      let top = mouseY - 16;
      if (top + box.height > container.clientHeight) top = mouseY - box.height - 16;
      tooltip
        .style('left', `${Math.max(0, Math.min(container.clientWidth - box.width, left))}px`)
        .style('top', `${Math.max(0, Math.min(container.clientHeight - box.height, top))}px`);
    }
    svg
      .on('pointermove', (event) => showTooltip(event))
      .on('pointerleave', () => {
        if (!pinned) closeTooltip();
      })
      .on('click', (event) => showTooltip(event, true));
    d3.select(window).on('keydown.headlineTooltip', (event) => {
      if (event.key === 'Escape' && pinned) {
        closeTooltip();
      }
    });
  }

  function createLabels({
    svg,
    series,
    grouped,
    summary,
    x,
    y,
    width,
    height,
    margin,
    mobile,
    plotStart,
    plotEnd,
    lineWidth,
    interaction,
  }) {
    const endpointCandidates = series.flatMap((item) => {
      const last = (grouped.get(item.id) || [])
        .filter((r) => r.value != null && r.year >= plotStart && r.year <= plotEnd)
        .sort((a, b) => a.year - b.year)
        .at(-1);
      if (!last) return [];
      const words = item.name.replace(' (population-weighted)', '').split(' ');
      const lines = [];
      for (const word of words) {
        if (!lines.length || (lines.at(-1) + ' ' + word).length > (mobile ? 16 : 23))
          lines.push(word);
        else lines[lines.length - 1] += ' ' + word;
      }
      return [
        {
          ...item,
          last,
          lines,
          anchorY: y(last.value),
          labelHeight: Math.max(18, lines.length * 14),
        },
      ];
    });
    const endpointLayer = svg.append('g');

    function visibleEndpointLabels(active) {
      const available = endpointCandidates.filter(
        (item) =>
          !interaction.labelPointer ||
          summary.get(item.id).get(interaction.labelPointer.year)?.value != null ||
          item.id === active,
      );
      const priority = (item) => isReference(item) || item.id === active;
      const distance = (item) =>
        interaction.labelPointer
          ? Math.abs(
              y(summary.get(item.id).get(interaction.labelPointer.year)?.value ?? item.last.value) -
                interaction.labelPointer.y,
            )
          : -item.last.value;
      const references = available.filter(priority);
      const nearby = available
        .filter((item) => !priority(item))
        .sort((a, b) => distance(a) - distance(b));
      const limit = Math.max(mobile ? 4 : 10, references.length);
      const chosen = [...references, ...nearby.slice(0, Math.max(0, limit - references.length))]
        .map((item) => ({ ...item }))
        .sort((a, b) => a.anchorY - b.anchorY);
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
      const labels = endpointLayer
        .selectAll('.line-label')
        .data(visibleEndpointLabels(active), (item) => item.id)
        .join((enter) => {
          const group = enter.append('g').attr('class', 'line-label');
          group
            .append('path')
            .attr('class', 'label-connector')
            .attr('fill', 'none')
            .attr('stroke-width', 1)
            .attr('stroke-opacity', 0.5);
          group.append('rect').attr('fill', 'transparent');
          group
            .append('text')
            .attr('font-size', 12)
            .attr('font-weight', 400)
            .attr('dominant-baseline', 'middle');
          group
            .on('pointerenter', (event, item) => {
              if (event.pointerType !== 'touch') {
                interaction.hoveredSeries = item.id;
                updateHighlight();
              }
            })
            .on('pointerleave', () => {
              interaction.hoveredSeries = null;
              updateHighlight();
            })
            .on('click', (event) => event.stopPropagation());
          return group;
        });
      labels.attr('data-series', (item) => item.id).attr('aria-label', (item) => item.name);
      labels
        .select('path')
        .attr('data-series', (item) => item.id)
        .attr('stroke', (item) => item.color)
        .attr(
          'd',
          (item) => `M${x(item.last.year)},${y(item.last.value)}L${labelX - 5},${item.labelY}`,
        );
      labels
        .select('rect')
        .attr('x', labelX - 4)
        .attr('y', (item) => item.labelY - item.labelHeight / 2)
        .attr('width', margin.right - 16)
        .attr('height', (item) => item.labelHeight);
      labels
        .select('text')
        .attr('x', labelX)
        .attr('y', (item) => item.labelY)
        .attr('fill', (item) => item.color)
        .each(function (item) {
          const text = d3.select(this);
          text.selectAll('*').remove();
          item.lines.forEach((line, i) =>
            text
              .append('tspan')
              .attr('x', labelX)
              .attr('dy', i ? '1.1em' : `${-(item.lines.length - 1) * 0.55}em`)
              .text(line),
          );
          text.append('title').text(`${item.name}: ${format(item.last.value)} (${item.last.year})`);
        });
    }
    function updateHighlight() {
      const selectedSeries = interaction.hoveredSeries;
      drawEndpointLabels(selectedSeries);
      svg.selectAll('.summary-line, .line-label').attr('opacity', function () {
        return !selectedSeries || this.getAttribute('data-series') === selectedSeries ? 1 : 0.15;
      });
      svg.selectAll('.summary-line').attr('stroke-width', function () {
        return lineWidth(
          this.getAttribute('data-series'),
          this.getAttribute('data-series') === selectedSeries,
        );
      });
    }
    return { drawEndpointLabels, updateHighlight };
  }

  return {
    draw,
    lineStyle,
    clear() {
      container.replaceChildren();
      d3.select(window).on('keydown.headlineTooltip', null);
    },
  };
}

// The hidden table has no rows until requested. Resizing the plot never invalidates it.
export function createTableView(names) {
  let current;
  let version;
  let renderedVersion;
  function renderIfVisible() {
    if (!current || document.getElementById('chart-data').hidden || version === renderedVersion)
      return;
    const { state, view, series, summary } = current;
    const head = document.querySelector('#table-caption + thead tr');
    head.replaceChildren();
    for (const name of ['Year', ...series.map((item) => item.name)]) {
      const th = document.createElement('th');
      th.scope = 'col';
      th.textContent = name;
      head.append(th);
    }
    const fragment = document.createDocumentFragment();
    if (view.available.length) {
      for (let year = view.plotEnd; year >= view.plotStart; year--) {
        const tr = document.createElement('tr');
        const th = document.createElement('th');
        th.scope = 'row';
        th.textContent = year;
        tr.append(th);
        for (const item of series) {
          const row = summary.get(item.id).get(year);
          const td = document.createElement('td');
          td.textContent =
            format(row?.value) +
            (row?.winners?.length
              ? ` (${row.winners.map((code) => names.get(code) || code).join(', ')})`
              : '');
          tr.append(td);
        }
        fragment.append(tr);
      }
    }
    document.getElementById('values').replaceChildren(fragment);
    document.getElementById('table-caption').textContent =
      `${state.sex === 'female' ? 'Female' : 'Male'} life expectancy at birth, in years`;
    renderedVersion = version;
  }
  return {
    update(context) {
      current = context;
      const { state, view } = context;
      version = JSON.stringify([
        state.sex,
        [...state.selectedSeries],
        view.plotStart,
        view.plotEnd,
      ]);
      renderIfVisible();
    },
    renderIfVisible,
  };
}
