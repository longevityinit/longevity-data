function parseCSV(text) {
  function splitLine(line) {
    const fields = [];
    let field = '', quoted = false;
    for (const ch of line) {
      if (ch === '"') { quoted = !quoted; }
      else if (ch === ',' && !quoted) { fields.push(field); field = ''; }
      else { field += ch; }
    }
    fields.push(field);
    return fields;
  }
  const lines = text.trim().split(/\r?\n/);
  const headers = splitLine(lines[0]);
  return lines.slice(1)
    .filter(l => l.trim())
    .map(l => {
      const vals = splitLine(l);
      return Object.fromEntries(headers.map((h, i) => [h, vals[i] ?? '']));
    });
}

async function fetchCSV(path) {
  const resp = await fetch(path);
  if (!resp.ok) throw new Error(`Failed to load ${path}: ${resp.status}`);
  return parseCSV(await resp.text());
}

function filterAndCoerce(allRows, filter, defaults, valueCol) {
  const visible = filter
    ? allRows.filter(d => filter.includes(d.code) || filter.includes(d.entity))
    : allRows.filter(d => defaults.includes(d.entity) || defaults.includes(d.code));
  return visible
    .filter(d => d[valueCol] !== '' && d[valueCol] != null)
    .map(d => ({ entity: d.entity, code: d.code, year: +d.year, value: +d[valueCol] }));
}

function addHoverFade(svg) {
  const paths = [...svg.querySelectorAll('[aria-label="line"] path')];
  paths.forEach(path => {
    path.style.pointerEvents = 'visibleStroke';
    path.addEventListener('mouseenter', () => {
      svg.classList.add('fade-others');
      path.classList.add('hovered');
    });
    path.addEventListener('mouseleave', () => {
      svg.classList.remove('fade-others');
      path.classList.remove('hovered');
    });
  });
}

function lineChart(containerId, data, opts) {
  opts = opts || {};
  const chart = Plot.plot({
    style: { fontSize: '13px' },
    marginLeft: 48,
    x: { label: null },
    y: { label: opts.yLabel || null, grid: true },
    color: { legend: true },
    marks: [
      Plot.lineY(data, {
        x: 'year', y: 'value', stroke: 'entity',
        strokeWidth: 2, curve: 'monotone-x',
      }),
      Plot.tip(data, Plot.pointerX({
        x: 'year', y: 'value',
        title: d => `${d.entity}: ${d.value} (${d.year})`,
      })),
    ],
  });
  document.getElementById(containerId).append(chart);
  const svg = chart.tagName === 'svg' ? chart : chart.querySelector('svg');
  addHoverFade(svg);
}
