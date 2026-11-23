/* Presentation only: summary series are calculated by Python. */
import {
  createModel,
  initialState,
  parseState,
  serializeState,
  updateState,
  visibleData,
  scheduleFrame,
  DEFAULT_SERIES,
} from './headline-state.js';
import { chartConfig } from './headline-config.js';
import { mountControls } from './headline-controls.js';
import { createRenderer, createTableView } from './headline-renderer.js';

function savedSelection(model, config) {
  try {
    const current = localStorage.getItem(config.storageKey);
    if (current !== null) {
      const codes = JSON.parse(current);
      return Array.isArray(codes) ? codes : null;
    }
    if (config.age !== 0) return null;
    const multiple = localStorage.getItem('headline-countries');
    if (multiple !== null) {
      const codes = JSON.parse(multiple);
      if (Array.isArray(codes))
        return [...DEFAULT_SERIES, ...codes.filter((code) => model.countryCodes.has(code))];
    }
    const legacy = localStorage.getItem('headline-country');
    if (legacy === '' || model.countryCodes.has(legacy))
      return [...DEFAULT_SERIES, ...(legacy ? [legacy] : [])];
  } catch {
    /* Storage is optional. */
  }
  return null;
}

function suggestedCountry(countries) {
  try {
    const regionNames = new Intl.DisplayNames(['en'], { type: 'region' });
    for (const language of navigator.languages || [navigator.language]) {
      const region = new Intl.Locale(language).region;
      if (!region) continue;
      const match = countries.find(([, name]) => name === regionNames.of(region));
      if (match) return match[0];
    }
  } catch {
    /* Manual selection remains available. */
  }
  return null;
}

async function main() {
  const status = document.getElementById('status');
  const container = document.getElementById('chart');
  function showError(error) {
    container.replaceChildren();
    status.hidden = false;
    status.textContent = `Unable to display the chart: ${error.message}. Try reloading, or download the data below.`;
  }
  try {
    const response = await fetch('headline.json', { cache: 'no-cache' });
    if (!response.ok) throw new Error(`Data request failed (${response.status})`);
    const data = await response.json();
    if (!Array.isArray(data.rows) || !data.rows.length)
      throw new Error('No observations available');
    const config = chartConfig(data);
    const model = createModel(data);
    const embedded = window.self !== window.top;
    let state = initialState(
      {
        search: location.search,
        embedded,
        saved: embedded ? null : savedSelection(model, config),
        suggestedCountry: suggestedCountry(model.countries),
      },
      model,
    );
    const renderer = createRenderer({ d3: window.d3, container, model, config });
    const table = createTableView(model.names, config);
    const controls = mountControls({
      model,
      getState: () => state,
      update,
      onTableOpen: () => table.renderIfVisible(),
    });
    const requestRender = scheduleFrame(() => {
      try {
        render();
      } catch (error) {
        showError(error);
      }
    });

    function update(action) {
      state =
        action.type === 'restore'
          ? parseState(action.search, model)
          : updateState(state, action, model);
      history.replaceState(null, '', serializeState(state, location.href));
      if (action.type !== 'restore' && !embedded) {
        try {
          localStorage.setItem(config.storageKey, JSON.stringify([...state.selectedSeries]));
        } catch {
          /* Optional persistence. */
        }
      }
      requestRender();
    }

    function render() {
      const view = visibleData(state, model);
      const series = [...state.selectedSeries].map(renderer.lineStyle);
      const grouped = window.d3.group(view.rows, (row) => row.series);
      const summary = new Map(
        series.map((item) => [
          item.id,
          new Map((grouped.get(item.id) || []).map((row) => [row.year, row])),
        ]),
      );
      controls.render(state, view);
      document.getElementById('source').href =
        data.source_urls?.[state.sex] || data.source_url || '#';
      const heading = config.title;
      const subtitle = `${config.subtitlePrefix}${state.sex === 'female' ? 'Women' : 'Men'} · ${view.plotStart}–${view.plotEnd}`;
      document.getElementById('chart-title').textContent = heading;
      document.getElementById('chart-subtitle').textContent = subtitle;
      document.title = `${heading}: ${subtitle} · Our Longevity in Data`;
      table.update({ state, view, series, summary });
      if (!view.available.length) {
        renderer.clear();
        status.hidden = false;
        status.textContent = 'No observations available for this selection.';
        return;
      }
      renderer.draw({ state, view, series, grouped, summary });
      status.hidden = true;
    }

    document.getElementById('source').href = data.source_url || '#';
    document.getElementById('source').textContent = `${data.source}, via Our World in Data`;
    for (const source of data.sources || []) {
      const entry = document.createElement('li');
      const link = document.createElement('a');
      link.href = source.originalChartUrl;
      link.textContent = source.title;
      entry.append(link, document.createTextNode(` — ${source.citation}; via Our World in Data.`));
      document.getElementById('data-source-credits').append(entry);
    }
    window.addEventListener('popstate', () => update({ type: 'restore', search: location.search }));
    history.replaceState(null, '', serializeState(state, location.href));
    let lastWidth;
    new ResizeObserver((entries) => {
      const width = Math.round(entries[0].contentRect.width);
      if (width !== lastWidth) {
        lastWidth = width;
        requestRender();
      }
    }).observe(container);
    requestRender();
  } catch (error) {
    showError(error);
  }
}

main();
