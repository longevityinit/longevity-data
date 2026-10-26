# Third-party source notices

## OWID Grapher tooltip positioning

The headline tooltip positioning adapts the left-side preference, offsets,
edge flipping and boundary clamping present in the following MIT-licensed revision:

- Revision: `a139fcfde4ffdd658019cf48962d23a985414a95`
- [TooltipCard.tsx](https://github.com/owid/owid-grapher/blob/a139fcfde4ffdd658019cf48962d23a985414a95/packages/@ourworldindata/grapher/src/tooltip/TooltipCard.tsx)
- [LineChartTooltip.tsx](https://github.com/owid/owid-grapher/blob/a139fcfde4ffdd658019cf48962d23a985414a95/packages/@ourworldindata/grapher/src/lineCharts/LineChartTooltip.tsx)
- [License at that revision](https://github.com/owid/owid-grapher/blob/a139fcfde4ffdd658019cf48962d23a985414a95/LICENSE.md)

The implementation is simplified for our D3 chart; it is not the original React
component. This attribution is to the historical revision, not current master.
The full notice is included at the top of `src/charts/headline-renderer.js` and travels
with the unminified JavaScript copied into the built chart and its publication manifest.
Preserve this notice if bundling or minification is introduced.

The MIT License (MIT)
Copyright (c) 2024 Our World in Data

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## Browser libraries

- D3 7.9.0 — Copyright 2010–2023 Mike Bostock; ISC license.
  [Full notice](../licenses/d3-7.9.0.txt), [upstream](https://github.com/d3/d3/tree/v7.9.0).
- Observable Plot 0.6.16 — Copyright 2020–2023 Observable, Inc.; ISC license.
  [Full notice](../licenses/observable-plot-0.6.16.txt), [upstream](https://github.com/observablehq/plot/tree/v0.6.16).

These notices accompany the respective libraries in chart publication manifests.

Bundled runtime components:

- [D3 component and dependency notices](../licenses/d3-7.9.0-dependencies.txt),
  using the [D3 7.9.0 release lockfile](https://github.com/d3/d3/blob/v7.9.0/yarn.lock):
  30 D3 modules, InternMap, Delaunator and robust-predicates. D3's CSV command-line
  dependencies are not part of the browser bundle.
- [Plot dependency notices](../licenses/observable-plot-0.6.16-dependencies.txt),
  using the [Plot 0.6.16 release lockfile](https://github.com/observablehq/plot/blob/v0.6.16/yarn.lock):
  interval-tree-1d, binary-search-bounds and isoformat. Plot loads D3 separately.

The corresponding dependency notice files are also included in chart publication manifests.
