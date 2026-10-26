"""Serve source assets and deterministic chart data for browser regression tests."""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[2]
D3 = ROOT / 'output/charts/vendor/d3-7.9.0.min.js'
if not D3.is_file():
    raise SystemExit('Build a chart first to prepare the pinned D3 asset (see docs/development.md).')

rows = []
locations = [('frontier', 'Best practice', 'summary', 1850),
             ('OWID_WRL', 'World life expectancy', 'summary', 1950),
             ('p10', '10th percentile (population-weighted)', 'summary', 1950),
             ('p25', '25th percentile (population-weighted)', 'summary', 1950),
             ('UN_EUR', 'Europe (UN)', 'summary', 1950),
             ('SWE', 'Sweden', 'country', 1751), ('AND', 'Andorra', 'country', 1950)]
locations += [(f'C{i:02}', f'Country {i:02}', 'country', 1950) for i in range(15)]
for sex in ('female', 'male'):
    for index, (code, name, kind, first) in enumerate(locations):
        for year in range(first, 2024):
            rows.append(dict(code=code, series=code, entity=name, kind=kind, sex=sex,
                             year=year, value=40 + (year - first) * .1 + index * .5 - (5 if sex == 'male' else 0),
                             winners=['SWE'] if code == 'frontier' else []))
DATA = json.dumps(dict(start_year=1751, end_year=2023, rows=rows, source='Browser test fixture', sources=[])).encode()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        name = Path(urlparse(self.path).path).name
        if name == 'headline.json':
            content, kind = DATA, 'application/json'
        else:
            if name == 'd3-7.9.0.min.js':
                path = D3
            elif name in ('headline.js', 'headline-state.js', 'headline-controls.js', 'headline-renderer.js', 'headline.css'):
                path = ROOT / 'src/charts' / name
            elif not name or name == 'index.html':
                path = ROOT / 'src/pipeline/charts/templates/headline.html'
            else:
                self.send_error(404)
                return
            content = path.read_bytes()
            kind = 'text/javascript' if path.suffix == '.js' else 'text/css' if path.suffix == '.css' else 'text/html'
        self.send_response(200)
        self.send_header('Content-Type', kind)
        self.send_header('Content-Length', str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, *_args):
        pass


HTTPServer(('127.0.0.1', 8765), Handler).serve_forever()
