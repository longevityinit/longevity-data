"""Build the standalone headline preview without changing the original chart."""
import hashlib
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'src/pipeline'))
from charts.life_expectancy import ensure_vendor_assets, D3_FILENAME
from utils.publication import validate_manifest, write_manifest


def source_assets(root: Path, age: int = 0) -> dict[str, Path]:
    """Map generated presentation assets to the source used by builds and releases."""
    names = ('headline-config.js', 'headline.js', 'headline-state.js', 'headline-controls.js',
             'headline-renderer.js', 'headline.css')
    assets = {**{name: root / 'src/charts' / name for name in names},
              'index.html': root / 'src/pipeline/charts/templates/headline.html'}
    if age == 65:
        assets['age65-methodology.html'] = root / 'src/pipeline/charts/templates/age65-methodology.html'
    return assets


def render_assets(sources: dict[str, bytes], age: int = 0) -> dict[str, bytes]:
    """Render presentation from verified source bytes for local and Pages builds."""
    version = hashlib.sha256(b''.join(sources[name] for name in sorted(sources))).hexdigest()[:12]
    assets = {name: content.decode('utf-8') for name, content in sources.items()}
    template = assets.pop('index.html')
    if age == 65:
        start = re.search(r'<section\s+id="methodology"', template).start()
        end = template.index('</section>', start) + len('</section>')
        template = template[:start] + assets.pop('age65-methodology.html') + template[end:]
        template = template.replace('life expectancy at birth', 'remaining life expectancy at age 65')
        template = template.replace('Life expectancy at birth', 'Remaining life expectancy at age 65')
        template = template.replace('<h1 id="chart-title">Remaining life expectancy at age 65</h1>', '<h1 id="chart-title">Life expectancy at age 65</h1>')
        template = template.replace('id="chart-subtitle">Women', 'id="chart-subtitle">Remaining years of life · Women')
    names = list(assets)
    for name in names:
        template = template.replace(f'="{name}"', f'="{name}?v={version}"')
        for dependency in names:
            assets[name] = assets[name].replace(f"'./{dependency}'", f"'./{dependency}?v={version}'")
    assets['index.html'] = template
    return {name: content.encode('utf-8') for name, content in assets.items()}



def main(age=0):
    if age not in (0, 65):
        raise ValueError('Chart age must be 0 or 65')
    output = ROOT / 'output'
    source = output / 'data/standardised/owid' / ('headline_age65' if age == 65 else 'headline')
    dependency = source / 'publish.json'
    validated = {p for p, _ in validate_manifest(dependency)}
    if any((source / name).resolve() not in validated for name in ('headline.json', 'observations.csv')):
        raise ValueError('Headline inputs missing from manifest')
    charts = output / 'charts'
    directory = charts / ('headline-life-expectancy-age-65' if age == 65 else 'headline-life-expectancy')
    directory.mkdir(parents=True, exist_ok=True)
    ensure_vendor_assets(charts / 'vendor')
    for name in ('headline.json', 'observations.csv'):
        shutil.copy2(source / name, directory / name)
    assets = render_assets({name: path.read_bytes() for name, path in source_assets(ROOT, age).items()}, age)
    for name, content in assets.items():
        (directory / name).write_bytes(content)
    files = [directory / name for name in ['headline.json', 'observations.csv', *assets]]
    files.insert(0, charts / 'vendor' / D3_FILENAME)
    files.extend(charts / 'vendor' / name for name in ('d3-7.9.0.txt', 'd3-7.9.0-dependencies.txt'))
    write_manifest({p: p.relative_to(output).as_posix() for p in files}, directory / 'publish.json',
                   kind='chart', dependencies=[dependency])
    print(f'Chart written to {directory}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--age', type=int, choices=(0, 65), default=0)
    main(parser.parse_args().age)
