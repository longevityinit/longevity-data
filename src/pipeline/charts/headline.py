"""Build the standalone headline preview without changing the original chart."""
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'src/pipeline'))
from charts.life_expectancy import ensure_vendor_assets, D3_FILENAME
from utils.publication import validate_manifest, write_manifest


def main():
    output = ROOT / 'output'
    source = output / 'data/standardised/owid/headline'
    dependency = source / 'publish.json'
    validated = {p for p, _ in validate_manifest(dependency)}
    if any((source / name).resolve() not in validated for name in ('headline.json', 'observations.csv')):
        raise ValueError('Headline inputs missing from manifest')
    charts = output / 'charts'
    directory = charts / 'headline-life-expectancy'
    directory.mkdir(parents=True, exist_ok=True)
    ensure_vendor_assets(charts / 'vendor')
    for name in ('headline.json', 'observations.csv'):
        shutil.copy2(source / name, directory / name)
    shutil.copy2(ROOT / 'src/charts/headline.js', directory / 'headline.js')
    shutil.copy2(Path(__file__).parent / 'templates/headline.html', directory / 'index.html')
    files = [directory / name for name in ('headline.json', 'observations.csv', 'headline.js', 'index.html')]
    files.insert(0, charts / 'vendor' / D3_FILENAME)
    write_manifest({p: p.relative_to(output).as_posix() for p in files}, directory / 'publish.json',
                   kind='chart', dependencies=[dependency])
    print(f'Chart written to {directory}')


if __name__ == '__main__':
    main()
