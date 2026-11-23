"""Build age-65 remaining-life-expectancy series using shared headline calculations."""
import csv
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'src/pipeline'))
from indicators.headline import (GROUPS, OPTIONAL_GROUPS, SEX_COLUMNS, attach_population_weights,
                                 calculate, historical_observations, number, population_by_sex,
                                 read_snapshot, standardise, validate_metadata)
from utils.publication import write_json, write_manifest


def standardise_age65(rows, metadata, sex, reference):
    """Join to validated country eligibility and total population; never infer countries from codes."""
    if sex not in SEX_COLUMNS:
        raise ValueError(f'Invalid age-65 sex: {sex}')
    key = f'Life expectancy - Sex: {sex} - Age: 65 - Type: period'
    column = metadata.get('columns', {}).get(key, {})
    if (column.get('unit') != 'years'
            or 'Remaining period life expectancy at age 65' not in column.get('descriptionShort', '')
            or 'World Population Prospects (2024)' not in column.get('citationShort', '')):
        raise ValueError(f'Unexpected age-65 definition, units, or release: {sex}')
    countries = {row['code'] for row in reference if row['kind'] == 'country'}
    index = {(row['code'], row['year']): row for row in reference if row['sex'] == sex}
    result, seen = [], set()
    for row in rows:
        code, year = row['Code'], int(row['Year'])
        if year > 2023 or code not in countries | GROUPS.keys() | OPTIONAL_GROUPS.keys():
            continue
        identity = code, year, sex
        if identity in seen:
            raise ValueError(f'Duplicate age-65 observation: {identity}')
        seen.add(identity)
        if 'Age 65' not in row:
            raise ValueError(f'Missing Age 65 column: {identity}')
        source = index.get((code, year))
        if code in countries and year >= 1950 and source is None:
            raise ValueError(f'Missing total population reference: {identity}')
        value = number(row['Age 65'], f'age-65 life expectancy {identity}')
        result.append(dict(code=code, entity=row['Entity'], year=year, sex=sex,
                           age=65, unit='years', kind='country' if code in countries else 'group',
                           population=source['population'] if source else None, value=value))
    if not any(row['value'] is not None for row in result):
        raise ValueError(f'No age-65 observations: {sex}')
    return result


def main():
    data = ROOT / 'output/data'
    modern, metadata, dependency, _ = read_snapshot(data, 'headline')
    history, history_metadata, history_dependency, _ = read_snapshot(data, 'headline_history')
    population, population_metadata, population_dependency, _ = read_snapshot(data, 'headline_population')
    validate_metadata(metadata)
    if population_metadata['columns']['Population (historical)']['unit'] != 'people':
        raise ValueError('Unexpected historical population units')
    reference = standardise(modern)
    reference = historical_observations(history, population, reference) + reference
    # Population references may predate the birth observations for a particular country.
    # Add available historical population joins directly for age-65 source years as well.
    country_names = {row['code']: row['entity'] for row in reference if row['kind'] == 'country'}
    existing = {(row['code'], row['year'], row['sex']) for row in reference}
    for row in population:
        year, code = int(row['Year']), row['Code']
        if code not in country_names or year >= 1950:
            continue
        for sex in SEX_COLUMNS:
            if (code, year, sex) not in existing:
                existing.add((code, year, sex))
                reference.append(dict(code=code, entity=country_names[code], year=year, sex=sex,
                                      kind='country', population=number(row['Population'], f'population {code}, {year}')))
    observations, weights, versions = [], {}, {}
    source_urls = {}
    sources = [metadata['chart'], history_metadata['chart'], population_metadata['chart']]
    dependencies = [dependency, history_dependency, population_dependency]
    for sex in SEX_COLUMNS:
        raw, meta, manifest, version = read_snapshot(data, f'headline_age65_{sex}')
        observations.extend(standardise_age65(raw, meta, sex, reference))
        dependencies.append(manifest)
        sources.append(meta['chart'])
        source_urls[sex] = meta['chart']['originalChartUrl']
        versions[sex] = version
        raw, meta, manifest, _ = read_snapshot(data, f'headline_population_{sex}')
        weights.update(population_by_sex(raw, meta, sex, minimum_age=65))
        dependencies.append(manifest)
        sources.append(meta['chart'])
    attach_population_weights(observations, weights)
    rows = calculate(observations)
    directory = data / 'standardised/owid/headline_age65'
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / 'observations.csv').open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(observations[0]))
        writer.writeheader()
        writer.writerows(observations)
    write_json(directory / 'headline.json', {
        'id': 'headline-life-expectancy-age-65', 'age': 65,
        'title': 'Remaining life expectancy at age 65',
        'start_year': min(row['year'] for row in observations if row['value'] is not None),
        'end_year': 2023, 'default_sex': 'female',
        'source': 'HMD (2025); UN WPP (2024)',
        'source_url': source_urls['female'], 'source_urls': source_urls, 'sources': sources,
        'snapshot_versions': versions, 'percentile_population_weights': 'sex_specific_age_65_plus',
        'code_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'code_dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)),
        'rows': rows,
    })
    artifacts = [directory / 'observations.csv', directory / 'headline.json']
    write_manifest({p: p.relative_to(data).as_posix() for p in artifacts}, directory / 'publish.json',
                   kind='standardised', dependencies=dependencies)
    print(f'Calculated {len(rows):,} age-65 observations')


if __name__ == '__main__':
    main()
