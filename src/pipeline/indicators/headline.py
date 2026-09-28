"""Validate at-birth observations and calculate the headline series."""
import csv
import json
import math
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'src/pipeline'))
from utils.publication import snapshot_manifest, write_manifest, write_json

SEX_COLUMNS = {'female': 'Life expectancy of women', 'male': 'Life expectancy of men'}
GROUPS = {'OWID_HIC': 'High-income countries', 'OWID_WRL': 'World life expectancy',
          'OWID_LIC': 'Low-income countries'}
REGIONS = {'Africa', 'Asia', 'Europe', 'North America', 'South America', 'Oceania'}


def number(value, name):
    if value == '':
        return None
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ValueError(f'Invalid {name}: {value}')
    return result


def standardise(rows):
    """Use OWID's country region field to exclude aggregates, including unnamed ones."""
    observations, seen = [], set()
    for row in rows:
        year = int(row['Year'])
        if not 1950 <= year <= 2023:
            continue
        code, entity = row['Code'], row['Entity']
        region = row['World region according to OWID']
        if region and region not in REGIONS:
            raise ValueError(f'Unknown region for {entity}: {region}')
        kind = 'country' if region else 'group'
        if kind == 'group' and code not in GROUPS:
            continue
        if not code:
            raise ValueError(f'Missing location ID: {entity}')
        population = number(row['Population'], 'population')
        if kind == 'country' and population is None:
            raise ValueError(f'Missing population: {entity}, {year}')
        for sex, column in SEX_COLUMNS.items():
            key = code, year, sex
            if key in seen:
                raise ValueError(f'Duplicate observation: {key}')
            seen.add(key)
            observations.append(dict(code=code, entity=entity, year=year, sex=sex,
                                     age=0, unit='years', kind=kind, population=population,
                                     value=number(row[column], 'life expectancy')))
    if not observations:
        raise ValueError('No historical observations found')
    return observations


def historical_observations(history, population, current):
    locations = {r['code'] for r in current if r['kind'] == 'country'}
    populations = {}
    for row in population:
        if not 1900 <= int(row['Year']) < 1950:
            continue
        key = row['Code'], int(row['Year'])
        if key in populations:
            raise ValueError(f'Duplicate historical population: {key}')
        populations[key] = number(row['Population'], 'population')
    result, seen = [], set()
    for row in history:
        year, code = int(row['Year']), row['Code']
        if not 1900 <= year < 1950 or code not in locations:
            continue
        pop = populations.get((code, year))
        if pop is None:
            raise ValueError(f'Missing historical population: {code}, {year}')
        for sex in SEX_COLUMNS:
            key = code, year, sex
            if key in seen:
                raise ValueError(f'Duplicate historical observation: {key}')
            seen.add(key)
            result.append(dict(code=code, entity=row['Entity'], year=year, sex=sex,
                               age=0, unit='years', kind='country', population=pop,
                               value=number(row[sex.title()], 'life expectancy')))
    return result


def read_snapshot(data, dataset):
    source = data / 'snapshots/owid' / dataset
    version = yaml.safe_load((source / 'current.yaml').read_text())['version']
    snapshot = source / version
    dependency = snapshot_manifest(snapshot, data)
    metadata = json.loads((snapshot / f'{dataset}.owid.json').read_text())
    with (snapshot / f'{dataset}.csv').open(newline='', encoding='utf-8') as stream:
        rows = list(csv.DictReader(stream))
    return rows, metadata, dependency, version


def population_by_sex(rows, metadata, sex):
    """Sum exhaustive, non-overlapping five-year ages, including 100+."""
    ages = [f'{age}-{age + 4}' for age in range(0, 100, 5)] + ['100+']
    for age in ages:
        column = metadata['columns'][f'Population - Sex: {sex} - Age: {age} - Variant: estimates']
        if column['unit'] != 'people' or 'World Population Prospects (2024)' not in column['citationShort']:
            raise ValueError('Unexpected sex-specific population source or units')
    result = {}
    for row in rows:
        year = int(row['Year'])
        if not row['Code'] or not 1950 <= year <= 2023:
            continue
        key = row['Code'], year, sex
        if key in result:
            raise ValueError(f'Duplicate sex-specific population: {key}')
        counts = [float(row[f'{age} years']) for age in ages]
        if any(not math.isfinite(n) or n < 0 for n in counts):
            raise ValueError(f'Invalid age-specific population: {key}')
        result[key] = math.fsum(counts)
    return result


def attach_population_weights(observations, populations):
    for row in observations:
        row['sex_population'] = None
        if row['kind'] != 'country' or row['year'] < 1950:
            continue
        key = row['code'], row['year'], row['sex']
        weight = populations.get(key)
        if weight is None or weight <= 0:
            raise ValueError(f'Missing or invalid sex-specific population: {key}')
        row['sex_population'] = weight


def weighted_percentile(countries, percentile):
    """Inverse weighted CDF: first value reaching the requested population share."""
    if not 0 < percentile <= 1:
        raise ValueError('Percentile must be in (0, 1]')
    valid = [r for r in countries if r['value'] is not None]
    if not valid:
        return None
    for row in valid:
        if row['sex_population'] is None or not math.isfinite(row['sex_population']) or row['sex_population'] <= 0:
            raise ValueError('Percentiles require positive finite population weights')
    target = math.fsum(r['sex_population'] for r in valid) * percentile
    cumulative = 0
    for row in sorted(valid, key=lambda r: (r['value'], r['code'])):
        cumulative += row['sex_population']
        if cumulative >= target:
            return row['value']
    return max(r['value'] for r in valid)


def calculate(observations):
    result = [dict(row, series=row['code'], winners=[]) for row in observations if row['kind'] == 'country']
    annual = defaultdict(list)
    for row in observations:
        annual[row['sex'], row['year']].append(row)
    for (sex, year), rows in sorted(annual.items()):
        countries = [r for r in rows if r['kind'] == 'country' and r['value'] is not None]
        eligible = [r for r in countries if r['population'] > 1_000_000]
        base = dict(year=year, sex=sex, age=0, unit='years', kind='summary', population=None)
        best = max((r['value'] for r in eligible), default=None)
        winners = sorted(r['code'] for r in eligible if r['value'] == best)
        result.append(dict(base, code='frontier', series='frontier', entity='Best practice',
                           value=best, winners=winners))
        for percentile in (10, 25):
            value = weighted_percentile(countries, percentile / 100) if year >= 1950 else None
            code = f'p{percentile}'
            result.append(dict(base, code=code, series=code,
                               entity=f'{percentile}th percentile (population-weighted)',
                               value=value, winners=[],
                               threshold_locations=sorted(r['code'] for r in countries if r['value'] == value),
                               population_weight='sex_specific',
                               covered_population=sum(r['sex_population'] for r in countries) if year >= 1950 else None,
                               country_count=len(countries)))
        for code, label in GROUPS.items():
            group = next((r for r in rows if r['code'] == code), None)
            if year < 1950:
                result.append(dict(base, code=code, series=code, entity=label, value=None, winners=[]))
                continue
            if group is None or group['value'] is None:
                raise ValueError(f'Missing source aggregate observation: {code}, {sex}, {year}')
            result.append(dict(base, code=code, series=code, entity=label, value=group['value'], winners=[]))
    return result


def validate_metadata(metadata):
    for sex in SEX_COLUMNS:
        key = f'Life expectancy - Sex: {sex} - Age: 0 - Variant: estimates'
        column = metadata['columns'][key]
        if column['unit'] != 'years' or 'World Population Prospects (2024)' not in column['citationShort']:
            raise ValueError('Unexpected source release or units; review the source mapping')
    population = metadata['columns']['Population - Sex: all - Age: all - Variant: estimates']
    if population['unit'] != 'people' or 'World Population Prospects (2024)' not in population['citationShort']:
        raise ValueError('Unexpected population source or units')


def main():
    data = ROOT / 'output/data'
    modern, metadata, dependency, version = read_snapshot(data, 'headline')
    history, history_meta, history_dep, history_version = read_snapshot(data, 'headline_history')
    population, population_meta, population_dep, population_version = read_snapshot(data, 'headline_population')
    validate_metadata(metadata)
    for sex in SEX_COLUMNS:
        if history_meta['columns'][f'Period life expectancy - Sex: {sex} - Age: 0']['unit'] != 'years':
            raise ValueError('Unexpected historical life expectancy units')
    if population_meta['columns']['Population (historical)']['unit'] != 'people':
        raise ValueError('Unexpected historical population units')
    observations = standardise(modern)
    observations = historical_observations(history, population, observations) + observations
    weights, weight_dependencies, weight_sources, weight_versions = {}, [], [], {}
    for sex in SEX_COLUMNS:
        raw, meta, manifest, weight_version = read_snapshot(data, f'headline_population_{sex}')
        weights.update(population_by_sex(raw, meta, sex))
        weight_dependencies.append(manifest)
        weight_sources.append(meta['chart'])
        weight_versions[sex] = weight_version
    attach_population_weights(observations, weights)
    rows = calculate(observations)
    directory = data / 'standardised/owid/headline'
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / 'observations.csv').open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(observations[0]))
        writer.writeheader()
        writer.writerows(observations)
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    write_json(directory / 'headline.json', {
        'id': 'headline-life-expectancy', 'title': 'Life expectancy at birth',
        'start_year': 1900, 'end_year': 2023, 'default_sex': 'female',
        'source': 'HMD (2025); UN WPP (2024); historical population: HYDE/Gapminder', 'source_url': metadata['chart']['originalChartUrl'],
        'snapshot_version': version,
        'percentile_population_weights': 'sex_specific',
        'population_snapshots': weight_versions,
        'historical_snapshots': {'life_expectancy': history_version, 'population': population_version},
        'sources': [m['chart'] for m in (metadata, history_meta, population_meta)] + weight_sources, 'code_revision': revision,
        'code_dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)),
        'rows': rows,
    })
    artifacts = [directory / 'observations.csv', directory / 'headline.json']
    write_manifest({p: p.relative_to(data).as_posix() for p in artifacts}, directory / 'publish.json',
                   kind='standardised', dependencies=[dependency, history_dep, population_dep] + weight_dependencies)
    print(f'Calculated {len(rows):,} headline observations')


if __name__ == '__main__':
    main()
