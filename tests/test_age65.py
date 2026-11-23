"""Age-65 units, population weights, and age isolation regression coverage."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src/pipeline'))
from indicators.age65 import standardise_age65
from indicators.headline import calculate, population_by_sex


def metadata(sex):
    return {'columns': {f'Life expectancy - Sex: {sex} - Age: 65 - Type: period': {
        'unit': 'years', 'descriptionShort': f'Remaining period life expectancy at age 65 among {sex}s, in a given year.',
        'citationShort': 'Human Mortality Database (2025); UN, World Population Prospects (2024)',
    }}}


class Age65Tests(unittest.TestCase):
    def test_adapter_retains_remaining_years_excludes_aggregates_and_preserves_missing(self):
        reference = [dict(code='SWE', year=2023, sex='female', kind='country', population=10_000_000)]
        raw = [dict(Code='SWE', Entity='Sweden', Year='2023', **{'Age 65': '22.2336'}),
               dict(Code='OWID_WRL', Entity='World', Year='2023', **{'Age 65': '18.9811'}),
               dict(Code='OWID_HIC', Entity='High income', Year='2023', **{'Age 65': '23'}),
               dict(Code='SWE', Entity='Sweden', Year='1751', **{'Age 65': ''})]
        result = standardise_age65(raw, metadata('female'), 'female', reference)
        self.assertEqual([row['value'] for row in result], [22.2336, 18.9811, None])
        self.assertTrue(all(row['age'] == 65 for row in result))
        self.assertIsNone(result[-1]['population'])
        with self.assertRaisesRegex(ValueError, 'Duplicate age-65'):
            standardise_age65(raw + [raw[0]], metadata('female'), 'female', reference)
        invalid = metadata('female')
        next(iter(invalid['columns'].values()))['descriptionShort'] = 'Expected age at death'
        with self.assertRaisesRegex(ValueError, 'definition'):
            standardise_age65(raw, invalid, 'female', reference)
        raw[0]['Age 65'] = 'nan'
        with self.assertRaisesRegex(ValueError, 'SWE'):
            standardise_age65(raw, metadata('female'), 'female', reference)

    def test_population_weights_include_65_through_100_plus_only(self):
        columns = {}
        row = {'Code': 'SWE', 'Year': '2023', '60-64 years': '999999'}
        for age in [f'{start}-{start + 4}' for start in range(65, 100, 5)] + ['100+']:
            columns[f'Population - Sex: female - Age: {age} - Variant: estimates'] = {
                'unit': 'people', 'citationShort': 'World Population Prospects (2024)'}
            row[f'{age} years'] = '10'
        self.assertEqual(population_by_sex([row], {'columns': columns}, 'female', 65),
                         {('SWE', 2023, 'female'): 80})
        del row['100+ years']
        with self.assertRaises(KeyError):
            population_by_sex([row], {'columns': columns}, 'female', 65)

    def test_summaries_isolate_age_and_sex_and_use_total_population_cutoff(self):
        observations = []
        for age in (0, 65):
            for sex in ('female', 'male'):
                # Younger populations favor AAA; age-65+ populations favor BBB.
                for code, value, total, weight in [('AAA', 10, 2_000_000, 1), ('BBB', 20, 3_000_000, 99),
                                                    ('CCC', 30, 1_000_000, 1)]:
                    observations.append(dict(code=code, entity=code, year=2023, sex=sex, age=age,
                                             unit='years', kind='country', population=total,
                                             sex_population=weight, value=value + (50 if age == 0 else 0) + (1 if sex == 'female' else 0)))
                observations.append(dict(code='OWID_WRL', entity='World', year=2023, sex=sex, age=age,
                                         unit='years', kind='group', population=None, sex_population=None,
                                         value=18 if age == 65 else 70))
        rows = calculate(observations)
        for age in (0, 65):
            for sex in ('female', 'male'):
                result = {row['series']: row for row in rows if row['age'] == age and row['sex'] == sex}
                offset = (50 if age == 0 else 0) + (1 if sex == 'female' else 0)
                self.assertEqual(result['frontier']['winners'], ['BBB'])
                self.assertEqual(result['frontier']['value'], 20 + offset)
                self.assertEqual(result['p10']['value'], 20 + offset)
                self.assertEqual(result['OWID_WRL']['value'], 18 if age == 65 else 70)


if __name__ == '__main__':
    unittest.main()
