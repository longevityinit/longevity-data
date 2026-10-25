import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src/pipeline'))
from indicators.headline import standardise as raw_standardise, calculate, historical_observations, weighted_percentile, attach_population_weights


def standardise(rows):
    observations = raw_standardise(rows)
    for row in observations:
        row['sex_population'] = row['population']
    return observations


def source(code, female, male, population=2_000_000, year=2000, region='Europe'):
    return {'Code': code, 'Entity': code, 'Year': str(year),
            'Life expectancy of women': str(female), 'Life expectancy of men': str(male),
            'Population': str(population), 'World region according to OWID': region}


def groups():
    return [source('OWID_HIC', 75, 70, region=''), source('OWID_LIC', 55, 50, region=''),
            source('OWID_WRL', 72, 67, region='')]


class HeadlineTests(unittest.TestCase):
    def test_sex_weights_change_percentiles_but_not_frontier_eligibility(self):
        observations = raw_standardise([source('AAA', 40, 40, 2_000_000),
                                       source('BBB', 80, 80, 2_000_000)] + groups())
        weights = {('AAA', 2000, 'female'): 100_000, ('BBB', 2000, 'female'): 1_900_000,
                   ('AAA', 2000, 'male'): 1_900_000, ('BBB', 2000, 'male'): 100_000}
        attach_population_weights(observations, weights)
        lookup = {(r['series'], r['sex']): r for r in calculate(observations)}
        self.assertEqual(lookup['p10', 'female']['value'], 80)
        self.assertEqual(lookup['p10', 'male']['value'], 40)
        self.assertEqual(lookup['frontier', 'male']['value'], 80)
        with self.assertRaises(ValueError): attach_population_weights(observations, {})

    def test_weighted_percentiles_boundaries_ties_and_missing(self):
        rows = [{'code': 'A', 'value': 40, 'sex_population': 10},
                {'code': 'B', 'value': 60, 'sex_population': 15},
                {'code': 'C', 'value': 80, 'sex_population': 75}]
        self.assertEqual(weighted_percentile(rows, .1), 40)
        self.assertEqual(weighted_percentile(rows, .25), 60)
        self.assertEqual(weighted_percentile(rows, .26), 80)
        self.assertEqual(weighted_percentile(list(reversed(rows)), .25), 60)
        rows.append({'code': 'D', 'value': None, 'sex_population': 1000})
        self.assertEqual(weighted_percentile(rows, .25), 60)
        rows[0]['value'] = 60
        self.assertEqual(weighted_percentile(rows, .1), 60)
        self.assertIsNone(weighted_percentile([], .1))
        with self.assertRaises(ValueError): weighted_percentile(rows, 0)

    def test_percentile_series_include_small_countries_and_separate_sexes(self):
        rows = standardise([source('AAA', 40, 80, 100_000),
                            source('BBB', 60, 50, 900_000)] + groups())
        lookup = {(r['series'], r['sex']): r for r in calculate(rows)}
        self.assertEqual(lookup['p10', 'female']['value'], 40)
        self.assertEqual(lookup['p25', 'female']['value'], 60)
        self.assertEqual(lookup['p10', 'male']['value'], 50)
        self.assertEqual(lookup['p10', 'female']['covered_population'], 1_000_000)
        self.assertIsNone(lookup['frontier', 'female']['value'])

    def test_threshold_ties_sex_isolation_and_aggregate_exclusion(self):
        rows = [source('AAA', 80, 60), source('BBB', 80, 65),
                source('CCC', 99, 99, 1_000_000), source('DDD', 70, 70, 1_000_001),
                source('UN_EUR', 120, 120, region='')] + groups()
        result = calculate(standardise(rows))
        lookup = {(r['series'], r['sex']): r for r in result}
        self.assertEqual(lookup['frontier', 'female']['winners'], ['AAA', 'BBB'])
        self.assertEqual(lookup['frontier', 'male']['winners'], ['DDD'])
        self.assertEqual(lookup['OWID_WRL', 'female']['value'], 72)
        self.assertEqual(lookup['OWID_WRL', 'male']['value'], 67)
        self.assertEqual(lookup['OWID_WRL', 'female']['kind'], 'summary')
        self.assertFalse(any(r['series'] == 'median' for r in result))
        self.assertEqual(lookup['OWID_HIC', 'female']['value'], 75)
        self.assertEqual(lookup['UN_EUR', 'female']['value'], 120)
        self.assertEqual(lookup['UN_EUR', 'female']['kind'], 'summary')
        self.assertEqual(lookup['p75', 'female']['value'], 80)
        self.assertEqual(lookup['p90', 'female']['value'], 99)

    def test_missing_values_remain_missing_and_do_not_change_world_series(self):
        rows = standardise([source('AAA', '', 60), source('BBB', 80, 65)] + groups())
        result = calculate(rows)
        self.assertIsNone(next(r for r in result if r['series'] == 'AAA' and r['sex'] == 'female')['value'])
        self.assertEqual(next(r for r in result if r['series'] == 'OWID_WRL' and r['sex'] == 'female')['value'], 72)

    def test_frontier_requires_two_eligible_countries_for_each_sex(self):
        observations = standardise([source('AAA', 80, 70), source('BBB', '', 65),
                                    source('CCC', 99, 99, 1_000_000)] + groups())
        lookup = {(r['series'], r['sex']): r for r in calculate(observations)}
        self.assertIsNone(lookup['frontier', 'female']['value'])
        self.assertEqual(lookup['frontier', 'female']['winners'], [])
        self.assertEqual(lookup['frontier', 'male']['value'], 70)
        self.assertEqual(lookup['frontier', 'male']['winners'], ['AAA'])

    def test_frontier_starts_in_1850_but_preserves_earlier_country_values(self):
        current = standardise([source('AAA', 80, 75), source('BBB', 85, 78)])
        history = [{'Code': code, 'Entity': code, 'Year': str(year), 'Female': str(female), 'Male': str(male)}
                   for year in (1849, 1850)
                   for code, female, male in [('AAA', 50, 45), ('BBB', 55, 48)]]
        population = [{'Code': code, 'Year': str(year), 'Population': '2000000'}
                      for year in (1849, 1850) for code in ('AAA', 'BBB')]
        observations = historical_observations(history, population, current)
        lookup = {(r['series'], r['sex'], r['year']): r for r in calculate(observations)}
        for sex in ('female', 'male'):
            self.assertIsNone(lookup['frontier', sex, 1849]['value'])
            self.assertEqual(lookup['frontier', sex, 1849]['winners'], [])
            self.assertIsNotNone(lookup['AAA', sex, 1849]['value'])
            self.assertEqual(lookup['frontier', sex, 1850]['winners'], ['BBB'])
            self.assertEqual(lookup['frontier', sex, 1850]['value'], 55 if sex == 'female' else 48)

    def test_duplicate_missing_population_and_invalid_values_fail(self):
        row = source('AAA', 80, 70)
        for rows in ([row, row], [source('AAA', 80, 70, '')], [source('AAA', 'nan', 70)]):
            with self.assertRaises(ValueError): standardise(rows)
        with self.assertRaises(ValueError): calculate(standardise([row]))

    def test_history_joins_by_code_year_and_has_no_income_values(self):
        current = standardise([source('AAA', 80, 70)])
        history = [{'Code': 'AAA', 'Entity': 'Old name', 'Year': '1900', 'Female': '50', 'Male': '45'}]
        population = [{'Code': 'AAA', 'Year': '1900', 'Population': '2000000'}]
        result = calculate(historical_observations(history, population, current))
        self.assertIsNone(next(r for r in result if r['series'] == 'frontier' and r['sex'] == 'female')['value'])
        self.assertTrue(all(r['value'] is None for r in result if r['series'] in ('OWID_HIC', 'OWID_WRL', 'OWID_LIC', 'p10', 'p25')))
        missing_population = historical_observations(history, [], current)
        self.assertEqual(missing_population[0]['value'], 50)
        self.assertIsNone(next(r for r in calculate(missing_population) if r['series'] == 'frontier')['value'])

    def test_early_history_keeps_values_without_inventing_population_or_reference_values(self):
        current = standardise([source('SWE', 85, 80)])
        history = [
            {'Code': 'SWE', 'Entity': 'Sweden', 'Year': '1751', 'Female': '39.93', 'Male': '36.87'},
            {'Code': 'SWE', 'Entity': 'Sweden', 'Year': '1760', 'Female': '40', 'Male': ''},
            {'Code': 'OTHER', 'Entity': 'Unknown', 'Year': '1750', 'Female': '99', 'Male': '99'},
            {'Code': 'SWE', 'Entity': 'Sweden', 'Year': '1950', 'Female': '99', 'Male': '99'},
        ]
        population = [{'Code': 'SWE', 'Year': '1760', 'Population': '1915532'}]
        observations = historical_observations(history, population, current)
        self.assertEqual(len(observations), 4)
        lookup = {(r['series'], r['sex'], r['year']): r for r in calculate(observations)}
        self.assertEqual(lookup['SWE', 'female', 1751]['value'], 39.93)
        self.assertEqual(lookup['SWE', 'male', 1751]['value'], 36.87)
        self.assertIsNone(lookup['SWE', 'female', 1751]['population'])
        self.assertIsNone(lookup['frontier', 'female', 1751]['value'])
        self.assertIsNone(lookup['frontier', 'female', 1760]['value'])
        self.assertIsNone(lookup['frontier', 'male', 1760]['value'])
        self.assertTrue(all(r['value'] is None for r in lookup.values() if r['series'] in ('OWID_WRL', 'p10', 'p25', 'p75', 'p90')))
        with self.assertRaises(ValueError): historical_observations(history + [history[0]], population, current)


if __name__ == '__main__':
    unittest.main()
