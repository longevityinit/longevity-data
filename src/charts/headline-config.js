/* Measure-specific wording; interactions and rendering remain shared. */
export function chartConfig(data) {
  const age = data.age ?? 0;
  if (![0, 65].includes(age)) throw new Error(`Unsupported life-expectancy age: ${age}`);
  const measure = age === 65 ? 'Remaining life expectancy at age 65' : 'Life expectancy at birth';
  return {
    age,
    title: age === 65 ? 'Life expectancy at age 65' : 'Life expectancy at birth',
    subtitlePrefix: age === 65 ? 'Remaining years of life · ' : '',
    measure,
    axisLabel: `${measure} (years)`,
    tooltipLabel: age === 65 ? 'Remaining life expectancy at age 65' : 'Life expectancy',
    storageKey: age === 65 ? 'headline-age65-lines' : 'headline-lines',
  };
}
