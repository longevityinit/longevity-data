import { test, expect } from '@playwright/test';

const base = '/?sex=f&show=best,world,p10,AND&start=1900&end=2023';
async function loaded(page, url = base) {
  await page.goto(url);
  await expect(page.locator('#status')).toBeHidden();
  await expect(page.locator('#chart > svg')).toBeVisible();
}

test('selection, sex, clear, URL reload, and local persistence stay synchronized', async ({
  page,
}) => {
  await loaded(page);
  await page.getByRole('checkbox', { name: 'Sweden', exact: true }).check();
  await expect(page).toHaveURL(/SWE/);
  await page.getByRole('radio', { name: 'Men', exact: true }).focus();
  await page.keyboard.press('Space');
  await expect(page).toHaveURL(/sex=m/);
  await page.reload();
  await expect(page.getByRole('checkbox', { name: 'Sweden', exact: true })).toBeChecked();
  await expect(page.getByRole('radio', { name: 'Men', exact: true })).toBeChecked();
  await page.goto('/');
  await expect(page.getByRole('checkbox', { name: 'Sweden', exact: true })).toBeChecked();
  await page.getByRole('button', { name: 'Clear selection', exact: true }).click();
  await expect(page).toHaveURL(/show=best%2Cworld(?:&|$)/);
  await expect(page.locator('#chart .summary-line')).toHaveCount(2);
  await expect(page.locator('#picker-heading-count')).toHaveText('0 comparisons');
});

test('keyboard range editing, handle boundaries, reset and the lazy table', async ({ page }) => {
  await loaded(page);
  await expect(page.locator('#chart-title')).toHaveText('Life expectancy at birth');
  await expect(page.locator('#chart-subtitle')).toHaveText('Women · 1900–2023');
  await expect(page.locator('#values tr')).toHaveCount(0);
  const start = page.getByRole('textbox', { name: 'Start year entry' });
  await start.fill('2000');
  await start.press('Enter');
  await expect(page).toHaveURL(/start=2000/);
  const handle = page.getByRole('slider', { name: 'Start year', exact: true });
  await expect(handle).toHaveValue('2000');
  await handle.focus();
  await handle.press('ArrowRight');
  await expect(page).toHaveURL(/start=2001/);
  await handle.press('End');
  await expect(page).toHaveURL(/start=2022/);
  await handle.press('Escape');
  await expect(page).toHaveURL(/start=1850/);
  await page.getByRole('button', { name: 'View data', exact: true }).click();
  await expect(page.locator('#values tr')).toHaveCount(174);
  await page.getByRole('button', { name: 'Close data table' }).click();
  await start.fill('2000');
  await start.press('Enter');
  await expect(page.locator('#values tr')).toHaveCount(174);
  await page.getByRole('button', { name: 'View data', exact: true }).click();
  await expect(page.locator('#values tr')).toHaveCount(24);
  await page.getByRole('button', { name: 'Close data table' }).click();
  await start.fill('2010');
  await start.press('Enter');
  await expect(page).toHaveURL(/start=2010/);
  await expect(page.locator('#values tr')).toHaveCount(24);
  await page.getByRole('button', { name: 'View data', exact: true }).click();
  await expect(page.locator('#values tr')).toHaveCount(14);
});

test('missing tooltip rows stay omitted, pinning and Escape work, endpoints are not focusable', async ({
  page,
}) => {
  await loaded(page);
  const svg = page.locator('#chart > svg');
  const box = await svg.boundingBox();
  const plotWidth = box.width - 64 - 175;
  const x = box.x + 64 + (plotWidth * 4) / 123;
  const y = box.y + 120;
  await page.mouse.move(x, y);
  await expect(page.getByRole('tooltip')).toContainText('1904');
  await expect(page.locator('#tooltip .tooltip-row')).toHaveCount(1);
  await page.mouse.click(x, y);
  await expect(page.getByRole('dialog', { name: 'Life expectancy in 1904' })).toBeVisible();
  await expect(page.locator('#tooltip .tooltip-row')).toHaveCount(1);
  await expect(page.getByRole('button', { name: 'Close year values' })).toBeFocused();
  await page.keyboard.press('Escape');
  await expect(page.locator('#tooltip')).toBeHidden();
  await expect(svg).not.toHaveAttribute('tabindex');
  await expect(page.locator('.line-label[tabindex]')).toHaveCount(0);
});

test('mobile controls collapse, marker labels hide, labels prioritize references without overlap', async ({
  page,
}) => {
  const codes = Array.from({ length: 15 }, (_, i) => `C${String(i).padStart(2, '0')}`);
  await page.setViewportSize({ width: 375, height: 812 });
  await loaded(page, `/?sex=f&show=best,world,p10,p25,${codes.join(',')}&start=1950&end=2023`);
  await expect(page.locator('#country-picker')).not.toHaveAttribute('open');
  await expect(page.locator('.year-marker:visible')).toHaveCount(0);
  await expect(page.locator('.line-label')).toHaveCount(4);
  for (const code of ['frontier', 'OWID_WRL', 'p10', 'p25']) {
    await expect(page.locator(`.line-label[data-series="${code}"]`)).toHaveCount(1);
  }
  const boxes = await page.locator('.line-label rect').evaluateAll((nodes) =>
    nodes
      .map((node) => {
        const box = node.getBoundingClientRect();
        return { top: box.top, bottom: box.bottom };
      })
      .sort((a, b) => a.top - b.top),
  );
  for (let i = 1; i < boxes.length; i++)
    expect(boxes[i].top).toBeGreaterThanOrEqual(boxes[i - 1].bottom);
  await page.getByText('Show on chart', { exact: true }).click();
  await page.getByRole('button', { name: 'Selected', exact: true }).click();
  await expect(page.getByRole('checkbox', { name: 'Country 00', exact: true })).toBeChecked();
});

test('a URL outside selected coverage normalizes controls, chart and URL', async ({ page }) => {
  await loaded(page, '/?sex=f&show=best,world&start=1751&end=1800');
  await expect(page).toHaveURL(/start=1850&end=2023/);
  await expect(page.locator('#start-value')).toHaveValue('1850');
  await expect(page.locator('#end-value')).toHaveValue('2023');
  await expect(page.locator('#chart-subtitle')).toContainText('1850–2023');
});

test('dragging the paired slider snaps to a preset without eagerly building the table', async ({
  page,
}) => {
  await loaded(page);
  const timeline = await page.locator('#timeline').boundingBox();
  const minimum = Number(await page.locator('#start-year').getAttribute('min'));
  const maximum = Number(await page.locator('#start-year').getAttribute('max'));
  const position = (year) => timeline.x + ((year - minimum) / (maximum - minimum)) * timeline.width;
  await page.mouse.move(position(1900), timeline.y + 18);
  await page.mouse.down();
  await page.mouse.move(position(1952), timeline.y + 18, { steps: 12 });
  await page.mouse.up();
  await expect(page).toHaveURL(/start=1950/);
  await expect(page.locator('#values tr')).toHaveCount(0);
  await page.getByRole('slider', { name: 'End year', exact: true }).focus();
  await page.keyboard.press('Home');
  await expect(page).toHaveURL(/end=1951/);
  await page.keyboard.press('Escape');
  await expect(page).toHaveURL(/end=2023/);
});

test('queued redraws preserve an uncommitted year edit', async ({ page }) => {
  await loaded(page);
  const start = page.getByRole('textbox', { name: 'Start year entry' });
  await start.fill('2010');
  const previousChart = await page.locator('#chart > svg').elementHandle();
  await page.setViewportSize({ width: 1100, height: 900 });
  // Wait for the ResizeObserver-driven render, not merely a browser resize event.
  await expect.poll(() => previousChart.evaluate((svg) => svg.isConnected)).toBe(false);
  await expect(start).toHaveValue('2010');
  await start.press('Enter');
  await expect(page).toHaveURL(/start=2010/);
});

test('age-65 configuration reuses controls with correct units and separate saved selections', async ({
  page,
}) => {
  await page.route('**/headline.json', async (route) => {
    const response = await route.fetch();
    const data = await response.json();
    data.age = 65;
    data.source_urls = { female: 'https://example.org/female', male: 'https://example.org/male' };
    data.rows = data.rows.map((row) => ({ ...row, value: row.value / 4 }));
    await route.fulfill({ response, json: data });
  });
  await loaded(page);
  await expect(page.locator('#chart-title')).toHaveText('Life expectancy at age 65');
  await expect(page.locator('#chart-subtitle')).toContainText('Remaining years of life · Women');
  await expect(page.locator('#chart > svg')).toContainText(
    'Remaining life expectancy at age 65 (years)',
  );
  await page.getByRole('button', { name: 'View data', exact: true }).click();
  await expect(page.locator('#table-caption')).toContainText('remaining life expectancy at age 65');
  await page.getByRole('button', { name: 'Close data table' }).click();
  await page.getByRole('checkbox', { name: 'Sweden', exact: true }).check();
  expect(await page.evaluate(() => localStorage.getItem('headline-age65-lines'))).toContain('SWE');
  expect(await page.evaluate(() => localStorage.getItem('headline-lines'))).toBeNull();
  await page.getByRole('radio', { name: 'Men', exact: true }).focus();
  await page.keyboard.press('Space');
  await expect(page.locator('#chart-subtitle')).toContainText('Men');
  await expect(page.locator('#source')).toHaveAttribute('href', 'https://example.org/male');
  await page.getByRole('radio', { name: 'Women', exact: true }).focus();
  await page.keyboard.press('Space');
  await expect(page.locator('#source')).toHaveAttribute('href', 'https://example.org/female');
});

test('data dialogs support keyboard dismissal, focus return, mobile layout and downloads', async ({
  page,
}) => {
  await page.setViewportSize({ width: 375, height: 812 });
  await loaded(page);
  await expect(page.locator('.chart-main .chart-footer #source')).toBeVisible();
  await expect(page.locator('.chart-main .chart-footer #view-data')).toBeVisible();
  await expect(page.locator('.chart-main .chart-footer #download-data')).toBeVisible();
  const view = page.getByRole('button', { name: 'View data', exact: true });
  await view.focus();
  await page.keyboard.press('Enter');
  const table = page.getByRole('dialog', { name: 'Data and methodology', exact: true });
  await expect(table).toBeVisible();
  const tableTab = table.getByRole('tab', { name: 'Table', exact: true });
  await expect(tableTab).toHaveAttribute('aria-selected', 'true');
  await table.getByRole('tab', { name: 'Methodology', exact: true }).click();
  await expect(table.locator('#methodology')).toBeVisible();
  await expect(table.locator('#panel-table')).toBeHidden();
  await expect(table.locator('#methodology')).toContainText(
    'without interpolating between countries',
  );
  await table.getByRole('tab', { name: 'Methodology', exact: true }).press('ArrowLeft');
  await expect(table.getByRole('tab', { name: 'Sources and credits' })).toBeFocused();
  await expect(table.locator('#panel-sources')).toBeVisible();
  await table.getByRole('tab', { name: 'Sources and credits' }).press('Home');
  await expect(tableTab).toBeFocused();
  await page.keyboard.press('Tab');
  await expect(page.getByRole('region', { name: 'Annual life expectancy values' })).toBeFocused();
  await page.keyboard.press('Escape');
  await expect(table).not.toBeVisible();
  await expect(view).toBeFocused();
  const download = page.getByRole('button', { name: 'Download', exact: true });
  await download.click();
  const panel = page.getByRole('dialog', { name: 'Download data', exact: true });
  await expect(panel).toBeVisible();
  await expect(panel).toContainText('not limited to your current selection');
  const box = await panel.boundingBox();
  expect(box.x).toBeGreaterThanOrEqual(0);
  expect(box.x + box.width).toBeLessThanOrEqual(375);
  for (const [format, filename] of [
    ['CSV', 'observations.csv'],
    ['JSON', 'headline.json'],
  ]) {
    const pending = page.waitForEvent('download');
    await panel.getByRole('link').filter({ hasText: format }).click();
    const file = await pending;
    expect(file.suggestedFilename()).toBe(filename);
    expect(await file.failure()).toBeNull();
  }
  await page.getByRole('button', { name: 'Close downloads' }).click();
  await expect(download).toBeFocused();
  await view.click();
  await page.mouse.click(2, 2);
  await expect(table).not.toBeVisible();
  await expect(view).toBeFocused();
});

test('timeline bounds follow selected data and remain stable while zooming', async ({ page }) => {
  await loaded(page, '/?sex=f&show=best,world&start=1751&end=2023');
  await expect(page.locator('#start-year')).toHaveAttribute('min', '1850');
  await expect(page.locator('#start-value')).toHaveValue('1850');
  await expect(page.locator('.year-marker[data-year="1850"]')).toBeVisible();
  await expect(page.locator('.year-marker[data-year="1900"]')).toBeVisible();
  await page.getByRole('checkbox', { name: 'Sweden', exact: true }).check();
  await expect(page.locator('#start-year')).toHaveAttribute('min', '1751');
  await expect(page.locator('.year-marker[data-year="1800"]')).toBeVisible();
  const field = page.getByRole('textbox', { name: 'Start year entry' });
  await field.fill('2000');
  await field.press('Enter');
  await expect(page.locator('#start-year')).toHaveValue('2000');
  await expect(page.locator('#start-year')).toHaveAttribute('min', '1751');
  await page.getByRole('button', { name: 'Clear selection', exact: true }).click();
  await expect(page.locator('#start-year')).toHaveAttribute('min', '1850');
});

test('Best practice definition opens separately from year values', async ({ page }) => {
  await loaded(page);
  const button = page.getByRole('button', { name: 'About Best practice' });
  const popup = page.locator('#best-practice-info');
  await expect(popup).toBeHidden();
  await button.click();
  await expect(popup).toBeVisible();
  await expect(popup).toContainText('more than one million');
  await expect(page.locator('#tooltip')).toBeHidden();
  await page.keyboard.press('Escape');
  await expect(popup).toBeHidden();
  await button.focus();
  await page.keyboard.press('Enter');
  await expect(popup).toBeVisible();
  await page.getByRole('heading', { level: 1 }).click();
  await expect(popup).toBeHidden();
  await expect(page.locator('.chart-note')).toHaveCount(0);
  await page.getByRole('button', { name: 'View data', exact: true }).click();
  await expect(page.locator('#chart-data .coverage-note')).toHaveText(
    'Historical coverage is limited before 1950.',
  );
});

test('wide stacked selector uses a compact single row of filters', async ({ page }) => {
  await page.setViewportSize({ width: 895, height: 828 });
  await loaded(page);
  const buttons = await page.locator('.picker-filters button').all();
  const boxes = await Promise.all(buttons.map((button) => button.boundingBox()));
  expect(boxes.every((box) => Math.abs(box.y - boxes[0].y) < 1)).toBe(true);
  const filters = await page.locator('.picker-filters').boundingBox();
  expect(filters.width).toBeLessThan(500);
});
