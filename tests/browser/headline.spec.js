import { test, expect } from '@playwright/test';

const base = '/?sex=f&show=best,world,p10,AND&start=1900&end=2023';
async function loaded(page, url = base) {
  await page.goto(url);
  await expect(page.locator('#status')).toBeHidden();
  await expect(page.locator('#chart svg')).toBeVisible();
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
  await expect(page.locator('#values tr')).toHaveCount(0);
  const start = page.getByRole('textbox', { name: 'Start year entry' });
  await start.fill('2000');
  await start.press('Enter');
  await expect(page).toHaveURL(/start=2000/);
  const handle = page.getByRole('slider', { name: 'Start year', exact: true });
  await handle.focus();
  await handle.press('ArrowRight');
  await expect(page).toHaveURL(/start=2001/);
  await handle.press('End');
  await expect(page).toHaveURL(/start=2022/);
  await handle.press('Escape');
  await expect(page).toHaveURL(/start=1751/);
  await page.getByRole('button', { name: 'View data', exact: true }).click();
  await expect(page.locator('#values tr')).toHaveCount(174);
  await start.fill('2000');
  await start.press('Enter');
  await expect(page.locator('#values tr')).toHaveCount(24);
  await page.getByRole('button', { name: 'View data', exact: true }).click();
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
  const svg = page.locator('#chart svg');
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

test('a range without observations reports the gap and can recover through the year controls', async ({
  page,
}) => {
  await page.goto('/?sex=f&show=best,world&start=1751&end=1800');
  await expect(page.locator('#status')).toHaveText('No observations available for this selection.');
  await expect(page.locator('#chart svg')).toHaveCount(0);
  await page.getByRole('slider', { name: 'End year', exact: true }).focus();
  await page.keyboard.press('Escape');
  await expect(page.locator('#chart svg')).toBeVisible();
  await expect(page.locator('#status')).toBeHidden();
  await expect(page.locator('#values tr')).toHaveCount(0);
});

test('dragging the paired slider snaps to a preset without eagerly building the table', async ({
  page,
}) => {
  await loaded(page);
  const timeline = await page.locator('#timeline').boundingBox();
  const position = (year) => timeline.x + ((year - 1751) / (2023 - 1751)) * timeline.width;
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
