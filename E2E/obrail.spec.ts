import { test, expect } from '@playwright/test';



const BASE = process.env.BASE_URL || 'http://localhost:3002';

// ── Page Accueil ──────────────────────────────────────────────

test.describe('Page Accueil', () => {

  test('charge et affiche le titre ObRail', async ({ page }) => {
    await page.goto(BASE);
    await expect(page).toHaveTitle(/ObRail/);
  });

  test('affiche le KPI Trains total', async ({ page }) => {
    await page.goto(BASE);
    await page.waitForSelector('text=Trains total', { timeout: 10000 });
    const kpi = page.locator('text=Trains total');
    await expect(kpi).toBeVisible();
  });

  test('contient les opérateurs SNCF Deutsche Bahn ou SNCB', async ({ page }) => {
    await page.goto(BASE);
    await page.waitForLoadState('networkidle');
    const body = await page.textContent('body');
    const ok = body?.includes('SNCF') || body?.includes('Deutsche Bahn') || body?.includes('SNCB');
    expect(ok).toBeTruthy();
  });

  test('la navigation est visible', async ({ page }) => {
    await page.goto(BASE);
    await page.waitForLoadState('networkidle');
    const nav = page.locator('nav, header').first();
    await expect(nav).toBeVisible();
  });

});

// ── Page Horaires ─────────────────────────────────────────────

test.describe('Page Horaires', () => {

  test('se charge sans erreur', async ({ page }) => {
    await page.goto(`${BASE}/horaires`);
    await page.waitForLoadState('networkidle');
    await expect(page.locator('body')).toBeVisible();
  });

  test('affiche le label Operateur', async ({ page }) => {
    await page.goto(`${BASE}/horaires`);
    await page.waitForLoadState('networkidle');
    const label = page.locator('text=Operateur').first();
    await expect(label).toBeVisible();
  });

  test('le bouton Export CSV est présent apres chargement', async ({ page }) => {
    await page.goto(`${BASE}/horaires`);
    await page.waitForTimeout(4000);
    const csvBtn = page.locator('text=Export CSV');
    await expect(csvBtn).toBeVisible({ timeout: 10000 });
  });

  test('le select Type service propose Jour et Nuit', async ({ page }) => {
    await page.goto(`${BASE}/horaires`);
    await page.waitForLoadState('networkidle');
    const select = page.locator('[role="combobox"]').nth(1);
    await select.click();
    await expect(page.locator('[role="option"]:has-text("Jour")').first()).toBeVisible();
    await expect(page.locator('[role="option"]:has-text("Nuit")').first()).toBeVisible();
  });

});

// ── Page CO2 ──────────────────────────────────────────────────

test.describe('Page CO2', () => {

  test('se charge et contient CO2', async ({ page }) => {
    await page.goto(`${BASE}/co2`);
    await page.waitForLoadState('networkidle');
    const body = await page.textContent('body');
    expect(body?.toLowerCase()).toContain('co2');
  });

  test('mentionne l avion pour le comparatif', async ({ page }) => {
    await page.goto(`${BASE}/co2`);
    await page.waitForLoadState('networkidle');
    const body = await page.textContent('body');
    expect(body?.toLowerCase()).toContain('avion');
  });

});

// ── Page Statistiques ─────────────────────────────────────────

test.describe('Page Statistiques', () => {

  test('se charge correctement', async ({ page }) => {
    await page.goto(`${BASE}/statistiques`);
    await page.waitForLoadState('networkidle');
    await expect(page.locator('body')).toBeVisible();
  });

  test('affiche des graphiques SVG', async ({ page }) => {
    await page.goto(`${BASE}/statistiques`);
    await page.waitForTimeout(3000);
    const charts = page.locator('svg').first();
    await expect(charts).toBeVisible({ timeout: 10000 });
  });

});

// ── Page Liaisons ─────────────────────────────────────────────

test.describe('Page Liaisons', () => {

  test('se charge sans erreur', async ({ page }) => {
    await page.goto(`${BASE}/liaisons`);
    await page.waitForLoadState('networkidle');
    await expect(page.locator('body')).toBeVisible();
  });

  test('contient le mot liaison', async ({ page }) => {
    await page.goto(`${BASE}/liaisons`);
    await page.waitForLoadState('networkidle');
    const body = await page.textContent('body');
    expect(body?.toLowerCase()).toContain('liaison');
  });

});

// ── Page Qualite ──────────────────────────────────────────────

test.describe('Page Qualite', () => {

  test('se charge correctement', async ({ page }) => {
    await page.goto(`${BASE}/qualite`);
    await page.waitForLoadState('networkidle');
    await expect(page.locator('body')).toBeVisible();
  });

  test('affiche la section RGPD', async ({ page }) => {
    await page.goto(`${BASE}/qualite`);
    await page.waitForLoadState('networkidle');
    const body = await page.textContent('body');
    expect(body?.toLowerCase()).toContain('rgpd');
  });

});

// ── Navigation et performance ─────────────────────────────────

test.describe('Navigation globale', () => {

  test('toutes les pages chargent en moins de 10 secondes', async ({ page }) => {
    const routes = ['/', '/horaires', '/co2', '/statistiques', '/liaisons', '/qualite'];
    for (const path of routes) {
      const start = Date.now();
      await page.goto(`${BASE}${path}`);
      await page.waitForLoadState('domcontentloaded');
      const duration = Date.now() - start;
      expect(duration).toBeLessThan(10000);
    }
  });

  test('la balise html a un attribut lang pour accessibilite', async ({ page }) => {
    await page.goto(BASE);
    const lang = await page.getAttribute('html', 'lang');
    expect(lang).toBeTruthy();
  });

  test('pas d erreur console critique sur la page accueil', async ({ page }) => {
    const errors: string[] = [];
    page.on('console', msg => {
      if (msg.type() === 'error') errors.push(msg.text());
    });
    await page.goto(BASE);
    await page.waitForLoadState('networkidle');
    const critiques = errors.filter(e =>
      !e.includes('favicon') && !e.includes('analytics') && !e.includes('ERR_')
    );
    expect(critiques.length).toBe(0);
  });

});

// ── Connectivite API ──────────────────────────────────────────

test.describe('Connectivite API', () => {

  test('le frontend charge sans erreur 500', async ({ page }) => {
    const responses: number[] = [];
    page.on('response', res => responses.push(res.status()));
    await page.goto(BASE);
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(2000);
    const errors500 = responses.filter(s => s >= 500);
    expect(errors500.length).toBe(0);
  });

});
