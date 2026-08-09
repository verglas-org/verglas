#!/usr/bin/env node
import { chromium } from "playwright";

const BASE = process.env.DEBUG_BASE_URL || "http://127.0.0.1:8787";
const USER = process.env.DEBUG_USER || "testuser";
const PASS = process.env.DEBUG_PASS || "Test123!";
const PROMPT = process.env.DEBUG_PROMPT ||
  "Say hello in one short sentence so I can verify the agent works.";

const logs = [];
const pageErrors = [];

function capture(msg) {
  logs.push(msg);
  console.log(msg);
}

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext();
const page = await context.newPage();
page.on("console", (msg) => capture(`[console.${msg.type()}] ${msg.text()}`));
page.on("pageerror", (err) => {
  pageErrors.push(String(err));
  capture(`[pageerror] ${err}`);
});
page.on("response", async (res) => {
  if (res.status() >= 400) {
    capture(`[http ${res.status()}] ${res.request().method()} ${res.url()}`);
  }
});

await page.goto(BASE, { waitUntil: "networkidle", timeout: 60_000 });
capture(`[nav] landed on ${page.url()} title=${await page.title()}`);
await page.screenshot({ path: "/tmp/verglas-debug-1-login.png", fullPage: true });

// Login if needed
const passwordInput = page.locator('input[type="password"]').first();
if (await passwordInput.count()) {
  capture("[auth] login form detected");
  const userInput = page.locator('input[type="text"], input[name="username"], input[autocomplete="username"]').first();
  await userInput.fill(USER);
  await passwordInput.fill(PASS);
  const submit = page.getByRole("button", { name: /log in|sign in|continue/i }).first();
  await submit.click();
  await page.waitForTimeout(2500);
  capture(`[auth] after login url=${page.url()}`);
  await page.screenshot({ path: "/tmp/verglas-debug-2-after-login.png", fullPage: true });
} else {
  capture("[auth] no login form; already authenticated?");
}

// Dump visible text clues
const bodyText = await page.locator("body").innerText();
capture(`[body-snippet]\n${bodyText.slice(0, 2500)}`);

// Look for model / onboarding blockers
const blockers = [
  /needs an AI model/i,
  /configure.*model/i,
  /add.*model/i,
  /link an AI runtime/i,
  /Create a .* account/i,
  /onboarding/i,
  /error/i,
  /failed/i,
  /disconnected/i,
];
for (const re of blockers) {
  if (re.test(bodyText)) capture(`[signal] matched ${re}`);
}

// Try to open providers / add model if prompted
const addModelBtn = page.getByRole("button", { name: /add model|link.*runtime|configure model|AI providers/i }).first();
if (await addModelBtn.count()) {
  capture(`[ui] clicking ${await addModelBtn.innerText()}`);
  await addModelBtn.click().catch(() => {});
  await page.waitForTimeout(1000);
  await page.screenshot({ path: "/tmp/verglas-debug-3-model.png", fullPage: true });
}

// Navigate to providers if link exists
const providersLink = page.getByRole("link", { name: /providers|AI providers|models/i }).first();
if (await providersLink.count()) {
  capture("[nav] opening providers");
  await providersLink.click();
  await page.waitForTimeout(1500);
  await page.screenshot({ path: "/tmp/verglas-debug-4-providers.png", fullPage: true });
  capture(`[providers]\n${(await page.locator("body").innerText()).slice(0, 2000)}`);
}

// Go home / new chat
await page.goto(`${BASE}/`, { waitUntil: "networkidle" }).catch(() => {});
await page.waitForTimeout(1500);
const composer = page.locator("textarea, [contenteditable='true'], [role='textbox']").first();
if (await composer.count()) {
  capture("[chat] found composer; sending probe prompt");
  await composer.click();
  await composer.fill(PROMPT);
  await page.keyboard.press("Enter");
  await page.waitForTimeout(8000);
  await page.screenshot({ path: "/tmp/verglas-debug-5-chat.png", fullPage: true });
  capture(`[chat-body]\n${(await page.locator("body").innerText()).slice(0, 3500)}`);
} else {
  capture("[chat] no composer found");
  await page.screenshot({ path: "/tmp/verglas-debug-5-no-composer.png", fullPage: true });
}

capture(`[summary] pageErrors=${pageErrors.length}`);
await browser.close();
