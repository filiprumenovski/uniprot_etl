import { expect, test } from "@playwright/test";

test("dashboard renders", async ({ page }) => {
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Pipeline Dashboard" })
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Start Pipeline" })
  ).toBeVisible();
});
