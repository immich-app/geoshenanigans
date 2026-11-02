/**
 * Every object a deployment published, with its size. Publishing writes it after everything else, so its presence
 * means the upload finished, and a listing compared against it shows whether anything has gone since.
 */
export const INVENTORY_NAME = 'inventory.json';

export type Inventory = { objects: Record<string, number> };

/** What the listing is missing, or has at a different size, compared with the inventory. */
export function inventoryProblems(inventory: Inventory, listing: Map<string, number>): string[] {
  const problems: string[] = [];
  for (const [name, size] of Object.entries(inventory.objects)) {
    const listed = listing.get(name);
    if (listed === undefined) {
      problems.push(`${name} is missing`);
    } else if (listed !== size) {
      problems.push(`${name} is ${listed} bytes, published ${size}`);
    }
  }
  return problems;
}
