/**
 * amazon-mock.ts
 *
 * Simulated Amazon SP-API adapter.
 * Replace the body of fetchProductFromAmazon with real SP-API calls once
 * credentials are configured. The signature and return type stay the same.
 */

export interface AmazonProductResult {
  name: string;
  price: number;
  image_url: string;
  source: "Amazon";
}

/**
 * Look up a product by barcode against the (mock) Amazon SP-API.
 *
 * @returns Product details if found, or null if the barcode is unknown.
 */
export async function fetchProductFromAmazon(
  barcode: string,
): Promise<AmazonProductResult | null> {
  // Simulate SP-API network latency
  await new Promise((r) => setTimeout(r, 800));

  if (barcode === "123456") {
    return {
      name: "Wireless Mouse Pro",
      price: 29.99,
      image_url: "https://via.placeholder.com/150",
      source: "Amazon",
    };
  }

  return null;
}
