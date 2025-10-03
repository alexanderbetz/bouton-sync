import "dotenv/config";
import "@shopify/shopify-api/adapters/node";
import { shopifyApi, ApiVersion } from "@shopify/shopify-api";
import { setTimeout as delay } from "timers/promises";
import { fetch } from "undici";

// ------------------------
// Configuration (env vars)
// ------------------------
const SHOP_DOMAIN = process.env.SHOPIFY_SHOP_DOMAIN || "";
const ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN || "";
const LOCATION_GID = process.env.SHOPIFY_LOCAL_LOCATION_GID || "";
const R2O_API_TOKEN = process.env.R2O_API_TOKEN || "";

if (!SHOP_DOMAIN || !ACCESS_TOKEN || !LOCATION_GID || !R2O_API_TOKEN) {
  console.error(
    "Please set SHOPIFY_SHOP_DOMAIN, SHOPIFY_ACCESS_TOKEN, SHOPIFY_LOCAL_LOCATION_GID, R2O_API_TOKEN"
  );
  process.exit(1);
}

// ------------------------
// Shopify GraphQL client
// ------------------------
const shopify = shopifyApi({
  adminApiAccessToken: ACCESS_TOKEN,
  apiKey: "null",
  apiSecretKey: "null",
  scopes: [],
  hostName: "github-actions",
  apiVersion: ApiVersion.July25,
  isCustomStoreApp: true,
  isEmbeddedApp: false,
});

const graphqlClient = new shopify.clients.Graphql({
  // @ts-ignore
  session: {
    id: `offline_${SHOP_DOMAIN}`,
    shop: SHOP_DOMAIN,
    state: "offline",
    isOnline: false,
    accessToken: ACCESS_TOKEN,
    scope: undefined,
    expires: undefined,
  },
});

async function shopifyGraphql<T = any>(
  query: string,
  variables: Record<string, any> = {}
): Promise<T> {
  const res = await graphqlClient.request(query, { variables });
  if (res.errors && res.errors.message) {
    throw new Error(`GraphQL errors: ${JSON.stringify(res.errors.message)}`);
  }
  return res as T;
}

// ------------------------
// Shopify operations
// ------------------------
async function gqlFindVariantBySku(sku: string): Promise<{
  variantId: string;
  productId: string;
  inventoryItemId: string;
  productTitle: string;
  productType: string;
  currentPrice: number;
} | null> {
  const query = `#graphql
	query($q: String!) {
	  productVariants(first: 1, query: $q) {
		edges { node { id sku price product { id title productType } inventoryItem { id } } }
	  }
	}`;
  const res = await shopifyGraphql<{
    data: { productVariants: { edges: Array<{ node: any }> } };
  }>(query, { q: `sku:${sku}` });
  const edges = res.data?.productVariants?.edges ?? [];
  if (!edges.length) return null;
  const node = edges[0].node;
  return {
    variantId: node.id,
    productId: node.product.id,
    inventoryItemId: node.inventoryItem.id,
    productTitle: node.product.title,
    productType: node.product.productType,
    currentPrice: parseFloat(node.price),
  };
}

async function gqlProductCreate(
  title: string,
  productType: string,
  sku: string,
  price: number | null,
  stock: number | null,
  locationGid: string
): Promise<{ productId: string; variantId: string; inventoryItemId: string }> {
  const mutation = `#graphql
	mutation($input: ProductSetInput!) {
	  productSet(input: $input) {
		product { id title productType variants(first: 1) { nodes { id sku price inventoryItem { id } } } }
		userErrors { field message }
	  }
	}`;
  const variantInput: any = {
    sku,
    inventoryPolicy: "DENY",
    inventoryItem: { tracked: true },
    optionValues: { optionName: "Title", name: "Default Title" },
  };
  if (price !== null) {
    variantInput.price = price.toFixed(2);
    variantInput.inventoryItem.cost = Number(price).toFixed(2);
  }
  if (stock !== null && locationGid) {
    variantInput.inventoryQuantities = [
      { locationId: locationGid, name: "on_hand", quantity: stock },
    ];
  }
  const input: any = {
    title,
    productType,
    status: "DRAFT",
    productOptions: [{ name: "Title", values: [{ name: "Default Title" }] }],
    variants: [variantInput],
  };
  const res = await shopifyGraphql<{
    data: {
      productSet: {
        product: any;
        userErrors: Array<{ field: string[]; message: string }>;
      };
    };
  }>(mutation, { input });
  const errs = res.data?.productSet?.userErrors ?? [];
  if (errs.length)
    throw new Error(`productSet errors: ${JSON.stringify(errs)}`);
  const product = res.data!.productSet!.product;
  const variant = product.variants.nodes?.[0];
  if (!variant) throw new Error("productSet: missing variant");
  return {
    productId: product.id,
    variantId: variant.id,
    inventoryItemId: variant.inventoryItem.id,
  };
}

async function gqlInventorySetOnHand(
  inventoryItemGid: string,
  locationGid: string,
  quantity: number
): Promise<void> {
  const mutation = `#graphql
	mutation($input: InventorySetOnHandQuantitiesInput!) {
	  inventorySetOnHandQuantities(input: $input) {
		userErrors { field message }
		inventoryAdjustmentGroup { createdAt }
	  }
	}`;
  const input = {
    reason: "correction",
    setQuantities: [
      { inventoryItemId: inventoryItemGid, locationId: locationGid, quantity },
    ],
  };
  const res = await shopifyGraphql<{
    data: {
      inventorySetOnHandQuantities: {
        userErrors: Array<{ field: string[]; message: string }>;
      };
    };
  }>(mutation, { input });
  const errs = res.data?.inventorySetOnHandQuantities?.userErrors ?? [];
  const isNotStocked = errs.some((e) =>
    e.message.includes("not stocked at the location")
  );
  if (errs.length && !isNotStocked) {
    throw new Error(
      `inventorySetOnHandQuantities errors: ${JSON.stringify(errs)}`
    );
  } else if (isNotStocked) {
    const mutation = `#graphql
    mutation ActivateInventoryItem($inventoryItemId: ID!, $locationId: ID!, $available: Int) {
      inventoryActivate(inventoryItemId: $inventoryItemId, locationId: $locationId, available: $available) {
        inventoryLevel {
          id
          quantities(names: ["available"]) {
            name
            quantity
          }
          item {
            id
          }
          location {
            id
          }
        }
      }
    }`;
    const variables = {
      inventoryItemId: inventoryItemGid,
      locationId: locationGid,
      available: quantity,
    };
    const res = await shopifyGraphql<{
      data: {
        inventoryActivate: {
          inventoryLevel: {
            id: string;
            quantities: Array<{ name: string; quantity: number }>;
            item: { id: string };
            location: { id: string };
          };
        };
      };
    }>(mutation, variables);
    const errs = res.data?.inventoryActivate?.userErrors ?? [];
    if (errs.length) {
      throw new Error(`inventoryActivate errors: ${JSON.stringify(errs)}`);
    }
  }
}

interface Product {
  product_id: number;
  product_itemnumber: string | null;
  product_barcode: string | null;
  product_name: string;
  product_price: number;
  product_priceIncludesVat: boolean;
  product_vat: number;
  product_vat_id: number;
  product_customPrice: boolean;
  product_customQuantity: boolean;
  product_fav: boolean;
  product_highlight: boolean;
  product_expressMode: boolean;
  product_stock_enabled: boolean;
  product_ingredients_enabled: boolean;
  product_variations_enabled: boolean;
  product_stock_value: number;
  product_stock_unit: "piece";
  product_stock_reorderLevel: number;
  product_stock_safetyStock: number;
  product_sortIndex: number;
  product_active: boolean;
  product_soldOut: boolean;
  product_sideDishOrder: boolean;
  product_discountable: boolean;
  product_accountingCode: string | null;
  product_colorClass: string | null;
  product_type_id: string | null;
  product_created_at: string;
  product_updated_at: string;
  images: [];
  product_type: null;
}

// ------------------------
// Main
// ------------------------
async function main(): Promise<void> {
  console.log("Downloading JSON products...");
  const allProducts: Product[] = [];
  let productsOfPage: Product[] = [];
  let page = 1;
  do {
    const response = await fetch(
      `https://api.ready2order.com/v1/products?limit=250&page=${page}`,
      {
        headers: {
          Authorization: `Bearer ${R2O_API_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );
    productsOfPage = (await response.json()) as Product[];
    allProducts.push(...productsOfPage);
    page++;
  } while (productsOfPage.length === 250);

  console.log(`Filtering products...`);
  // TODO: remove itemnumber filter as soon as all skus of derksen products are in ready2order
  const products = allProducts.filter(
    (p) => p.product_active && p.product_itemnumber
  );
  console.log(`Found ${products.length} active products with sku`);

  let processed = 0,
    created = 0,
    stockUpdates = 0;
  const totalProducts = products.length;

  for (const p of products) {
    const { product_itemnumber: sku, product_name: title } = p;
    const type = "Uncategorized";
    const price = p.product_price;
    const stock = p.product_stock_value;

    if (!sku) {
      continue;
    }

    try {
      const existing = await gqlFindVariantBySku(sku);
      if (existing) {
        const { inventoryItemId } = existing;
        if (stock) {
          await gqlInventorySetOnHand(inventoryItemId, LOCATION_GID, stock);
          stockUpdates++;
        }
      } else {
        // await gqlProductCreate(title, type, sku, price, stock, LOCATION_GID);
        // created++;
      }
    } catch (e: any) {
      console.error(`[SKU ${sku}] Error: ${e?.message || String(e)}`);
      await delay(300); // backoff
    }
    processed++;
    await delay(120); // pacing
    const percentage = Math.round((processed / totalProducts) * 1000) / 10;
    process.stdout.write(
      `\rProcessing: ${processed}/${totalProducts} (${percentage}%) - Created: ${created}, Stock Updates: ${stockUpdates}`
    );
  }
  process.stdout.write("\n");
  console.log(
    `Done. Processed=${processed}, Created=${created}, StockUpdated=${stockUpdates}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
