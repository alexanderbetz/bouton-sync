<?php
// sync_derksen_to_shopify.php
// Requirements: PHP 8+, mbstring, curl
// Usage: Set env vars below, then: php sync_derksen_to_shopify.php

declare(strict_types=1);
set_time_limit(0);

// ------------------------
// Configuration (env vars)
// ------------------------
$SHOP_DOMAIN        = getenv('SHOPIFY_SHOP_DOMAIN') ?: throw new RuntimeException('SHOPIFY_SHOP_DOMAIN is not set');
$ACCESS_TOKEN       = getenv('SHOPIFY_ACCESS_TOKEN') ?: throw new RuntimeException('SHOPIFY_ACCESS_TOKEN is not set');
$API_VERSION        = '2025-07';
$LOCATION_GID       = getenv('SHOPIFY_LOCATION_GID') ?: throw new RuntimeException('SHOPIFY_LOCATION_GID is not set');
$CSV_URL            = getenv('CSV_URL') ?: throw new RuntimeException('CSV_URL is not set');

// ------------------------
// HTTP / Shopify helpers
// ------------------------
function http_get(string $url, int $timeout = 30): string {
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_TIMEOUT => $timeout,
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_SSL_VERIFYHOST => 2,
        CURLOPT_USERAGENT => 'DerksenCSVShopifySync/1.0',
    ]);
    $body = curl_exec($ch);
    if ($body === false) {
        $err = curl_error($ch);
        curl_close($ch);
        throw new RuntimeException("GET failed: $err");
    }
    $status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    if ($status < 200 || $status >= 300) {
        throw new RuntimeException("GET failed with HTTP $status");
    }
    return $body;
}

function get_suggested_retail_price(float $price): float {
    if (($price * 1.3) % 10 >= 5) {
        return floor($price * 1.3 / 10) * 10 + 9.99;
    } else {
        return floor($price * 1.3 / 10) * 10 + 4.99;
    }
}

function shopify_graphql(string $shopDomain, string $accessToken, string $apiVersion, string $query, array $variables = [], int $timeout = 30): array {
    $url = "https://{$shopDomain}/admin/api/{$apiVersion}/graphql.json";
    $ch = curl_init($url);
    $payload = json_encode(['query' => $query, 'variables' => $variables], JSON_UNESCAPED_SLASHES);

    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => $payload,
        CURLOPT_HTTPHEADER => [
            'Content-Type: application/json',
            "X-Shopify-Access-Token: {$accessToken}",
        ],
        CURLOPT_TIMEOUT => $timeout,
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_SSL_VERIFYHOST => 2,
    ]);

    $body = curl_exec($ch);
    if ($body === false) {
        $err = curl_error($ch);
        curl_close($ch);
        throw new RuntimeException("GraphQL failed: $err");
    }
    $status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);

    $json = json_decode($body, true);
    if (!is_array($json)) {
        throw new RuntimeException("GraphQL invalid JSON (HTTP $status): $body");
    }
    if ($status !== 200) {
        throw new RuntimeException("GraphQL HTTP $status: " . json_encode($json));
    }
    if (!empty($json['errors'])) {
        throw new RuntimeException("GraphQL errors: " . json_encode($json['errors']));
    }
    return $json;
}

function get_product_image_url(string $sku): string|null {
    $url = "https://www.derksen.at/p/{$sku}";

    try {
    $html = http_get($url);
    $dom = Dom\HTMLDocument::createFromString($html);
    $image = $dom->querySelector('a[data-fancybox=gallery] img');
    if ($image) {
        return 'https://www.derksen.at/' . $image->getAttribute('src');
    }
    return null;
    } catch (Exception $e) {
        return null;
    }
}

function to_gid(string $type, string|int $id): string {
    return "gid://shopify/{$type}/{$id}";
}

// ------------------------
// CSV parsing
// ------------------------
function detect_separator_from_first_line(string $firstLine): string {
    // Expected format: "sep=;" or "sep=," or "sep=\t" etc.
    $line = $firstLine;
    if (stripos($line, 'sep=') === 0) {
        $sep = substr($line, 4); // everything after "sep="
        if ($sep === '\\t') return "\t";
        if ($sep !== '') return $sep[0];
    }
    // Fallback guess: semicolon
    return ';';
}

function normalize_headers(array $headers): array {
    $norm = [];
    foreach ($headers as $h) {
        $h = trim($h);
        // Normalize: remove quotes, spaces, lowercase
        $h = str_replace(['"', "'"], '', $h);
        $k = strtolower(preg_replace('/\s+/', '', $h));
        $norm[] = [$k, $h]; // store normalized and original
    }
    return $norm;
}

function parse_price_eu(string $raw): float {
    // Convert "1.234,56" or "57,10" to "1234.56"
    $s = trim($raw);
    // Remove thousand separators '.' only if comma is present
    if (str_contains($s, ',')) {
        $s = str_replace('.', '', $s);
        $s = str_replace(',', '.', $s);
    }
    // Strip any non-number except dot and minus
    $s = preg_replace('/[^0-9\.\-]/', '', $s);
    if ($s === '' || $s === '.' || $s === '-') return 0.0;
    return (float)$s;
}

function parse_csv_products(string $csvContent): array {
    // Normalize encoding to UTF-8
    $enc = mb_detect_encoding($csvContent, ['UTF-8', 'ISO-8859-1', 'Windows-1252'], true) ?: 'UTF-8';
    if ($enc !== 'UTF-8') {
        $csvContent = mb_convert_encoding($csvContent, 'UTF-8', $enc);
    }

    $lines = preg_split("/\r\n|\n|\r/", $csvContent);
    if (!$lines || count($lines) < 2) {
        throw new RuntimeException('CSV: not enough lines');
    }

    $separator = detect_separator_from_first_line($lines[0]);
    // Remove the 'sep=' line
    array_shift($lines);

    // Build a temp stream for fgetcsv reliability
    $stream = fopen('php://temp', 'r+');
    foreach ($lines as $line) {
        fwrite($stream, $line . "\n");
    }
    rewind($stream);

    // Read header row
    $headers = fgetcsv($stream, 0, $separator);
    if ($headers === false) {
        throw new RuntimeException('CSV: failed to read header row');
    }
    $headerMap = normalize_headers($headers);

    // Identify key columns by normalized name
    $colIdx = [
        'artnr'       => null,
        'artikel'     => null,
        'kategorie'   => null,
        'lagerstand'  => null,
        'preisnetto'  => null,
    ];
    foreach ($headerMap as $idx => [$norm, $orig]) {
        if (array_key_exists($norm, $colIdx)) {
            $colIdx[$norm] = $idx;
        }
    }
    // Basic validation: require at least SKU+Title
    if ($colIdx['artnr'] === null || $colIdx['artikel'] === null) {
        throw new RuntimeException('CSV: required columns missing (need at least ArtNr and Artikel)');
    }

    $rows = [];
    while (($row = fgetcsv($stream, 0, $separator)) !== false) {
        if (count(array_filter($row, fn($v) => trim((string)$v) !== '')) === 0) {
            continue; // skip blank lines
        }
        $sku = trim((string)($row[$colIdx['artnr']] ?? ''));
        $title = trim((string)($row[$colIdx['artikel']] ?? ''));
        if ($sku === '' || $title === '') continue;

        $category = $colIdx['kategorie'] !== null ? trim((string)$row[$colIdx['kategorie']]) : '';
        $stockRaw = $colIdx['lagerstand'] !== null ? trim((string)$row[$colIdx['lagerstand']]) : '';
        $priceRaw = $colIdx['preisnetto'] !== null ? trim((string)$row[$colIdx['preisnetto']]) : '';

        // Parse numbers
        $price = $priceRaw !== '' ? parse_price_eu($priceRaw) : null;
        $stock = null;
        if ($stockRaw !== '') {
            // if numeric, use it; if "lagernd" or other text, leave null to skip inventory update
            $stockRawClean = str_replace(['+', '−', "\xE2\x88\x92"], '-', $stockRaw);
            if (preg_match('/^-?\d+$/', $stockRawClean)) {
                $stock = (int)$stockRawClean;
            } else if ($stockRaw === 'lagernd') {
                $stock = 25;
            } else if($stockRaw === 'limitiert') {
                $stock = 1;
            }
        }

        if($price !== 'Preis auf Anfrage') {
            $rows[] = [
                'sku' => $sku,
                'title' => $title,
                'category' => $category,
                'price' => $price,     // float|null
                'stock' => $stock,     // int|null
            ];
        }
    }
    fclose($stream);
    return $rows;
}

// ------------------------
// Shopify operations
// ------------------------
function gql_find_variant_by_sku(string $shop, string $token, string $ver, string $sku): array|null {
    $query = <<<'GQL'
query($q: String!) {
  productVariants(first: 1, query: $q) {
    edges {
      node {
        id
        sku
        price
        product { id title productType }
        inventoryItem { id }
      }
    }
  }
}
GQL;
    $vars = ['q' => "sku:{$sku}"];
    $res = shopify_graphql($shop, $token, $ver, $query, $vars);
    $edges = $res['data']['productVariants']['edges'] ?? [];
    if (count($edges) === 0) return null;
    $node = $edges[0]['node'];
    return [
        'variantId' => $node['id'],
        'productId' => $node['product']['id'],
        'inventoryItemId' => $node['inventoryItem']['id'],
        'productTitle' => $node['product']['title'],
        'productType' => $node['product']['productType'],
        'currentPrice' => (float)$node['price'],
    ];
}

function gql_product_create(string $shop, string $token, string $ver, string $title, string $productType, string $sku, ?float $price, ?int $stock = null, string $locationGid = ''): array {
    // Use productSet mutation to create product with variant in one call
    $mutation = <<<'GQL'
mutation($input: ProductSetInput!) {
  productSet(input: $input) {
    product {
      id
      title
      productType
      variants(first: 1) {
        nodes {
          id
          sku
          price
          inventoryItem { id }
        }
      }
    }
    userErrors { field message }
  }
}
GQL;
    $imageUrl = get_product_image_url($sku);
    $variantInput = [
        'sku' => $sku,
        'inventoryPolicy' => 'DENY',
        'inventoryItem' => ['tracked' => true],
        'optionValues' => ['optionName' => 'Title', 'name'=> 'Default Title'],
        'file' => !is_null($imageUrl) ? ['contentType' => 'IMAGE', 'originalSource' => $imageUrl] : null,
    ];
    
    if ($price !== null) {
        $variantInput['price'] = number_format(get_suggested_retail_price($price), 2, '.', '');
        $variantInput['inventoryItem']['cost'] = number_format($price, 2, '.', '');
    }
    
    // Add inventory quantity if stock and location are provided
    if ($stock !== null && $locationGid !== '') {
        $variantInput['inventoryQuantities'] = [[
            'locationId' => $locationGid,
            'name' => 'on_hand',
            'quantity' => $stock,
        ]];
    }
    
    $input = [
        'title' => $title,
        'productType' => $productType,
        'status' => 'DRAFT',
        'productOptions' => [ 'name' => 'Title', 'values'  => [['name' => 'Default Title']]],
        'variants' => [$variantInput],
        'files' => !is_null($imageUrl) ? ['contentType' => 'IMAGE', 'originalSource' => $imageUrl] : null,
    ];
    
    $res = shopify_graphql($shop, $token, $ver, $mutation, ['input' => $input]);
    $errs = $res['data']['productSet']['userErrors'] ?? [];
    if (!empty($errs)) {
        throw new RuntimeException('productSet errors: ' . json_encode($errs));
    }
    
    $product = $res['data']['productSet']['product'];
    $variant = $product['variants']['nodes'][0] ?? null;
    if (!$variant) {
        throw new RuntimeException('productSet: missing variant');
    }

    return [
        'productId' => $product['id'],
        'variantId' => $variant['id'],
        'inventoryItemId' => $variant['inventoryItem']['id'],
    ];
}

function gql_product_update_meta(string $shop, string $token, string $ver, string $productId, string $newTitle, string $newType): void {
    // Use productSet mutation to update product metadata
    $mutation = <<<'GQL'
mutation($input: ProductSetInput!) {
  productSet(input: $input) {
    product { id title productType }
    userErrors { field message }
  }
}
GQL;
    
    $input = [
        'id' => $productId,
        'title' => $newTitle,
        'productType' => $newType
    ];
    
    $res = shopify_graphql($shop, $token, $ver, $mutation, ['input' => $input]);
    $errs = $res['data']['productSet']['userErrors'] ?? [];
    if (!empty($errs)) {
        throw new RuntimeException('productSet update errors: ' . json_encode($errs));
    }
}

function gql_variant_update_price(string $shop, string $token, string $ver, string $productId, string $variantId, float $price): void {
    // Use productSet mutation to update variant price
    $mutation = <<<'GQL'
mutation($productId: ID!, $input: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $input) {
    product { id }
    userErrors { field message }
  }
}
GQL;
    
    $input = [
            'id' => $variantId,
            'price' => number_format(get_suggested_retail_price($price), 2, '.', ''),
            'inventoryItem' => ['cost' => number_format($price, 2, '.', '')],
    ];
    
    $res = shopify_graphql($shop, $token, $ver, $mutation, ['input' => $input, 'productId' => $productId]);
    $errs = $res['data']['productSet']['userErrors'] ?? [];
    if (!empty($errs)) {
        throw new RuntimeException('productSet price update errors: ' . json_encode($errs));
    }
}

function gql_inventory_set_on_hand(string $shop, string $token, string $ver, string $inventoryItemGid, string $locationGid, int $quantity): void {
    // Sets absolute on-hand quantities
    $mutation = <<<'GQL'
mutation($input: InventorySetOnHandQuantitiesInput!) {
  inventorySetOnHandQuantities(input: $input) {
    userErrors { field message }
    inventoryAdjustmentGroup { createdAt }
  }
}
GQL;
    $input = [
        'reason' => 'correction',
        'setQuantities' => [[
            'inventoryItemId' => $inventoryItemGid,
            'locationId' => $locationGid,
            'quantity' => $quantity,
        ]],
    ];
    $res = shopify_graphql($shop, $token, $ver, $mutation, ['input' => $input]);
    $errs = $res['data']['inventorySetOnHandQuantities']['userErrors'] ?? [];
    if (!empty($errs)) {
        throw new RuntimeException('inventorySetOnHandQuantities errors: ' . json_encode($errs));
    }
}

// ------------------------
// Main
// ------------------------
function main(): void {
    global $SHOP_DOMAIN, $ACCESS_TOKEN, $API_VERSION, $LOCATION_GID, $CSV_URL;

    if (!$SHOP_DOMAIN || !$ACCESS_TOKEN || !$API_VERSION || !$LOCATION_GID) {
        fwrite(STDERR, "Please set SHOPIFY_SHOP_DOMAIN, SHOPIFY_ACCESS_TOKEN, SHOPIFY_API_VERSION, SHOPIFY_LOCATION_GID\n");
        exit(1);
    }

    echo "Downloading CSV...\n";
    $csv = http_get($CSV_URL);

    echo "Parsing CSV...\n";
    $products = parse_csv_products($csv);
    echo "Found " . count($products) . " rows.\n";

    $processed = 0; $created = 0; $updated = 0; $priceUpdates = 0; $stockUpdates = 0;
    $totalProducts = count($products);
    
    foreach ($products as $p) {
        $sku = $p['sku'];
        $title = $p['title'];
        $type  = $p['category'] ?: 'Uncategorized';
        $price = $p['price'];  // float|null
        $stock = $p['stock'];  // int|null

        try {
            $existing = gql_find_variant_by_sku($SHOP_DOMAIN, $ACCESS_TOKEN, $API_VERSION, $sku);

            if ($existing) {
                $variantId = $existing['variantId'];
                $productId = $existing['productId'];
                $inventoryItemId = $existing['inventoryItemId'];

                // Update price if provided and different
                // if ($price !== null && abs($existing['currentPrice'] - get_suggested_retail_price($price)) > 0.0001) {
                //     gql_variant_update_price($SHOP_DOMAIN, $ACCESS_TOKEN, $API_VERSION, $productId, $variantId, $price);
                //     $priceUpdates++;
                // }

                // Update stock if numeric
                if ($stock !== null) {
                    gql_inventory_set_on_hand($SHOP_DOMAIN, $ACCESS_TOKEN, $API_VERSION, $inventoryItemId, $LOCATION_GID, $stock);
                    $stockUpdates++;
                }
            } else {
                // Create new product + variant
                $createRes = gql_product_create($SHOP_DOMAIN, $ACCESS_TOKEN, $API_VERSION, $title, $type, $sku, $price, $stock, $LOCATION_GID);
                $created++;
            }
        } catch (Throwable $e) {
            // Log and continue with next product
            fwrite(STDERR, "[SKU {$sku}] Error: " . $e->getMessage() . "\n");
            // Backoff a bit in case of throttling
            usleep(300000);
        }

        $processed++;

        // Gentle pacing to respect rate limits
        usleep(120000); // ~0.12s
        
        // Update progress after every iteration with count and total
        $percentage = round(($processed / $totalProducts) * 100, 1);
        echo "\rProcessing: {$processed}/{$totalProducts} ({$percentage}%) - Created: {$created}, Stock Updates: {$stockUpdates}";
    }
    
    // Final newline after progress updates
    echo "\n";

    echo "Done. Processed={$processed}, Created={$created}, ProductMetaUpdated={$updated}, PriceUpdated={$priceUpdates}, StockUpdated={$stockUpdates}\n";
}

error_reporting(E_ERROR);
main();