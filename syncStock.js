require('dotenv').config();
const Shopify = require('shopify-api-node');
const axios = require('axios');
const fs = require('fs');
const csv = require('csv-parser');

// Setup Shopify client
const shopify = new Shopify({
  shopName: '697f5c-9e',
  accessToken: process.env.SHOPIFY_ACCESS_TOKEN,
});

// Download the CSV
async function downloadCSV() {
  try {
    const response = await axios.get(
      'https://www.btswholesaler.com/generatefeedbts?user_id=1318121&pass=MiNNi800HyG201&format=csv&language_code=en-gb',
      { responseType: 'stream' }
    );

    const filePath = './stockfile.csv'; // <- Save locally in project root
    const writer = fs.createWriteStream(filePath);
    response.data.pipe(writer);

    writer.on('finish', () => {
      console.log('✅ CSV file downloaded successfully');
      processCSV(filePath);
    });
  } catch (error) {
    console.error('❌ Error downloading CSV:', error.message);
  }
}

// Process the CSV file
function processCSV(filePath) {
  const products = [];

  fs.createReadStream(filePath)
    .pipe(csv({ separator: ';' }))
    .on('data', (row) => {
      if (products.length === 0) {
        console.log('🔍 CSV Headers:', Object.keys(row));
      }
      const ean = row.ean;
      const stock = parseInt(row.stock, 10);

      if (!ean || isNaN(stock)) {
        console.warn(`⚠️ Skipping row: invalid EAN or stock - EAN: ${ean}, Stock: ${row.stock}`);
        return;
      }

      products.push({ ean, stock });
    })
    .on('end', async () => {
      console.log(`✅ CSV file processed with ${products.length} rows`);
      for (const product of products) {
        await syncStockByBarcode(product.ean, product.stock);
        await delay(500 + Math.floor(Math.random() * 200)); // Add jitter
      }
    });
}

// Helper: wait
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Sync stock by looking up product by barcode
async function syncStockByBarcode(ean, stock, attempt = 1) {
  try {
    const products = await shopify.product.list({ barcode: ean });

    if (!products.length) {
      console.warn(`⚠️ No product found with barcode: ${ean}`);
      return;
    }

    const product = products[0];
    const inventoryItemId = product.variants[0]?.inventory_item_id;

    if (!inventoryItemId) {
      console.warn(`⚠️ No valid inventory_item_id found for product with barcode: ${ean}`);
      return;
    }

    await shopify.inventoryLevel.set({
      location_id: process.env.SHOPIFY_LOCATION_ID,
      inventory_item_id: String(inventoryItemId),
      available: stock,
    });

    console.log(`✅ Stock updated for EAN ${ean} -> ${stock}`);
  } catch (error) {
    if (error.code === 'ECONNRESET' && attempt <= 3) {
      console.warn(`🔁 ECONNRESET on EAN ${ean}, retrying in 3s (Attempt ${attempt})`);
      await delay(3000);
      return syncStockByBarcode(ean, stock, attempt + 1);
    }

    if (error.response?.data) {
      console.error(`❌ Error updating stock for EAN ${ean}:`, error.response.data);
    } else if (error.response?.body) {
      console.error(`❌ Error updating stock for EAN ${ean}:`, error.response.body);
    } else {
      console.error(`❌ Error updating stock for EAN ${ean}:`, error.message);
    }
  }
}

// Start the script
downloadCSV();
