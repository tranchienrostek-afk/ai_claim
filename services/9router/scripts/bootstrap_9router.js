const fs = require('fs');
const path = require('path');
const os = require('os');

// Simple UUID-like generator to avoid external dependencies during bootstrap
function simpleUuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

// Paths
const PROJECT_ROOT = 'd:\\desktop_folder\\01_claudecodeleak';
const PROXY_ENV_FILE = path.join(PROJECT_ROOT, 'azure_anthropic_sdk', '.env');
const APPDATA = process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming');
const DATA_DIR = path.join(APPDATA, '9router');
const DB_FILE = path.join(DATA_DIR, 'db.json');

// Function to read .env values
function getEnvValue(key) {
  if (!fs.existsSync(PROXY_ENV_FILE)) return null;
  const content = fs.readFileSync(PROXY_ENV_FILE, 'utf8');
  const lines = content.split('\n');
  for (let line of lines) {
    if (line.trim().startsWith(`${key}=`)) {
      // Find the last occurrence of '=' to get the value
      const lastIndex = line.lastIndexOf('=');
      const val = line.substring(lastIndex + 1);
      return val.trim().replace(/^['"]|['"]$/g, '');
    }
  }
  return null;
}

// Constants from environment or azure_anthropic_sdk/.env
const GLM_API_KEY = process.env.GLM_API_KEY || getEnvValue("GLM_API_KEY") || "";
const AZURE_API_KEY = process.env.AZURE_OPENAI_API_KEY || getEnvValue("AZURE_OPENAI_API_KEY") || "";
const PROXY_URL = "http://127.0.0.1:8009/v1";
const DEPLOYMENT_NAME = "gpt-5-mini";

const defaultData = {
  providerConnections: [],
  providerNodes: [],
  proxyPools: [],
  modelAliases: {},
  mitmAlias: {},
  combos: [],
  apiKeys: [],
  settings: {
    requireLogin: false,
    observabilityEnabled: true,
  },
  pricing: {}
};

async function bootstrap() {
  console.log(`Checking database at: ${DB_FILE}`);
  
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }

  let db = defaultData;
  if (fs.existsSync(DB_FILE)) {
    console.log("Database exists, performing merge...");
    try {
      db = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
    } catch (e) {
      console.error("Error parsing existing DB, using defaults.");
    }
  }

  // 1. Add GLM Connection (Provider: glm)
  const hasGlm = db.providerConnections.some(c => c.provider === 'glm');
  if (!hasGlm) {
    console.log("Adding GLM connection...");
    db.providerConnections.push({
      id: simpleUuid(),
      provider: 'glm',
      authType: 'apikey',
      name: 'GLM Primary',
      apiKey: GLM_API_KEY,
      isActive: true,
      priority: 1,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    });
  }

  // 2. Add Azure Proxy Node (Type: anthropic-compatible)
  const hasAzureProxy = db.providerNodes.some(n => n.name === 'Azure-Proxy' || n.baseUrl === PROXY_URL);
  if (!hasAzureProxy) {
    console.log("Adding Azure Proxy node...");
    db.providerNodes.push({
      id: simpleUuid(),
      type: 'anthropic-compatible', // 9Router's internal categorization for Anthropic-like APIs
      name: 'Azure-Proxy',
      prefix: 'emerald',
      baseUrl: PROXY_URL,
      apiType: 'anthropic',
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    });
  }

  // 2.1 Add Azure Proxy Connection
  const hasAzureConn = db.providerConnections.some(c => c.provider === 'emerald');
  if (!hasAzureConn) {
    console.log("Adding Azure Proxy connection...");
    db.providerConnections.push({
      id: simpleUuid(),
      provider: 'emerald',
      authType: 'apikey', 
      name: 'Azure Local Proxy',
      apiKey: AZURE_API_KEY,
      isActive: true,
      priority: 2,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    });
  } else {
    // Forcefully update existing connection with real key
    const conn = db.providerConnections.find(c => c.provider === 'emerald');
    if (conn.apiKey !== AZURE_API_KEY) {
      console.log("Updating Azure Proxy connection with real API Key...");
      conn.apiKey = AZURE_API_KEY;
    }
  }

  // 3. Add model aliases for easy usage in Claude Code
  db.modelAliases = db.modelAliases || {};
  db.modelAliases['sonnet'] = `emerald/${DEPLOYMENT_NAME}`; 
  db.modelAliases['glm'] = 'glm/glm-5-turbo';

  // 4. Ensure an API key exists for 9Router itself
  if (db.apiKeys.length === 0) {
    console.log("Generating 9Router API key...");
    const key = `9r-${simpleUuid().replace(/-/g, '')}`;
    db.apiKeys.push({
      id: simpleUuid(),
      key: key,
      name: 'Claude Code Key',
      isActive: true,
      createdAt: new Date().toISOString()
    });
    console.log(`\n!!! IMPORTANT: YOUR 9ROUTER API KEY IS: ${key} !!!\n`);
  } else {
    console.log(`9Router already has ${db.apiKeys.length} API keys.`);
  }

  // Save back
  fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
  console.log("Bootstrap complete.");
}

bootstrap().catch(console.error);
