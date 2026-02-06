import express from 'express';
import multer from 'multer';
import cors from 'cors';
import MDBReader from 'mdb-reader';
import crypto from 'crypto';

const app = express();
const PORT = process.env.PORT || 3000;

// Configure multer for file uploads (200 MB limit)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 200 * 1024 * 1024 }, // 200 MB
});

// Enable CORS for all origins
app.use(cors());
app.use(express.json());

// ============================================
// FILE SESSION CACHE
// Keep uploaded files in memory to avoid re-uploads
// ============================================
const fileCache = new Map(); // sessionId -> { buffer, filename, timestamp }
const SESSION_TIMEOUT = 30 * 60 * 1000; // 30 minutes

// Clean up expired sessions periodically
setInterval(() => {
  const now = Date.now();
  for (const [sessionId, data] of fileCache.entries()) {
    if (now - data.timestamp > SESSION_TIMEOUT) {
      console.log(`[cache] Expiring session: ${sessionId}`);
      fileCache.delete(sessionId);
    }
  }
}, 5 * 60 * 1000); // Check every 5 minutes

function generateSessionId() {
  return crypto.randomBytes(16).toString('hex');
}

// ============================================
// COLUMN MAPPINGS
// ============================================
const columnMappings = {
  nome: 'name',
  'nome do poço': 'name',
  poço: 'name',
  poco: 'name',
  bloco: 'block',
  campo: 'field',
  'campo petrolífero': 'field',
  província: 'province',
  provincia: 'province',
  latitude: 'latitude',
  lat: 'latitude',
  longitude: 'longitude',
  long: 'longitude',
  lng: 'longitude',
  profundidade: 'depth',
  depth: 'depth',
  tipo: 'type',
  type: 'type',
  'tipo de hidrocarboneto': 'type',
  reservas: 'estimated_reserves',
  'reservas estimadas': 'estimated_reserves',
  estimated_reserves: 'estimated_reserves',
  'produção diária': 'daily_production',
  'producao diaria': 'daily_production',
  produção: 'daily_production',
  daily_production: 'daily_production',
  'data de início': 'production_start_date',
  'data inicio': 'production_start_date',
  'início produção': 'production_start_date',
  production_start_date: 'production_start_date',
  status: 'status',
  estado: 'status',
  'taxa de declínio': 'decline_rate',
  declínio: 'decline_rate',
  decline_rate: 'decline_rate',
};

function normalizeColumnName(name) {
  const normalized = String(name).toLowerCase().trim();
  return columnMappings[normalized] || normalized;
}

function normalizeType(val) {
  if (!val) return '';
  const normalized = String(val).toLowerCase().trim();
  if (['petróleo', 'petroleo', 'oil'].includes(normalized)) return 'oil';
  if (['gás', 'gas'].includes(normalized)) return 'gas';
  if (['misto', 'mixed'].includes(normalized)) return 'mixed';
  return String(val);
}

function normalizeStatus(val) {
  if (!val) return '';
  const normalized = String(val).toLowerCase().trim();
  if (['ativo', 'active'].includes(normalized)) return 'active';
  if (['inativo', 'inactive'].includes(normalized)) return 'inactive';
  if (['exploratório', 'exploratorio', 'exploratory'].includes(normalized)) return 'exploratory';
  if (['declínio', 'declining', 'em declínio'].includes(normalized)) return 'declining';
  return String(val);
}

function parseNumber(val) {
  if (val === null || val === undefined) return null;
  if (typeof val === 'number') return val;
  const str = String(val).replace(',', '.').replace(/[^\d.-]/g, '');
  const num = parseFloat(str);
  return isNaN(num) ? null : num;
}

function parseDate(val) {
  if (!val) return null;
  try {
    const date = new Date(val);
    if (isNaN(date.getTime())) return null;
    return date.toISOString().split('T')[0];
  } catch {
    return null;
  }
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    activeSessions: fileCache.size 
  });
});

// ============================================
// LIST TABLES - Upload file and return sessionId
// ============================================
app.post('/list-tables', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ success: false, error: 'Arquivo não fornecido' });
    }

    console.log(`[list-tables] Processing file: ${req.file.originalname}, size: ${req.file.size} bytes`);

    // Generate session ID and cache the file
    const sessionId = generateSessionId();
    fileCache.set(sessionId, {
      buffer: req.file.buffer,
      filename: req.file.originalname,
      timestamp: Date.now(),
    });

    console.log(`[list-tables] Created session: ${sessionId}`);

    const reader = new MDBReader(req.file.buffer);
    const tableNames = reader.getTableNames();

    const tables = tableNames.map((name) => {
      try {
        const table = reader.getTable(name);
        const columns = table.getColumnNames();
        const data = table.getData();
        return { name, rowCount: data.length, columns };
      } catch (err) {
        console.error(`Error reading table ${name}:`, err);
        return { name, rowCount: 0, columns: [] };
      }
    });

    console.log(`[list-tables] Found ${tables.length} tables`);
    
    // Return sessionId so client can use it for parse-table
    res.json({ success: true, tables, sessionId });
  } catch (err) {
    console.error('[list-tables] Error:', err);
    res.status(500).json({ success: false, error: err.message || 'Erro ao processar arquivo' });
  }
});

// ============================================
// PARSE TABLE - Use sessionId OR upload file
// ============================================
app.post('/parse-table', upload.single('file'), async (req, res) => {
  try {
    const tableName = req.body.tableName;
    if (!tableName) {
      return res.status(400).json({ success: false, error: 'Nome da tabela não fornecido' });
    }

    let buffer;
    const sessionId = req.body.sessionId;

    // Try to get file from session cache first
    if (sessionId && fileCache.has(sessionId)) {
      console.log(`[parse-table] Using cached file from session: ${sessionId}`);
      const cached = fileCache.get(sessionId);
      cached.timestamp = Date.now(); // Refresh timestamp
      buffer = cached.buffer;
    } else if (req.file) {
      // Fallback to uploaded file
      console.log(`[parse-table] Using uploaded file: ${req.file.originalname}, size: ${req.file.size} bytes`);
      buffer = req.file.buffer;
    } else {
      return res.status(400).json({ 
        success: false, 
        error: 'Arquivo não fornecido. Sessão pode ter expirado - faça upload novamente.' 
      });
    }

    console.log(`[parse-table] Processing table: ${tableName}`);

    const reader = new MDBReader(buffer);
    const table = reader.getTable(tableName);
    const columns = table.getColumnNames();
    const data = table.getData();

    const totalRows = data.length;
    const success = [];
    const failed = [];

    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const original = {};
      const mapped = {};

      columns.forEach((col) => {
        const normalizedCol = normalizeColumnName(col);
        original[col] = row[col];
        mapped[normalizedCol] = row[col];
      });

      const errors = [];

      if (!mapped.name) errors.push('Nome do poço é obrigatório');
      if (!mapped.block) errors.push('Bloco é obrigatório');
      if (!mapped.field) errors.push('Campo é obrigatório');
      if (!mapped.province) errors.push('Província é obrigatória');

      const latitude = parseNumber(mapped.latitude);
      const longitude = parseNumber(mapped.longitude);
      const depth = parseNumber(mapped.depth);

      if (latitude === null || latitude < -90 || latitude > 90) errors.push('Latitude inválida');
      if (longitude === null || longitude < -180 || longitude > 180) errors.push('Longitude inválida');
      if (depth === null || depth < 0) errors.push('Profundidade inválida');

      const type = normalizeType(mapped.type);
      if (!['oil', 'gas', 'mixed'].includes(type)) errors.push('Tipo deve ser: petróleo, gás ou misto');

      const status = normalizeStatus(mapped.status);
      if (!['active', 'inactive', 'exploratory', 'declining'].includes(status)) {
        errors.push('Status deve ser: ativo, inativo, exploratório ou declínio');
      }

      if (errors.length === 0) {
        success.push({
          row: i + 1,
          data: {
            name: String(mapped.name),
            block: String(mapped.block),
            field: String(mapped.field),
            province: String(mapped.province),
            latitude,
            longitude,
            depth,
            type,
            estimated_reserves: parseNumber(mapped.estimated_reserves) || 0,
            daily_production: parseNumber(mapped.daily_production) || 0,
            production_start_date: parseDate(mapped.production_start_date),
            status,
            decline_rate: parseNumber(mapped.decline_rate) || 0,
          },
          errors: [],
          original,
        });
      } else {
        failed.push({ row: i + 1, data: null, errors, original });
      }
    }

    console.log(`[parse-table] Processed ${totalRows} rows: ${success.length} valid, ${failed.length} failed`);
    res.json({ success: true, parseResult: { success, failed, totalRows } });
  } catch (err) {
    console.error('[parse-table] Error:', err);
    res.status(500).json({ success: false, error: err.message || 'Erro ao processar tabela' });
  }
});

// ============================================
// CLEAR SESSION - Clean up cached file
// ============================================
app.post('/clear-session', express.json(), (req, res) => {
  const { sessionId } = req.body;
  
  if (sessionId && fileCache.has(sessionId)) {
    fileCache.delete(sessionId);
    console.log(`[clear-session] Deleted session: ${sessionId}`);
    res.json({ success: true });
  } else {
    res.json({ success: false, error: 'Session not found' });
  }
});

app.listen(PORT, () => {
  console.log(`Access Parser Server running on port ${PORT}`);
  console.log(`Health check: http://localhost:${PORT}/health`);
});
