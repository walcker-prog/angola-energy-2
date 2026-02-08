import express from 'express';
import multer from 'multer';
import cors from 'cors';
import MDBReader from 'mdb-reader';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

// ============================================
// DISK STORAGE CONFIGURATION
// Store uploads on disk to avoid memory exhaustion on large files
// ============================================
const UPLOADS_DIR = path.join(__dirname, 'uploads');

// Ensure uploads directory exists
if (!fs.existsSync(UPLOADS_DIR)) {
  fs.mkdirSync(UPLOADS_DIR, { recursive: true });
}

const diskStorage = multer.diskStorage({
  destination: (_req, _file, cb) => {
    cb(null, UPLOADS_DIR);
  },
  filename: (_req, file, cb) => {
    const uniqueSuffix = crypto.randomBytes(8).toString('hex');
    cb(null, `${uniqueSuffix}-${file.originalname}`);
  },
});

// Configure multer for file uploads (200 MB limit) - using disk storage
const upload = multer({
  storage: diskStorage,
  limits: { fileSize: 200 * 1024 * 1024 }, // 200 MB
});

// ============================================
// CHUNKED UPLOAD (avoid proxy timeouts)
// ============================================
const CHUNK_SIZE = Number(process.env.CHUNK_SIZE_BYTES) || 8 * 1024 * 1024; // 8 MB
const CHUNKS_DIR = path.join(UPLOADS_DIR, '_chunks');

if (!fs.existsSync(CHUNKS_DIR)) {
  fs.mkdirSync(CHUNKS_DIR, { recursive: true });
}

// Keep each request small; store chunk in memory then persist to disk.
const chunkUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: CHUNK_SIZE + 1024 * 1024 }, // chunk + overhead
});

const chunkSessions = new Map(); // uploadId -> { dirPath, filename, totalChunks, size, createdAt }

// Enable CORS for all origins
app.use(cors());
app.use(express.json());

// ============================================
// FILE SESSION CACHE
// Keep references to files on disk to avoid re-uploads
// ============================================
const fileCache = new Map(); // sessionId -> { filePath, filename, timestamp }
const SESSION_TIMEOUT = 60 * 60 * 1000; // 60 minutes

// Clean up expired sessions periodically (also delete files from disk)
setInterval(() => {
  const now = Date.now();

  // Expire parse sessions
  for (const [sessionId, data] of fileCache.entries()) {
    if (now - data.timestamp > SESSION_TIMEOUT) {
      console.log(`[cache] Expiring session: ${sessionId}`);
      // Delete file from disk
      if (data.filePath && fs.existsSync(data.filePath)) {
        try {
          fs.unlinkSync(data.filePath);
          console.log(`[cache] Deleted file: ${data.filePath}`);
        } catch (e) {
          console.error(`[cache] Failed to delete file: ${data.filePath}`, e);
        }
      }
      fileCache.delete(sessionId);
    }
  }

  // Expire chunk uploads (keep a bit longer than normal sessions)
  const CHUNK_TIMEOUT = 2 * 60 * 60 * 1000; // 2 hours
  for (const [uploadId, data] of chunkSessions.entries()) {
    if (now - data.createdAt > CHUNK_TIMEOUT) {
      console.log(`[chunks] Expiring upload: ${uploadId}`);
      if (data.dirPath && fs.existsSync(data.dirPath)) {
        try {
          fs.rmSync(data.dirPath, { recursive: true, force: true });
          console.log(`[chunks] Deleted dir: ${data.dirPath}`);
        } catch (e) {
          console.error(`[chunks] Failed to delete dir: ${data.dirPath}`, e);
        }
      }
      chunkSessions.delete(uploadId);
    }
  }
}, 5 * 60 * 1000); // Check every 5 minutes

function generateSessionId() {
  return crypto.randomBytes(16).toString('hex');
}

function generateUploadId() {
  return crypto.randomBytes(16).toString('hex');
}

function sanitizeFilename(filename) {
  return String(filename || 'upload.accdb')
    .replace(/[/\\?%*:|"<>]/g, '_')
    .slice(0, 200);
}

function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function getChunkDir(uploadId) {
  return path.join(CHUNKS_DIR, uploadId);
}

function getChunkPath(uploadId, index) {
  return path.join(getChunkDir(uploadId), `${index}.part`);
}

function assertChunkSession(uploadId) {
  if (!uploadId || !chunkSessions.has(uploadId)) {
    const err = new Error('Upload não encontrado ou expirado. Reenvie o arquivo.');
    // @ts-ignore
    err.statusCode = 400;
    throw err;
  }
  return chunkSessions.get(uploadId);
}

function assembleChunksSync(uploadId, totalChunks, outFilePath) {
  const fd = fs.openSync(outFilePath, 'w');
  try {
    for (let i = 0; i < totalChunks; i++) {
      const chunkPath = getChunkPath(uploadId, i);
      if (!fs.existsSync(chunkPath)) {
        const err = new Error(`Chunk ausente: ${i}/${totalChunks - 1}`);
        // @ts-ignore
        err.statusCode = 400;
        throw err;
      }
      const buf = fs.readFileSync(chunkPath);
      fs.writeSync(fd, buf);
    }
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * Helper to read file buffer from disk
 */
function readFileBuffer(filePath) {
  return fs.readFileSync(filePath);
}

// ============================================
// COLUMN MAPPINGS - WELLS
// ============================================
const wellsColumnMappings = {
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

// ============================================
// COLUMN MAPPINGS - PRODUCTION
// ============================================
const productionColumnMappings = {
  wlbr_id: 'wlbr_id',
  wellbore_id: 'wlbr_id',
  'id do poço': 'wlbr_id',
  wlbr_nm: 'wlbr_nm',
  wellbore_name: 'wlbr_nm',
  'nome do poço': 'wlbr_nm',
  'wellbore name': 'wlbr_nm',
  cmpl_id: 'cmpl_id',
  completion_id: 'cmpl_id',
  daytime: 'production_date',
  date: 'production_date',
  data: 'production_date',
  'data produção': 'production_date',
  oil: 'oil_volume',
  óleo: 'oil_volume',
  petroleo: 'oil_volume',
  petróleo: 'oil_volume',
  water: 'water_volume',
  água: 'water_volume',
  agua: 'water_volume',
  gas: 'gas_volume',
  gás: 'gas_volume',
  glg: 'glg',
  'gas lift': 'glg',
  hours: 'hours_produced',
  horas: 'hours_produced',
  choke: 'choke_size',
  bhp: 'bhp',
  bht: 'bht',
  whp: 'whp',
  wht: 'wht',
  chp: 'chp',
};

function normalizeColumnName(name, dataType) {
  const normalized = String(name).toLowerCase().trim();
  const mappings = dataType === 'production' ? productionColumnMappings : wellsColumnMappings;
  return mappings[normalized] || normalized;
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

// ============================================
// DETECT DATA TYPE
// ============================================
function detectDataType(columns) {
  const normalized = columns.map(c => c.toLowerCase());
  const productionIndicators = ['oil', 'gas', 'water', 'daytime', 'wlbr_id', 'cmpl_id', 'bhp', 'whp', 'choke'];
  const wellIndicators = ['latitude', 'longitude', 'block', 'field', 'province', 'poço', 'poco', 'bloco', 'campo'];
  
  const productionMatches = productionIndicators.filter(ind => 
    normalized.some(col => col.includes(ind))
  ).length;
  
  const wellMatches = wellIndicators.filter(ind => 
    normalized.some(col => col.includes(ind))
  ).length;
  
  console.log(`[detectDataType] production=${productionMatches}, wells=${wellMatches}`);
  return productionMatches > wellMatches ? 'production' : 'wells';
}

// ============================================
// PARSE WELLS ROW
// ============================================
function parseWellsRow(row, columns, rowIndex) {
  const original = {};
  const mapped = {};

  columns.forEach((col) => {
    const normalizedCol = normalizeColumnName(col, 'wells');
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
    return {
      row: rowIndex + 1,
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
    };
  } else {
    return { row: rowIndex + 1, data: null, errors, original };
  }
}

// ============================================
// PARSE PRODUCTION ROW
// ============================================
function parseProductionRow(row, columns, rowIndex) {
  const original = {};
  const mapped = {};

  columns.forEach((col) => {
    const normalizedCol = normalizeColumnName(col, 'production');
    original[col] = row[col];
    mapped[normalizedCol] = row[col];
  });

  const errors = [];

  const wlbrId = mapped.wlbr_id;
  if (!wlbrId) errors.push('Wellbore ID é obrigatório');

  const productionDate = parseDate(mapped.production_date);
  if (!productionDate) errors.push('Data de produção é obrigatória');

  if (errors.length === 0) {
    return {
      row: rowIndex + 1,
      data: {
        wlbr_id: String(wlbrId),
        wlbr_nm: mapped.wlbr_nm ? String(mapped.wlbr_nm) : null,
        cmpl_id: mapped.cmpl_id ? String(mapped.cmpl_id) : null,
        production_date: productionDate,
        oil_volume: parseNumber(mapped.oil_volume) || 0,
        water_volume: parseNumber(mapped.water_volume) || 0,
        gas_volume: parseNumber(mapped.gas_volume) || 0,
        glg: parseNumber(mapped.glg) || 0,
        hours_produced: parseNumber(mapped.hours_produced) || 0,
        choke_size: parseNumber(mapped.choke_size) || 0,
        bhp: parseNumber(mapped.bhp) || 0,
        bht: parseNumber(mapped.bht) || 0,
        whp: parseNumber(mapped.whp) || 0,
        wht: parseNumber(mapped.wht) || 0,
        chp: parseNumber(mapped.chp) || 0,
      },
      errors: [],
      original,
    };
  } else {
    return { row: rowIndex + 1, data: null, errors, original };
  }
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    activeSessions: fileCache.size,
    activeChunkUploads: chunkSessions.size,
    chunkSizeBytes: CHUNK_SIZE,
  });
});

// ============================================
// CHUNKED UPLOAD - INIT / CHUNK / COMPLETE
// ============================================
app.post('/upload-init', (req, res) => {
  try {
    const { filename, size, totalChunks } = req.body || {};

    if (!filename || typeof filename !== 'string') {
      return res.status(400).json({ success: false, error: 'Nome do arquivo inválido' });
    }

    const uploadId = generateUploadId();
    const dirPath = getChunkDir(uploadId);
    ensureDir(dirPath);

    chunkSessions.set(uploadId, {
      dirPath,
      filename: sanitizeFilename(filename),
      totalChunks: Number(totalChunks) || null,
      size: Number(size) || null,
      createdAt: Date.now(),
    });

    console.log(`[upload-init] uploadId=${uploadId}, file=${filename}, size=${size}, totalChunks=${totalChunks}`);
    res.json({ success: true, uploadId, chunkSize: CHUNK_SIZE });
  } catch (err) {
    console.error('[upload-init] Error:', err);
    res.status(500).json({ success: false, error: err.message || 'Erro ao iniciar upload' });
  }
});

app.get('/upload-status', (req, res) => {
  try {
    const uploadId = String(req.query?.uploadId || '').trim();
    if (!uploadId) {
      return res.status(400).json({ success: false, error: 'uploadId não fornecido' });
    }

    if (!chunkSessions.has(uploadId)) {
      return res.status(404).json({ success: false, error: 'Upload não encontrado ou expirado. Reenvie o arquivo.' });
    }

    const session = chunkSessions.get(uploadId);
    const dirPath = session?.dirPath;

    let receivedIndices = [];
    if (dirPath && fs.existsSync(dirPath)) {
      const entries = fs.readdirSync(dirPath, { withFileTypes: true });
      receivedIndices = entries
        .filter((e) => e.isFile() && e.name.endsWith('.part'))
        .map((e) => Number(e.name.replace('.part', '')))
        .filter((n) => Number.isFinite(n))
        .sort((a, b) => a - b);
    }

    res.json({
      success: true,
      uploadId,
      totalChunks: session?.totalChunks ?? null,
      received: receivedIndices.length,
      receivedIndices,
    });
  } catch (err) {
    console.error('[upload-status] Error:', err);
    res.status(500).json({ success: false, error: err.message || 'Erro ao obter status do upload' });
  }
});

app.post('/upload-chunk', chunkUpload.single('chunk'), (req, res) => {
  try {
    const uploadId = req.body?.uploadId;
    const indexRaw = req.body?.index;
    const totalChunksRaw = req.body?.totalChunks;

    const index = Number(indexRaw);
    const totalChunks = Number(totalChunksRaw);

    if (!uploadId) {
      return res.status(400).json({ success: false, error: 'uploadId não fornecido' });
    }

    if (!Number.isFinite(index) || index < 0) {
      return res.status(400).json({ success: false, error: 'index inválido' });
    }

    if (!Number.isFinite(totalChunks) || totalChunks <= 0) {
      return res.status(400).json({ success: false, error: 'totalChunks inválido' });
    }

    const session = assertChunkSession(uploadId);
    session.createdAt = Date.now(); // keep alive
    if (!session.totalChunks) session.totalChunks = totalChunks;

    if (!req.file || !req.file.buffer) {
      return res.status(400).json({ success: false, error: 'Chunk não fornecido' });
    }

    const dirPath = session.dirPath;
    ensureDir(dirPath);

    const chunkPath = getChunkPath(uploadId, index);

    // Idempotent write: if already exists, we accept it.
    if (!fs.existsSync(chunkPath)) {
      fs.writeFileSync(chunkPath, req.file.buffer);
    }

    res.json({ success: true });
  } catch (err) {
    console.error('[upload-chunk] Error:', err);
    const status = err.statusCode || 500;
    res.status(status).json({ success: false, error: err.message || 'Erro ao receber chunk' });
  }
});

app.post('/upload-complete', async (req, res) => {
  try {
    const { uploadId } = req.body || {};
    const session = assertChunkSession(uploadId);

    const totalChunks = Number(session.totalChunks);
    if (!Number.isFinite(totalChunks) || totalChunks <= 0) {
      return res.status(400).json({ success: false, error: 'totalChunks ausente/ inválido' });
    }

    const sessionId = generateSessionId();
    const finalFilename = `${uploadId}-${session.filename}`;
    const finalPath = path.join(UPLOADS_DIR, finalFilename);

    console.log(`[upload-complete] Assembling uploadId=${uploadId} into ${finalPath}`);

    assembleChunksSync(uploadId, totalChunks, finalPath);

    // Clean up chunks directory
    try {
      fs.rmSync(session.dirPath, { recursive: true, force: true });
    } catch (e) {
      console.warn('[upload-complete] Failed to delete chunks dir:', e);
    }
    chunkSessions.delete(uploadId);

    // Cache file path for later parsing
    fileCache.set(sessionId, {
      filePath: finalPath,
      filename: session.filename,
      timestamp: Date.now(),
    });

    console.log(`[upload-complete] Created session: ${sessionId}`);

    // Parse tables (like /list-tables)
    const buffer = readFileBuffer(finalPath);
    const reader = new MDBReader(buffer);
    const tableNames = reader.getTableNames();

    const tables = tableNames.map((name) => {
      try {
        const table = reader.getTable(name);
        const columns = table.getColumnNames();
        const rowCount = typeof table.rowCount === 'number' ? table.rowCount : 0;
        return { name, rowCount, columns };
      } catch (err) {
        console.error(`Error reading table ${name}:`, err);
        return { name, rowCount: 0, columns: [] };
      }
    });

    console.log(`[upload-complete] Found ${tables.length} tables`);
    res.json({ success: true, tables, sessionId });
  } catch (err) {
    console.error('[upload-complete] Error:', err);
    const status = err.statusCode || 500;
    res.status(status).json({ success: false, error: err.message || 'Erro ao finalizar upload' });
  }
});

app.post('/upload-abort', (req, res) => {
  try {
    const { uploadId } = req.body || {};
    if (!uploadId || !chunkSessions.has(uploadId)) {
      return res.json({ success: true });
    }

    const session = chunkSessions.get(uploadId);
    if (session?.dirPath && fs.existsSync(session.dirPath)) {
      fs.rmSync(session.dirPath, { recursive: true, force: true });
    }

    chunkSessions.delete(uploadId);
    res.json({ success: true });
  } catch (err) {
    console.error('[upload-abort] Error:', err);
    res.status(500).json({ success: false, error: err.message || 'Erro ao abortar upload' });
  }
});

// ============================================
// LIST TABLES - Upload file and return sessionId
// ============================================
app.post('/list-tables', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ success: false, error: 'Arquivo não fornecido' });
    }

    // File is now on disk at req.file.path
    const filePath = req.file.path;
    console.log(`[list-tables] Processing file: ${req.file.originalname}, size: ${req.file.size} bytes, path: ${filePath}`);

    // Generate session ID and cache the file path (not buffer)
    const sessionId = generateSessionId();
    fileCache.set(sessionId, {
      filePath: filePath,
      filename: req.file.originalname,
      timestamp: Date.now(),
    });

    console.log(`[list-tables] Created session: ${sessionId}`);

    // Read file from disk for parsing
    const buffer = readFileBuffer(filePath);
    const reader = new MDBReader(buffer);
    const tableNames = reader.getTableNames();

    const tables = tableNames.map((name) => {
      try {
        const table = reader.getTable(name);
        const columns = table.getColumnNames();
        // IMPORTANT: do NOT call table.getData() here.
        // For large databases, loading all rows of every table can exhaust memory and
        // crash/reset the process, which surfaces in the client as a network upload error.
        const rowCount = typeof table.rowCount === 'number' ? table.rowCount : 0;
        return { name, rowCount, columns };
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
// LIST TABLES FROM URL - Download file from signed URL
// This is used by the Edge Function to avoid memory limits
// ============================================
app.post('/list-tables-from-url', async (req, res) => {
  try {
    const { fileUrl, filename } = req.body;
    
    if (!fileUrl) {
      return res.status(400).json({ success: false, error: 'URL do arquivo não fornecida' });
    }
    
    console.log(`[list-tables-from-url] Downloading file from signed URL: ${filename || 'unknown'}`);
    
    // Download file from the signed URL
    const response = await fetch(fileUrl);
    
    if (!response.ok) {
      console.error(`[list-tables-from-url] Failed to download: ${response.status} ${response.statusText}`);
      return res.status(400).json({ success: false, error: `Erro ao baixar arquivo: ${response.status}` });
    }
    
    // Get file as array buffer and write to disk
    const arrayBuffer = await response.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    
    console.log(`[list-tables-from-url] Downloaded ${buffer.length} bytes`);
    
    // Save to disk
    const sessionId = generateSessionId();
    const safeFilename = sanitizeFilename(filename || 'download.accdb');
    const finalPath = path.join(UPLOADS_DIR, `${sessionId}-${safeFilename}`);
    
    fs.writeFileSync(finalPath, buffer);
    
    // Cache file path
    fileCache.set(sessionId, {
      filePath: finalPath,
      filename: safeFilename,
      timestamp: Date.now(),
    });
    
    console.log(`[list-tables-from-url] Created session: ${sessionId}, file: ${finalPath}`);
    
    // Parse tables
    const reader = new MDBReader(buffer);
    const tableNames = reader.getTableNames();
    
    const tables = tableNames.map((name) => {
      try {
        const table = reader.getTable(name);
        const columns = table.getColumnNames();
        const rowCount = typeof table.rowCount === 'number' ? table.rowCount : 0;
        return { name, rowCount, columns };
      } catch (err) {
        console.error(`Error reading table ${name}:`, err);
        return { name, rowCount: 0, columns: [] };
      }
    });
    
    console.log(`[list-tables-from-url] Found ${tables.length} tables`);
    res.json({ success: true, tables, sessionId });
    
  } catch (err) {
    console.error('[list-tables-from-url] Error:', err);
    res.status(500).json({ success: false, error: err.message || 'Erro ao processar arquivo' });
  }
});

// ============================================
// PARSE TABLE BATCH - Paginated data retrieval
// Avoids memory issues by returning data in batches
// ============================================
app.post('/parse-table-batch', async (req, res) => {
  try {
    const { sessionId, tableName, offset = 0, limit = 1000 } = req.body;
    
    if (!tableName) {
      return res.status(400).json({ success: false, error: 'Nome da tabela não fornecido' });
    }
    
    if (!sessionId || !fileCache.has(sessionId)) {
      return res.status(400).json({ 
        success: false, 
        error: 'Sessão não encontrada. Faça upload novamente.' 
      });
    }
    
    const cached = fileCache.get(sessionId);
    cached.timestamp = Date.now(); // Keep alive
    
    if (!cached.filePath || !fs.existsSync(cached.filePath)) {
      return res.status(400).json({ 
        success: false, 
        error: 'Arquivo da sessão não encontrado.' 
      });
    }
    
    console.log(`[parse-table-batch] Table=${tableName}, offset=${offset}, limit=${limit}`);
    
    const buffer = readFileBuffer(cached.filePath);
    const reader = new MDBReader(buffer);
    const table = reader.getTable(tableName);
    const columns = table.getColumnNames();
    const totalRows = typeof table.rowCount === 'number' ? table.rowCount : 0;
    
    // Get all data and slice (mdb-reader doesn't support pagination natively)
    const allData = table.getData();
    const slicedData = allData.slice(offset, offset + limit);
    
    // Detect data type
    const dataType = detectDataType(columns);
    
    // Parse rows
    const rows = [];
    for (let i = 0; i < slicedData.length; i++) {
      const row = slicedData[i];
      const globalIndex = offset + i;
      const result = dataType === 'production'
        ? parseProductionRow(row, columns, globalIndex)
        : parseWellsRow(row, columns, globalIndex);
      
      rows.push(result);
    }
    
    const hasMore = offset + slicedData.length < totalRows;
    
    console.log(`[parse-table-batch] Returned ${rows.length} rows, hasMore=${hasMore}`);
    
    res.json({
      success: true,
      tableName,
      columns,
      dataType,
      totalRows,
      offset,
      limit,
      rows,
      hasMore,
    });
    
  } catch (err) {
    console.error('[parse-table-batch] Error:', err);
    res.status(500).json({ success: false, error: err.message || 'Erro ao processar tabela' });
  }
});

app.post('/parse-table', upload.single('file'), async (req, res) => {
  try {
    const tableName = req.body.tableName;
    if (!tableName) {
      return res.status(400).json({ success: false, error: 'Nome da tabela não fornecido' });
    }

    let buffer;
    const sessionId = req.body.sessionId;

    // Try to get file from session cache first (read from disk)
    if (sessionId && fileCache.has(sessionId)) {
      console.log(`[parse-table] Using cached file from session: ${sessionId}`);
      const cached = fileCache.get(sessionId);
      cached.timestamp = Date.now(); // Refresh timestamp
      
      if (!cached.filePath || !fs.existsSync(cached.filePath)) {
        return res.status(400).json({ 
          success: false, 
          error: 'Arquivo da sessão não encontrado. Faça upload novamente.' 
        });
      }
      
      buffer = readFileBuffer(cached.filePath);
    } else if (req.file) {
      // Fallback to uploaded file (already on disk)
      console.log(`[parse-table] Using uploaded file: ${req.file.originalname}, size: ${req.file.size} bytes`);
      buffer = readFileBuffer(req.file.path);
      
      // Clean up the uploaded file after reading (not needed for session)
      try {
        fs.unlinkSync(req.file.path);
      } catch (e) {
        console.warn('[parse-table] Failed to delete temp file:', e);
      }
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

    // Detect data type based on columns
    const dataType = detectDataType(columns);
    console.log(`[parse-table] Detected data type: ${dataType} for table ${tableName}`);

    const totalRows = data.length;
    const success = [];
    const failed = [];

    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const result = dataType === 'production' 
        ? parseProductionRow(row, columns, i)
        : parseWellsRow(row, columns, i);

      if (result.data) {
        success.push(result);
      } else {
        failed.push(result);
      }
    }

    console.log(`[parse-table] Processed ${totalRows} rows: ${success.length} valid, ${failed.length} failed`);
    res.json({ success: true, parseResult: { success, failed, totalRows }, dataType });
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
    const cached = fileCache.get(sessionId);
    
    // Delete file from disk
    if (cached.filePath && fs.existsSync(cached.filePath)) {
      try {
        fs.unlinkSync(cached.filePath);
        console.log(`[clear-session] Deleted file: ${cached.filePath}`);
      } catch (e) {
        console.error(`[clear-session] Failed to delete file: ${cached.filePath}`, e);
      }
    }
    
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
