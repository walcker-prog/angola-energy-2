# Access Parser Server

Servidor Node.js para processar arquivos Microsoft Access (.accdb, .mdb) de até 200 MB.

## Deploy no Railway

### 1. Preparar o Repositório

1. Crie um novo repositório no GitHub
2. Copie os arquivos desta pasta (`package.json`, `server.js`) para o repositório
3. Faça commit e push

### 2. Deploy no Railway

1. Acesse [railway.app](https://railway.app) e faça login com GitHub
2. Clique em **"New Project"**
3. Selecione **"Deploy from GitHub repo"**
4. Escolha o repositório que você criou
5. Railway vai detectar automaticamente que é um projeto Node.js
6. Aguarde o deploy (geralmente 2-3 minutos)

### 3. Obter a URL

1. Após o deploy, clique no serviço
2. Vá em **Settings** → **Networking** → **Generate Domain**
3. Copie a URL gerada (ex: `https://seu-projeto.up.railway.app`)

### 4. Configurar no Lovable

Volte ao Lovable e informe a URL do servidor para integrar com a página de importação.

## Endpoints

### GET /health
Verifica se o servidor está funcionando.

```bash
curl https://seu-servidor.up.railway.app/health
```

### POST /list-tables
Lista todas as tabelas do arquivo Access.

**Request:**
- `file`: arquivo .accdb ou .mdb (multipart/form-data)

**Response:**
```json
{
  "success": true,
  "tables": [
    { "name": "Pocos", "rowCount": 150, "columns": ["Nome", "Bloco", ...] }
  ]
}
```

### POST /parse-table
Processa uma tabela específica e valida os dados.

**Request:**
- `file`: arquivo .accdb ou .mdb (multipart/form-data)
- `tableName`: nome da tabela a processar

**Response:**
```json
{
  "success": true,
  "parseResult": {
    "success": [...],
    "failed": [...],
    "totalRows": 150
  }
}
```

## Desenvolvimento Local

```bash
npm install
npm run dev
```

O servidor estará disponível em `http://localhost:3000`.

## Limites

- **Tamanho máximo**: 200 MB por arquivo
- **Memória**: Railway Free tier tem ~512 MB RAM, suficiente para arquivos de ~200 MB
- **Timeout**: Arquivos grandes podem levar até 30 segundos para processar
