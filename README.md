# 🚀 Fabric AI Gateway

**MCP Server cho Microsoft Fabric Cloud - Chạy Native trên macOS**

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 📋 Mô tả

Fabric AI Gateway là một **MCP (Model Context Protocol) Server** cho phép AI CLI (Gemini, Codex) tương tác trực tiếp với Microsoft Fabric Cloud mà **không cần Windows VM hoặc Power BI Desktop**.

### Hỗ trợ 2 chế độ:
| Mode | Mô tả | Backend |
|------|-------|---------|
| **Semantic Model** | Đọc schema, thực thi DAX, tạo/sửa Measures | REST API + XMLA |
| **Data Warehouse** | Quét cấu trúc, profiling, sampling, SQL | pyodbc + ODBC 18 |

---

## 🏗️ Kiến trúc Hệ thống

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              USER                                           │
│                                │                                            │
│                    ┌───────────▼───────────┐                                │
│                    │   mcp_cloud_fabric.py │ ◄── Entry Point                │
│                    │   (Auth + Setup)      │                                │
│                    └───────────┬───────────┘                                │
│                                │                                            │
│              ┌─────────────────┼─────────────────┐                          │
│              ▼                 ▼                 ▼                          │
│   ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐            │
│   │   Gemini CLI     │ │   Codex CLI      │ │ Standalone CLI   │            │
│   │                  │ │                  │ │   (src/cli.py)   │            │
│   └────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘            │
│            │ stdio              │ stdio              │ direct               │
│            └────────────────────┼────────────────────┘                      │
│                                 ▼                                           │
│                    ┌───────────────────────┐                                │
│                    │    MCP Server         │                                │
│                    │  (src/mcp_server.py)  │                                │
│                    │                       │                                │
│                    │  ┌─────────────────┐  │                                │
│                    │  │ Context Manager │  │ ◄── ~/.fabric-gateway/         │
│                    │  │                 │  │      context.json              │
│                    │  └─────────────────┘  │                                │
│                    └───────────┬───────────┘                                │
│                                │                                            │
│              ┌─────────────────┴─────────────────┐                          │
│              ▼                                   ▼                          │
│   ┌──────────────────────┐          ┌──────────────────────┐                │
│   │  SEMANTIC MODE       │          │  WAREHOUSE MODE      │                │
│   │  (13 Tools)          │          │  (3 Tools)           │                │
│   │                      │          │                      │                │
│   │  ● list_workspaces   │          │  ● get_warehouse_    │                │
│   │  ● list_datasets     │          │    tables            │                │
│   │  ● connect_dataset   │          │  ● execute_sql       │                │
│   │  ● get_tables        │          │  ● describe_table    │                │
│   │  ● get_columns       │          │                      │                │
│   │  ● get_measures *    │          └──────────┬───────────┘                │
│   │  ● get_relationships*│                     │                            │
│   │  ● execute_dax       │                     ▼                            │
│   │  ● get_dataset_info  │          ┌──────────────────────┐                │
│   │  ● refresh_dataset   │          │  warehouse_adapter   │                │
│   │  ● create_measure *  │          │  (pyodbc + Token)    │                │
│   │  ● delete_measure *  │          └──────────┬───────────┘                │
│   │  ● create_relationship*         │                            │
│   └──────────┬───────────┘                     │                            │
│              │                                 │                            │
│              ▼                                 ▼                            │
│   ┌──────────────────────┐          ┌──────────────────────┐                │
│   │  semantic_adapter    │          │  Fabric Warehouse    │                │
│   │  (REST API + DAX)    │          │  (SQL Endpoint)      │                │
│   └──────────┬───────────┘          └──────────────────────┘                │
│              │                                                              │
│              ▼                                                              │
│   ┌──────────────────────┐                                                  │
│   │  Power BI Service    │          * = Requires Premium/Fabric             │
│   │  (api.powerbi.com)   │                                                  │
│   └──────────────────────┘                                                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Cấu trúc Project

```
MCP_Cloud_Fabric/
├── mcp_cloud_fabric.py      # 🚀 ENTRY POINT - Unified CLI Launcher
├── config.yaml              # ⚙️ Auth configuration (Client ID, Tenant ID)
├── config.yaml.template     # Template cấu hình
├── requirements.txt         # Python dependencies
├── CHANGELOG.md             # 📝 Lịch sử thay đổi và bug fixes
│
├── src/
│   ├── mcp_server.py        # 🤖 MCP Server (JSON-RPC stdio)
│   ├── cli.py               # 🖥️ Standalone Interactive CLI
│   ├── auth.py              # 🔐 MSAL Authentication
│   ├── context_manager.py   # 📋 State & Context Management
│   ├── semantic_adapter.py  # 📊 Power BI REST API + DAX
│   ├── warehouse_adapter.py # 🗄️ SQL via pyodbc
│   └── utils/
│       ├── xmla_client.py   # XMLA SOAP wrapper
│       └── tmsl_generator.py # TMSL JSON generator
│
├── scripts/
│   ├── fabricgw             # Main menu launcher
│   ├── geminigw             # Gemini CLI launcher
│   └── codexgw              # Codex CLI launcher
│
├── tests/                   # Unit tests
│
└── docs/
    ├── azure_ad_setup.md    # Hướng dẫn Azure AD App
    └── usage_guide.md       # Hướng dẫn sử dụng
```

---

## ⚡ Yêu cầu Hệ thống

| Component | Yêu cầu |
|-----------|---------|
| OS | macOS (Apple Silicon hoặc Intel) |
| Python | 3.9+ |
| ODBC Driver | ODBC Driver 18 for SQL Server |
| Azure AD | App Registration với Fabric permissions |

### Cài đặt ODBC Driver

```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
brew install msodbcsql18
```

---

## 🛠️ Cài đặt

```bash
# Clone repository
git clone <repo-url>
cd MCP_Cloud_Fabric

# Tạo virtual environment
python3 -m venv venv
source venv/bin/activate

# Cài đặt dependencies
pip install -r requirements.txt

# Copy và cấu hình config
cp config.yaml.template config.yaml
# Sửa config.yaml với Client ID và Tenant ID của bạn
```

---

## ⚙️ Cấu hình

### config.yaml

```yaml
auth:
  client_id: "YOUR_AZURE_AD_APP_CLIENT_ID"
  tenant_id: "YOUR_AZURE_AD_TENANT_ID"

limits:
  max_dax_rows: 1000
  sample_rows: 10
```

### Azure AD App Permissions

| API | Permission | Type |
|-----|------------|------|
| Power BI Service | Dataset.Read.All | Delegated |
| Power BI Service | Dataset.ReadWrite.All | Delegated |
| Power BI Service | Workspace.Read.All | Delegated |
| Azure SQL Database | user_impersonation | Delegated |

---

## 🚀 Sử dụng

### Chạy CLI chính

```bash
python mcp_cloud_fabric.py
```

### Flow sử dụng

```
1. Authenticate (Browser login)
         │
         ▼
2. Select Workspace
         │
         ▼
3. Select Mode ─────────────────────────┐
         │                              │
         ▼                              ▼
   [Semantic Model]              [Data Warehouse]
         │                              │
         │                              ▼
         │                    Enter SQL Endpoint
         │                    Enter Warehouse Name
         │                              │
         └──────────────┬───────────────┘
                        │
                        ▼
4. Select Interface ────────────────────┐
         │              │               │
         ▼              ▼               ▼
   [Gemini CLI]   [Codex CLI]   [Standalone]
         │              │               │
         ▼              ▼               ▼
    Chat với AI    Chat với AI    Gõ lệnh thủ công
    về data        về data        (dax, sql, etc.)
```

---

## � MCP Tools Reference

### Semantic Model Mode (13 Tools)

| Tool | Mô tả | Premium? |
|------|-------|----------|
| `list_workspaces` | Liệt kê tất cả workspaces | ❌ |
| `list_datasets` | Liệt kê semantic models trong workspace | ❌ |
| `connect_dataset` | Kết nối tới một model | ❌ |
| `get_tables` | Lấy danh sách tables + số columns | ❌ |
| `get_columns` | Lấy chi tiết columns (có filter) | ❌ |
| `get_measures` | Lấy tất cả measures | ✅ |
| `get_relationships` | Lấy tất cả relationships | ✅ |
| `execute_dax` | Chạy DAX query | ❌ |
| `get_dataset_info` | Lấy metadata dataset | ❌ |
| `refresh_dataset` | Trigger refresh | ❌ |
| `create_measure` | Tạo measure mới (TMSL) | ✅ |
| `delete_measure` | Xóa measure (TMSL) | ✅ |
| `create_relationship` | Tạo relationship (TMSL) | ✅ |

### Data Warehouse Mode (3 Tools)

| Tool | Mô tả |
|------|-------|
| `get_warehouse_tables` | Liệt kê all tables (có filter schema) |
| `execute_sql` | Chạy SQL SELECT query |
| `describe_table` | Lấy schema của bảng |

---

## 🔒 Bảo mật

- ✅ Token cache mã hóa tại `~/.fabric-gateway/token_cache.bin`
- ✅ Kết nối SQL được mã hóa (`Encrypt=yes`)
- ✅ Không sử dụng `TrustServerCertificate=yes`
- ✅ SQL queries được validate để chặn lệnh nguy hiểm
- ✅ Context file chỉ lưu IDs, không lưu credentials

---

## � License

MIT License

---

## 📚 Tài liệu thêm

- [CHANGELOG.md](./CHANGELOG.md) - Lịch sử thay đổi và bug fixes
- [docs/azure_ad_setup.md](./docs/azure_ad_setup.md) - Hướng dẫn setup Azure AD
- [docs/usage_guide.md](./docs/usage_guide.md) - Hướng dẫn sử dụng chi tiết
