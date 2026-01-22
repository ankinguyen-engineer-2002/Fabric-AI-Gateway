# CHANGELOG - Fabric AI Gateway

## Lịch sử thay đổi và các lỗi đã sửa

---

## v1.0.0 - 2026-01-22

### 🏗️ Thay đổi Kiến trúc

#### Giai đoạn 1: Foundation
- Tạo cấu trúc dự án cơ bản với `src/`, `tests/`, `scripts/`
- Implement `auth.py` với MSAL authentication và browser flow
- Tạo dual-scope token (Power BI + SQL Database)
- Token cache tại `~/.fabric-gateway/token_cache.bin`

#### Giai đoạn 2: Semantic Model Read
- Implement `semantic_adapter.py` cho REST API + XMLA
- Thêm DAX query execution qua `executeQueries` API
- Sử dụng `COLUMNSTATISTICS()` thay vì `INFO.*` functions (vì Pro không hỗ trợ)

#### Giai đoạn 3: Data Warehouse
- Implement `warehouse_adapter.py` với pyodbc + ODBC Driver 18
- Token authentication dạng binary cho SQL Server
- Thêm các commands: `overview`, `profile`, `sample`, `sql`

#### Giai đoạn 4: MCP Server Integration
- Tạo `mcp_server.py` với JSON-RPC stdio protocol
- Hỗ trợ dual-mode: `semantic` và `warehouse`
- Context persistence tại `~/.fabric-gateway/context.json`

#### Giai đoạn 5: Unified CLI
- Tạo `mcp_cloud_fabric.py` làm entry point chính
- Tích hợp Gemini CLI và Codex CLI thông qua MCP settings
- Menu chọn Mode → Workspace → Interface

---

### 🐛 Các lỗi đã sửa

#### Lỗi Authentication
| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| Token expired | Cache không refresh | Thêm logic check expiry trong `auth.py` |
| SQL token format | pyodbc cần binary token | Convert UTF-8 → binary struct |

#### Lỗi DAX Queries
| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| `INFO.MEASURES()` failed | Chỉ hỗ trợ Premium/Fabric | Fallback message + dùng `COLUMNSTATISTICS()` |
| `executeQueries` 400 error | Pro limitation | Document limitation, suggest Premium |

#### Lỗi Warehouse Connection
| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| Kết nối vào DB sai (`DataflowsStagingWarehouse`) | Không chỉ định Database trong connection string | Thêm prompt nhập Warehouse Name, truyền vào `Database=` parameter |
| Schema `Commercial` không thấy | DB context sai | Fix như trên |

#### Lỗi MCP Server
| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| `pbi_desktop: fetch failed` | URL Cloudflare tunnel cũ/expired | Giữ lại cho dự án cũ, không ảnh hưởng `fabric` |
| `fabric: No prompts, tools found` | Warning làm nhiễu stdout | Suppress warnings, fix JSON response |
| Tools không trả về | `get_tools()` thiếu tools mới | Thêm đầy đủ 13 tools cho Semantic Mode |

#### Lỗi CLI Flow
| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| Semantic Mode không linh hoạt | Bắt chọn dataset trước | Đổi thành chỉ chọn Workspace, AI tự list/connect |
| Warehouse Name sai | Tự parse từ endpoint | Thêm prompt nhập rõ ràng |

#### Lỗi XMLA Client
| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| `XMLA client not available` | Import path sai (relative vs absolute) | Fix import path trong `mcp_server.py` và `xmla_client.py` |
| `workspace_name` missing | Context không save đủ fields | Đảm bảo `save_context()` lưu cả `workspace_name` |
| 404 XMLA endpoint | Power BI XMLA dùng Analysis Services protocol, không phải REST | **macOS Limitation**: Trả về TMSL script cho user chạy trong SSMS/Tabular Editor |

---

### ⚡ Tính năng mới

#### Semantic Model Mode (13 tools)
- `list_workspaces` - Liệt kê workspaces
- `list_datasets` - Liệt kê semantic models
- `connect_dataset` - Kết nối tới model
- `get_tables` - Lấy danh sách tables
- `get_columns` - Lấy chi tiết columns
- `get_measures` - Lấy measures (Premium/Fabric)
- `get_relationships` - Lấy relationships (Premium/Fabric)
- `execute_dax` - Chạy DAX query
- `get_dataset_info` - Metadata
- `refresh_dataset` - Trigger refresh
- `create_measure` - Tạo measure (TMSL)
- `delete_measure` - Xóa measure (TMSL)
- `create_relationship` - Tạo relationship (TMSL)

#### Data Warehouse Mode (3 tools)
- `get_warehouse_tables` - Liệt kê tables
- `execute_sql` - Chạy SQL SELECT
- `describe_table` - Lấy schema bảng

---

### 📁 Files đã xóa/cleanup
- `debug_warehouse.py` - Script debug tạm
- `simulate_mcp_warehouse.py` - Script test
- `pytest.ini` - Config pytest không cần
- `.pytest_cache/` - Cache pytest
- `src/quickstart.py` - CLI cũ, thay bằng `cli.py`
- `src/main.py` - Entry point cũ, thay bằng `mcp_cloud_fabric.py`

---

### 🔧 Dependencies
```
msal>=1.24.0
requests>=2.31.0
PyYAML>=6.0
rich>=13.0.0
pydantic>=2.0.0
pyodbc>=5.0.0
tabulate>=0.9.0
```

### 📋 System Requirements
- macOS (Apple Silicon hoặc Intel)
- Python 3.9+
- ODBC Driver 18 for SQL Server (`brew install msodbcsql18`)
- Azure AD App Registration với Fabric permissions
