# Hướng dẫn sử dụng Fabric AI Gateway

## 1. Khởi chạy CLI

```bash
cd /Users/MAC/Documents/MCP_Cloud_Fabric
source venv/bin/activate
python -m src.main
```

---

## 2. Mode 1: Semantic Model (Power BI Cloud)

### Flow:
```
🔐 Checking authentication...
✅ Authenticated as: kiet.nguyen@ecentric.vn

Select Mode:
[1] Semantic Model (Power BI Cloud)
[2] Data Warehouse Analytics
[0] Exit

> 1

📊 Select Workspace:
[1] Sales Analytics
[2] Finance Reports
[3] Marketing Dashboard
...

> 1

📊 Select Semantic Model:
[1] Sales Model 2024
[2] Revenue Analysis
...

> 1

✅ Connected to: Sales Analytics / Sales Model 2024
```

### Các tool có thể sử dụng sau khi connect:

| Tool | Cách dùng |
|------|-----------|
| `semantic_get_schema` | Xem schema (tables, columns, measures) |
| `semantic_execute_dax` | Chạy DAX query (phải bắt đầu bằng EVALUATE) |
| `semantic_measure_upsert` | Tạo/sửa measure |
| `semantic_measure_delete` | Xóa measure |

### Ví dụ DAX Query:
```dax
EVALUATE 
TOPN(10, Sales, Sales[Amount], DESC)
```

---

## 3. Mode 2: Data Warehouse Analytics

### Flow:
```
Select Mode:
[1] Semantic Model (Power BI Cloud)
[2] Data Warehouse Analytics

> 2

🗄️ Data Warehouse Mode

Enter SQL endpoint: myworkspace.datawarehouse.fabric.microsoft.com

schema.table (blank for overview): dbo.Sales

✅ Warehouse endpoint set: myworkspace.datawarehouse.fabric.microsoft.com
```

### Các tool có thể sử dụng:

| Tool | Cách dùng |
|------|-----------|
| `dwh_overview` | Quét tổng quan warehouse (schemas, tables, row counts) |
| `dwh_profile_table` | Phân tích chi tiết 1 bảng (columns, types, null %, distinct values) |
| `dwh_sample_rows` | Lấy sample dữ liệu từ bảng (mặc định 10 rows) |
| `dwh_execute_sql` | Chạy SQL SELECT query |

### Lấy SQL Endpoint từ Fabric:
1. Vào Microsoft Fabric → Workspace
2. Click vào Data Warehouse
3. Copy **SQL connection string** từ Settings

---

## 4. Sử dụng với MCP Client (AI CLI)

Để sử dụng với AI CLI như Claude hay GPT, chạy server mode:

```bash
python -m src.main --server
```

Server sẽ giao tiếp qua stdio với MCP protocol.

### Cấu hình MCP Client:

Thêm vào file config của MCP client:

```json
{
  "mcpServers": {
    "fabric-gateway": {
      "command": "python",
      "args": ["-m", "src.main", "--server"],
      "cwd": "/Users/MAC/Documents/MCP_Cloud_Fabric",
      "env": {
        "PATH": "/Users/MAC/Documents/MCP_Cloud_Fabric/venv/bin:$PATH"
      }
    }
  }
}
```

---

## 5. Tips & Troubleshooting

### Token hết hạn:
Token tự động refresh. Nếu gặp lỗi 401, xóa cache:
```bash
rm ~/.fabric-gateway/token_cache.bin
```

### ODBC Driver không tìm thấy:
```bash
odbcinst -q -d  # Kiểm tra driver đã cài
```

### Kiểm tra permissions:
Đảm bảo Azure AD App có đủ permissions (xem `docs/azure_ad_setup.md`)
