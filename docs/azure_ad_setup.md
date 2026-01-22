# Hướng dẫn tạo Azure AD App Registration

## Bước 1: Đăng nhập Azure Portal

Truy cập: https://portal.azure.com

## Bước 2: Tạo App Registration

1. Tìm kiếm **"App registrations"** trong thanh tìm kiếm
2. Click **"+ New registration"**
3. Điền thông tin:
   - **Name:** `Fabric AI Gateway`
   - **Supported account types:** Chọn "Accounts in this organizational directory only"
   - **Redirect URI:** Chọn "Public client/native (mobile & desktop)" và nhập `http://localhost`
4. Click **"Register"**

## Bước 3: Lưu thông tin quan trọng

Sau khi tạo, copy 2 giá trị sau:
- **Application (client) ID:** `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
- **Directory (tenant) ID:** `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`

## Bước 4: Thêm API Permissions

1. Trong app vừa tạo, click **"API permissions"** (menu bên trái)
2. Click **"+ Add a permission"**

### 4.1 Power BI Service:
- Chọn **"APIs my organization uses"**
- Tìm **"Power BI Service"**
- Chọn **"Delegated permissions"**
- Tick các permissions:
  - `Dataset.Read.All`
  - `Dataset.ReadWrite.All`
  - `Workspace.Read.All`
- Click **"Add permissions"**

### 4.2 Azure SQL Database:
- Click **"+ Add a permission"** lần nữa
- Chọn **"APIs my organization uses"**
- Tìm **"Azure SQL Database"**
- Chọn **"Delegated permissions"**
- Tick: `user_impersonation`
- Click **"Add permissions"**

## Bước 5: Grant Admin Consent (nếu cần)

Nếu bạn là admin, click **"Grant admin consent for [Organization]"**

## Bước 6: Cập nhật config.yaml

```yaml
auth:
  client_id: "PASTE_CLIENT_ID_HERE"
  tenant_id: "PASTE_TENANT_ID_HERE"
```

---

**Sau khi hoàn thành, hãy cung cấp cho tôi Client ID và Tenant ID để tiếp tục cấu hình.**
