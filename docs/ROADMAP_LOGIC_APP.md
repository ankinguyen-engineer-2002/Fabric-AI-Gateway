# Implementation Plan: Azure Logic App TMSL Proxy

## Goal
Enable XMLA write operations (create/delete measures, relationships) from macOS by using Azure Logic App as a serverless proxy.

---

## Architecture

```
┌─────────────────┐     HTTPS POST      ┌──────────────────────┐
│  macOS Client   │ ──────────────────► │   Azure Logic App    │
│  (Python)       │     + Bearer Token  │   (HTTP Trigger)     │
│                 │     + TMSL JSON     │                      │
└─────────────────┘                     └──────────┬───────────┘
                                                   │
                                                   ▼
                                        ┌──────────────────────┐
                                        │  Power BI Connector  │
                                        │  "Execute queries    │
                                        │   against dataset"   │
                                        └──────────┬───────────┘
                                                   │
                                                   ▼
                                        ┌──────────────────────┐
                                        │  Power BI Service    │
                                        │  (XMLA Endpoint)     │
                                        └──────────────────────┘
```

---

## Proposed Changes

### Azure Portal Setup

#### [NEW] Logic App Workflow

1. **Create Logic App**
   - Go to Azure Portal → Create Resource → Logic App
   - Choose: Consumption plan (pay-per-execution, free tier available)
   - Region: Same as Power BI tenant

2. **Configure HTTP Trigger**
   ```json
   {
     "type": "Request",
     "kind": "Http",
     "inputs": {
       "method": "POST",
       "schema": {
         "type": "object",
         "properties": {
           "workspace_name": {"type": "string"},
           "dataset_name": {"type": "string"},
           "tmsl": {"type": "object"}
         }
       }
     }
   }
   ```

3. **Add Power BI Action**
   - Action: "Run a query against a dataset"
   - Or use HTTP action with XMLA endpoint directly

---

### Python Client Changes

#### [MODIFY] `src/utils/fabric_client_wrapper.py`

Add `LogicAppClient` class as alternative to `FabricDotNetClient`:

```python
class LogicAppClient:
    def __init__(self, logic_app_url: str, token: str, workspace_name: str):
        self.url = logic_app_url
        self.token = token
        self.workspace_name = workspace_name
    
    def execute_tmsl(self, tmsl_dict: dict) -> dict:
        import requests
        response = requests.post(
            self.url,
            json={
                "workspace_name": self.workspace_name,
                "tmsl": tmsl_dict
            },
            headers={"Authorization": f"Bearer {self.token}"}
        )
        return response.json()
```

#### [MODIFY] `config.yaml`

Add Logic App configuration:

```yaml
azure:
  logic_app_url: "https://prod-xx.westus.logic.azure.com/workflows/..."
  # URL từ Logic App HTTP trigger
```

#### [MODIFY] `src/mcp_server.py`

Update `initialize()` to prefer Logic App client:

```python
# Check for Logic App first (works on macOS)
if config.get("azure", {}).get("logic_app_url"):
    self.xmla_client = LogicAppClient(
        config["azure"]["logic_app_url"],
        self.token,
        self.workspace_name
    )
elif DOTNET_CLIENT_AVAILABLE:
    # Fallback to .NET (Windows only)
    self.xmla_client = FabricDotNetClient(...)
```

---

## Verification Plan

### Manual Steps (Azure Portal)
1. Create Logic App in Azure Portal
2. Configure HTTP trigger + Power BI connector
3. Test with Postman/curl
4. Copy Logic App URL to `config.yaml`

### Automated Tests
```bash
# Test from macOS
python3 -c "
from src.utils.fabric_client_wrapper import LogicAppClient
client = LogicAppClient('https://...', token, 'eCentric')
result = client.execute_tmsl({'test': 'ping'})
print(result)
"
```

---

## Security Considerations

> [!IMPORTANT]
> Logic App URL contains SAS token - keep it secret!

- Store URL in environment variable or secure config
- Enable Azure AD authentication on Logic App (optional, more secure)
- Limit IP access if possible

---

## Estimated Cost

| Component | Free Tier | Beyond Free |
|-----------|-----------|-------------|
| Logic App | 4,000 actions/month | ~$0.000025/action |
| Power BI API | Unlimited | N/A |

**For typical usage**: ~$0/month (stays within free tier)
