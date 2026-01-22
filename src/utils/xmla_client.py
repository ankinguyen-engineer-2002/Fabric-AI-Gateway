"""
XMLA Client for Fabric AI Gateway

Provides SOAP-based communication with Power BI XMLA endpoints for:
- DMV queries (schema discovery)
- DAX query execution
- TMSL command execution
"""

from typing import Optional, Any
import xml.etree.ElementTree as ET

import requests

from ..auth import FabricAuthManager


# XMLA SOAP envelope templates
XMLA_DISCOVER_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <Discover xmlns="urn:schemas-microsoft-com:xml-analysis">
      <RequestType>{request_type}</RequestType>
      <Restrictions>
        <RestrictionList>
          {restrictions}
        </RestrictionList>
      </Restrictions>
      <Properties>
        <PropertyList>
          <Catalog>{catalog}</Catalog>
        </PropertyList>
      </Properties>
    </Discover>
  </soap:Body>
</soap:Envelope>"""

XMLA_EXECUTE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
      <Command>
        <Statement>{statement}</Statement>
      </Command>
      <Properties>
        <PropertyList>
          <Catalog>{catalog}</Catalog>
          <Format>Tabular</Format>
        </PropertyList>
      </Properties>
    </Execute>
  </soap:Body>
</soap:Envelope>"""

XMLA_TMSL_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
      <Command>
        <Statement>
          <![CDATA[{tmsl_json}]]>
        </Statement>
      </Command>
      <Properties>
        <PropertyList>
          <Catalog>{catalog}</Catalog>
        </PropertyList>
      </Properties>
    </Execute>
  </soap:Body>
</soap:Envelope>"""


class XMLAClient:
    """
    Client for XMLA SOAP communication with Power BI.
    
    Note: This is a simplified implementation. Production use may require
    additional error handling and connection pooling.
    """
    
    def __init__(
        self, 
        endpoint: str, 
        auth: FabricAuthManager,
        catalog: Optional[str] = None
    ):
        """
        Initialize XMLA client.
        
        Args:
            endpoint: XMLA endpoint URL (e.g., powerbi://api.powerbi.com/v1.0/myorg/WorkspaceName)
            auth: Authenticated FabricAuthManager
            catalog: Database/Model name (optional, extracted from endpoint if not provided)
        """
        self.endpoint = self._normalize_endpoint(endpoint)
        self.auth = auth
        self.catalog = catalog or self._extract_catalog(endpoint)
    
    def _normalize_endpoint(self, endpoint: str) -> str:
        """Convert powerbi:// URL to HTTPS XMLA endpoint."""
        if endpoint.startswith("powerbi://"):
            # Convert powerbi://api.powerbi.com/v1.0/myorg/Workspace
            # to https://api.powerbi.com/v1.0/myorg/Workspace/xmla
            https_url = endpoint.replace("powerbi://", "https://")
            if not https_url.endswith("/xmla"):
                https_url = https_url.rstrip("/") + "/xmla"
            return https_url
        return endpoint
    
    def _extract_catalog(self, endpoint: str) -> str:
        """Extract catalog name from endpoint URL."""
        # Extract workspace/database name from URL
        parts = endpoint.rstrip("/").split("/")
        if parts:
            return parts[-1]
        return "Model"
    
    def _get_headers(self) -> dict[str, str]:
        """Get authorization headers."""
        token = self.auth.get_token(service="powerbi")
        if not token:
            raise RuntimeError("Not authenticated to Power BI")
        
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/soap+xml; charset=utf-8",
            "SOAPAction": "urn:schemas-microsoft-com:xml-analysis:Execute"
        }
    
    def _parse_soap_response(self, response_text: str) -> list[dict[str, Any]]:
        """Parse SOAP response and extract row data."""
        # Remove namespaces for easier parsing
        response_text = response_text.replace('xmlns:', 'xmlns_')
        
        root = ET.fromstring(response_text)
        
        # Find all row elements
        rows = []
        for row_elem in root.iter():
            if row_elem.tag.endswith("row") or "row" in row_elem.tag.lower():
                row_dict = {}
                for child in row_elem:
                    # Extract column name (remove namespace)
                    tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    row_dict[tag] = child.text
                if row_dict:
                    rows.append(row_dict)
        
        return rows
    
    async def query_dmv(self, dmv_query: str) -> list[dict[str, Any]]:
        """
        Execute a DMV (Dynamic Management View) query.
        
        Args:
            dmv_query: DMV query (e.g., SELECT * FROM $SYSTEM.TMSCHEMA_TABLES)
            
        Returns:
            List of row dictionaries
        """
        # Use Execute with Statement for DMV queries
        soap_body = XMLA_EXECUTE_TEMPLATE.format(
            statement=dmv_query,
            catalog=self.catalog
        )
        
        response = requests.post(
            self.endpoint,
            data=soap_body,
            headers=self._get_headers()
        )
        response.raise_for_status()
        
        return self._parse_soap_response(response.text)
    
    async def execute_dax(
        self, 
        dax_query: str, 
        max_rows: int = 1000
    ) -> list[dict[str, Any]]:
        """
        Execute a DAX query.
        
        Args:
            dax_query: DAX query (must start with EVALUATE)
            max_rows: Maximum rows to return
            
        Returns:
            List of row dictionaries
        """
        # Wrap query with TOPN if no limit specified
        query_upper = dax_query.strip().upper()
        if "TOPN" not in query_upper and max_rows > 0:
            # Try to wrap with TOPN for safety
            dax_query = f"EVALUATE TOPN({max_rows}, {dax_query.replace('EVALUATE', '').strip()})"
        
        soap_body = XMLA_EXECUTE_TEMPLATE.format(
            statement=dax_query,
            catalog=self.catalog
        )
        
        response = requests.post(
            self.endpoint,
            data=soap_body,
            headers=self._get_headers()
        )
        response.raise_for_status()
        
        return self._parse_soap_response(response.text)
    
    async def execute_tmsl(self, tmsl_json: str) -> dict[str, Any]:
        """
        Execute a TMSL (Tabular Model Scripting Language) command.
        
        Args:
            tmsl_json: TMSL command as JSON string
            
        Returns:
            Execution result/status
        """
        soap_body = XMLA_TMSL_TEMPLATE.format(
            tmsl_json=tmsl_json,
            catalog=self.catalog
        )
        
        response = requests.post(
            self.endpoint,
            data=soap_body,
            headers=self._get_headers()
        )
        response.raise_for_status()
        
        # Parse response for success/failure
        if "success" in response.text.lower():
            return {"status": "success", "message": "TMSL command executed successfully"}
        elif "error" in response.text.lower():
            return {"status": "error", "message": "TMSL command failed", "details": response.text}
        else:
            return {"status": "unknown", "raw_response": response.text}
