
import os
import subprocess
import json
import tempfile

class FabricDotNetClient:
    def __init__(self, workspace_name, token):
        self.endpoint = f"powerbi://api.powerbi.com/v1.0/myorg/{workspace_name}"
        self.token = token
        
        # Path to executable
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.exe_path = os.path.join(base_dir, "src/utils/FabricClient/bin/FabricClient/FabricClient")
        
        # Environment
        self.env = os.environ.copy()
        # Use .NET 8 for Homebrew install on Silicon Mac (binary built for net8.0)
        if "DOTNET_ROOT" not in self.env:
            # Try .NET 8 first (preferred for our binary), fallback to default
            dotnet8_path = "/opt/homebrew/opt/dotnet@8/libexec"
            if os.path.exists(dotnet8_path):
                self.env["DOTNET_ROOT"] = dotnet8_path
            else:
                self.env["DOTNET_ROOT"] = "/opt/homebrew/opt/dotnet/libexec"

    def execute_tmsl(self, tmsl_dict):
        tmsl_json = json.dumps(tmsl_dict)
        return self._run(tmsl_json)

    def execute_dax(self, query):
        return self._run(query)

    def _run(self, script_content):
        # Create temp token file and script file
        # Note: We need to be careful with file persistence if process takes time, but tempfile is fine.
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as token_file:
            token_file.write(self.token)
            token_path = token_file.name
            
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as script_file:
            script_file.write(script_content)
            script_path = script_file.name

        try:
            cmd = [self.exe_path, self.endpoint, token_path, script_path]
            
            result = subprocess.run(cmd, capture_output=True, text=True, env=self.env)
            
            # Clean up immediately
            try:
                os.unlink(token_path)
                os.unlink(script_path)
            except:
                pass

            if result.returncode != 0:
                 err = result.stderr or result.stdout
                 return {"status": "error", "message": f"Middleware Error: {err}"}
            
            output = result.stdout
            
            if "TMSL Execution completed successfully" in output:
                return {"status": "success", "message": "Executed successfully via Middleware"}
            
            if "Error:" in result.stderr:
                return {"status": "error", "message": result.stderr}

            return {"status": "success", "output": output}
            
        except Exception as e:
            # Clean up on error
            if os.path.exists(token_path): os.unlink(token_path)
            if os.path.exists(script_path): os.unlink(script_path)
            return {"status": "error", "message": str(e)}
