
using System;
using System.IO;
using Microsoft.AnalysisServices.Tabular;
using TOM = Microsoft.AnalysisServices.Tabular;
using Microsoft.AnalysisServices.AdomdClient;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;

namespace FabricClient
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.Error.WriteLine("Usage: FabricClient <endpoint> <token_file> [script_file]");
                return;
            }

            string endpoint = args[0];
            string tokenFile = args[1];
            string scriptFile = args.Length > 2 ? args[2] : null;

            if (!File.Exists(tokenFile))
            {
                Console.Error.WriteLine($"Token file not found: {tokenFile}");
                return;
            }

            string token = File.ReadAllText(tokenFile).Trim();
            // TOM Connection String
            string tomConnStr = $"DataSource={endpoint};Password={token};";

            try
            {
                // Check if we have a script file to execute
                if (!string.IsNullOrEmpty(scriptFile) && File.Exists(scriptFile))
                {
                    string content = File.ReadAllText(scriptFile);
                    
                    // SMART MODE: Check if it's our custom Operation JSON
                    if (content.Contains("\"operation\": \"upsert_measure\"")) 
                    {
                        Console.WriteLine("Detected Smart Operation: Upsert Measure");
                        HandleUpsertMeasure(tomConnStr, content);
                        return;
                    }
                    else if (content.Contains("\"operation\": \"delete_measure\""))
                    {
                        Console.WriteLine("Detected Smart Operation: Delete Measure");
                        HandleDeleteMeasure(tomConnStr, content);
                        return;
                    }
                    else if (content.Contains("\"operation\": \"delete_relationship\""))
                    {
                        Console.WriteLine("Detected Smart Operation: Delete Relationship");
                        HandleDeleteRelationship(tomConnStr, content);
                        return;
                    }

                    // Default: Execute as TMSL/DAX via ADOMD
                    ExecuteRaw(tomConnStr, content);
                }
                else
                {
                    // Just test connection
                    ExecuteRaw(tomConnStr, "EVALUATE {1}"); 
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Error: " + ex.Message);
                Console.Error.WriteLine(ex.StackTrace);
                Environment.Exit(1);
            }
        }

        static void ExecuteRaw(string connStr, string script)
        {
             using (var conn = new AdomdConnection(connStr))
            {
                conn.Open();
                Console.WriteLine("Connected successfully (ADOMD).");

                if (!string.IsNullOrWhiteSpace(script) && script != "EVALUATE {1}")
                {
                     var cmd = conn.CreateCommand();
                    cmd.CommandText = script;
                    string trimmed = script.TrimStart();
                    // Robust check: if it contains <Create or starts with {, treat as command
                    bool isCommand = trimmed.StartsWith("{") || script.Contains("<Create") || script.Contains("<Delete");
                    
                    if (isCommand)
                    {
                        Console.WriteLine("Executing Command (TMSL/XMLA)...");
                        cmd.ExecuteNonQuery();
                        Console.WriteLine("Command Execution completed successfully.");
                    }
                    else
                    {
                        Console.WriteLine("Executing DAX...");
                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                for (int i = 0; i < reader.FieldCount; i++)
                                    Console.Write(reader.GetName(i) + ": " + reader[i] + "\t");
                                Console.WriteLine();
                            }
                        }
                    }
                }
            }
        }

        static void HandleUpsertMeasure(string connStr, string jsonContent)
        {
            try 
            {
                // Parse JSON
                var jobj = JObject.Parse(jsonContent);
                string tableName = jobj["table"]?.ToString();
                string dbNameTarget = jobj["database"]?.ToString();
                var mObj = jobj["measure"];
                string mName = mObj["name"]?.ToString();
                string mExpr = mObj["expression"]?.ToString();
                string mDesc = mObj["description"]?.ToString();
                string mFormat = mObj["formatString"]?.ToString();

                Console.WriteLine($"[TOM Mode] Upserting measure '{mName}' on table '{tableName}' in DB '{dbNameTarget}'...");

                // Use TOM Server instead of ADOMD for write operations
                using (var server = new TOM.Server())
                {
                    server.Connect(connStr);
                    Console.WriteLine("Connected to TOM Server.");
                    
                    // Get the database
                    TOM.Database db = null;
                    if (!string.IsNullOrEmpty(dbNameTarget))
                    {
                        db = server.Databases.FindByName(dbNameTarget);
                        if (db == null)
                        {
                            // Try by ID if name doesn't match
                            foreach (TOM.Database d in server.Databases)
                            {
                                Console.WriteLine($"  Found DB: {d.Name}");
                                if (d.Name.Equals(dbNameTarget, StringComparison.OrdinalIgnoreCase))
                                {
                                    db = d;
                                    break;
                                }
                            }
                        }
                    }
                    else if (server.Databases.Count > 0)
                    {
                        db = server.Databases[0];
                    }

                    if (db == null)
                    {
                        throw new Exception($"Database '{dbNameTarget}' not found.");
                    }
                    
                    Console.WriteLine($"Using database: {db.Name}");
                    
                    // Get the model and table
                    var model = db.Model;
                    var table = model.Tables.Find(tableName);
                    
                    if (table == null)
                    {
                        throw new Exception($"Table '{tableName}' not found in model.");
                    }
                    
                    Console.WriteLine($"Found table: {table.Name}");
                    
                    // Find or create measure
                    var measure = table.Measures.Find(mName);
                    if (measure != null)
                    {
                        Console.WriteLine($"Updating existing measure: {mName}");
                        measure.Expression = mExpr;
                        if (!string.IsNullOrEmpty(mDesc)) measure.Description = mDesc;
                        if (!string.IsNullOrEmpty(mFormat)) measure.FormatString = mFormat;
                    }
                    else
                    {
                        Console.WriteLine($"Creating new measure: {mName}");
                        measure = new TOM.Measure();
                        measure.Name = mName;
                        measure.Expression = mExpr;
                        if (!string.IsNullOrEmpty(mDesc)) measure.Description = mDesc;
                        if (!string.IsNullOrEmpty(mFormat)) measure.FormatString = mFormat;
                        table.Measures.Add(measure);
                    }
                    
                    // Save changes - THIS IS THE KEY!
                    Console.WriteLine("Saving changes to server...");
                    model.SaveChanges();
                    
                    Console.WriteLine("Success: Measure upserted and saved!");
                }
            }
            catch (Exception ex)
            {
               Console.Error.WriteLine("Error in HandleUpsertMeasure: " + ex.Message);
               Console.Error.WriteLine(ex.StackTrace);
               throw;
            }
        }
        
        static void HandleDeleteMeasure(string connStr, string jsonContent)
        {
            try
            {
                var jobj = JObject.Parse(jsonContent);
                string tableName = jobj["table"]?.ToString();
                string dbNameTarget = jobj["database"]?.ToString();
                string mName = jobj["measure"]?.ToString();

                Console.WriteLine($"[TOM Mode] Deleting measure '{mName}' from table '{tableName}' in DB '{dbNameTarget}'...");

                using (var server = new TOM.Server())
                {
                    server.Connect(connStr);
                    Console.WriteLine("Connected to TOM Server.");
                    
                    // Get the database
                    TOM.Database db = null;
                    if (!string.IsNullOrEmpty(dbNameTarget))
                    {
                        db = server.Databases.FindByName(dbNameTarget);
                        if (db == null)
                        {
                            foreach (TOM.Database d in server.Databases)
                            {
                                if (d.Name.Equals(dbNameTarget, StringComparison.OrdinalIgnoreCase))
                                {
                                    db = d;
                                    break;
                                }
                            }
                        }
                    }
                    else if (server.Databases.Count > 0)
                    {
                        db = server.Databases[0];
                    }

                    if (db == null)
                    {
                        Console.WriteLine($"Database '{dbNameTarget}' not found.");
                        return;
                    }
                    
                    Console.WriteLine($"Using database: {db.Name}");
                    
                    // Get the table
                    var table = db.Model.Tables.Find(tableName);
                    if (table == null)
                    {
                        Console.WriteLine($"Table '{tableName}' not found.");
                        return;
                    }
                    
                    // Find the measure
                    var measure = table.Measures.Find(mName);
                    if (measure == null)
                    {
                        Console.WriteLine($"Measure '{mName}' not found, nothing to delete.");
                        return;
                    }
                    
                    Console.WriteLine($"Found measure: {mName}. Deleting...");
                    table.Measures.Remove(measure);
                    
                    // Save changes
                    Console.WriteLine("Saving changes to server...");
                    db.Model.SaveChanges();
                    
                    Console.WriteLine("Success: Measure deleted and saved!");
                }
            }
            catch (Exception ex)
            {
               Console.Error.WriteLine("Error in HandleDeleteMeasure: " + ex.Message);
               Console.Error.WriteLine(ex.StackTrace);
               throw;
            }
        }

        static void HandleDeleteRelationship(string connStr, string jsonContent)
        {
            try
            {
                var jobj = JObject.Parse(jsonContent);
                string dbNameTarget = jobj["database"]?.ToString();
                string relName = jobj["relationship"]?.ToString();

                Console.WriteLine($"[TOM Mode] Deleting relationship '{relName}' from DB '{dbNameTarget}'...");

                using (var server = new TOM.Server())
                {
                    server.Connect(connStr);
                    Console.WriteLine("Connected to TOM Server.");
                    
                    // Get the database
                    TOM.Database db = null;
                    if (!string.IsNullOrEmpty(dbNameTarget))
                    {
                        db = server.Databases.FindByName(dbNameTarget);
                        if (db == null)
                        {
                            foreach (TOM.Database d in server.Databases)
                            {
                                if (d.Name.Equals(dbNameTarget, StringComparison.OrdinalIgnoreCase))
                                {
                                    db = d;
                                    break;
                                }
                            }
                        }
                    }
                    else if (server.Databases.Count > 0)
                    {
                        db = server.Databases[0];
                    }

                    if (db == null)
                    {
                        Console.WriteLine($"Database '{dbNameTarget}' not found.");
                        return;
                    }
                    
                    Console.WriteLine($"Using database: {db.Name}");
                    
                    // Find the relationship
                    var model = db.Model;
                    TOM.Relationship relationship = null;
                    foreach (var rel in model.Relationships)
                    {
                        if (rel.Name.Equals(relName, StringComparison.OrdinalIgnoreCase))
                        {
                            relationship = rel;
                            break;
                        }
                    }
                    
                    if (relationship == null)
                    {
                        Console.WriteLine($"Relationship '{relName}' not found, nothing to delete.");
                        return;
                    }
                    
                    Console.WriteLine($"Found relationship: {relName}. Deleting...");
                    model.Relationships.Remove(relationship);
                    
                    // Save changes
                    Console.WriteLine("Saving changes to server...");
                    model.SaveChanges();
                    
                    Console.WriteLine("Success: Relationship deleted and saved!");
                }
            }
            catch (Exception ex)
            {
               Console.Error.WriteLine("Error in HandleDeleteRelationship: " + ex.Message);
               Console.Error.WriteLine(ex.StackTrace);
               throw;
            }
        }
    }
}
