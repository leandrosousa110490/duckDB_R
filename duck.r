# DuckDB SQL Query Interface
# A Shiny application for executing SQL queries on DuckDB databases

library(shiny)
library(DBI)
library(duckdb)
library(DT)
library(shinythemes)
library(shinyjs)
library(shinyWidgets)
library(shinydashboard)
library(shinyFiles)

# UI Definition
ui <- fluidPage(
  theme = shinytheme("flatly"),
  useShinyjs(),
  tags$head(
    tags$style(HTML("
      .btn-primary { margin: 5px 0; }
      .well { background-color: #f8f9fa; }
      .nav-tabs { margin-bottom: 15px; }
      #sql_query { font-family: monospace; }
      .info-box { margin-bottom: 15px; }
      .file-input { margin-top: 10px; }
      .recent-conn { cursor: pointer; color: #337ab7; }
      .recent-conn:hover { text-decoration: underline; }
      #database_explorer { max-height: 300px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; margin-top: 10px; }
      .table-item { cursor: pointer; padding: 5px; margin: 2px 0; border-radius: 3px; }
      .table-item:hover { background-color: #f5f5f5; }
      .schema-item { font-weight: bold; margin-top: 8px; }
    "))
  ),
  titlePanel(div(
    icon("database"), 
    "DuckDB SQL Query Interface", 
    style = "color: #2c3e50; font-weight: bold;"
  )),
  
  sidebarLayout(
    sidebarPanel(
      # Database connection section
      box(
        width = 12,
        title = "Database Connection", 
        status = "primary", 
        solidHeader = TRUE,
        collapsible = TRUE,
        
        radioButtons("conn_type", "Connection Type:", 
                     choices = c("In-Memory" = "memory", 
                                 "File-based" = "file"),
                     selected = "memory"),
        conditionalPanel(
          condition = "input.conn_type == 'file'",
          div(
            style = "display: flex; justify-content: space-between; align-items: center;",
            textInput("db_path", "Database Path:", value = "my_database.duckdb", width = "80%"),
            shinyFilesButton("file_select", "Browse", "Select DuckDB file", 
                             multiple = FALSE, class = "btn-sm btn-info")
          ),
          checkboxInput("create_db", "Create if it doesn't exist", value = TRUE),
          conditionalPanel(
            condition = "output.has_recent_connections == 'true'",
            h5("Recent Connections:"),
            uiOutput("recent_connections")
          )
        ),
        div(
          style = "display: flex; justify-content: space-between;",
          actionButton("connect_btn", "Connect", icon = icon("plug"), 
                       class = "btn-success"),
          actionButton("disconnect_btn", "Disconnect", icon = icon("unlink"), 
                       class = "btn-danger")
        ),
        textOutput("connection_status")
      ),
      
      # Database explorer section
      box(
        width = 12,
        title = "Database Explorer", 
        status = "info", 
        solidHeader = TRUE,
        collapsible = TRUE,
        div(id = "database_explorer", 
            uiOutput("db_structure_ui")),
        div(
          style = "margin-top: 10px;",
          actionButton("refresh_explorer_btn", "Refresh", icon = icon("sync"), 
                       class = "btn-sm btn-info")
        )
      ),
      
      # Query input section
      box(
        width = 12,
        title = "SQL Query", 
        status = "primary", 
        solidHeader = TRUE,
        collapsible = TRUE,
        
        textAreaInput("sql_query", NULL, height = "200px", 
                      value = "SELECT * FROM sqlite_master;"),
        checkboxInput("explain_query", "Explain Query Plan", value = FALSE),
        div(
          style = "display: flex; justify-content: space-between;",
          actionButton("run_query_btn", "Run Query", icon = icon("play"), 
                       class = "btn-primary"),
          actionButton("clear_btn", "Clear Results", icon = icon("eraser"))
        )
      ),
      
      # Database tools section
      box(
        width = 12,
        title = "Database Tools", 
        status = "primary", 
        solidHeader = TRUE,
        collapsible = TRUE,
        
        div(
          style = "display: flex; justify-content: space-between; margin-bottom: 10px;",
          actionButton("list_tables_btn", "List Tables", icon = icon("list"), 
                       class = "btn-sm btn-info"),
          actionButton("show_schemas_btn", "Show Schemas", icon = icon("sitemap"), 
                       class = "btn-sm btn-info")
        ),
        hr(),
        # Data import
        selectInput("import_file_type", "File Type:",
                    choices = c("CSV" = "csv", 
                                "Parquet" = "parquet", 
                                "Excel" = "excel"),
                    selected = "csv"),
        
        # Import options
        conditionalPanel(
          condition = "input.import_file_type == 'csv'",
          fileInput("csv_file", "Import CSV File", 
                    accept = c("text/csv", "text/comma-separated-values", ".csv")),
          checkboxInput("csv_header", "First Row is Header", value = TRUE),
          textInput("csv_delimiter", "Delimiter:", value = ",")
        ),
        
        conditionalPanel(
          condition = "input.import_file_type == 'parquet'",
          fileInput("parquet_file", "Import Parquet File", 
                    accept = c(".parquet"))
        ),
        
        conditionalPanel(
          condition = "input.import_file_type == 'excel'",
          fileInput("excel_file", "Import Excel File", 
                    accept = c(".xlsx", ".xls")),
          checkboxInput("excel_header", "First Row is Header", value = TRUE),
          textInput("excel_sheet", "Sheet Name (optional):", value = "")
        ),
        
        textInput("table_name", "Import As Table:", value = "imported_data"),
        
        div(
          style = "display: flex; justify-content: center;",
          actionButton("import_btn", "Import Data", icon = icon("file-import"), 
                       class = "btn-sm btn-primary")
        )
      ),
      width = 3
    ),
    
    mainPanel(
      tabsetPanel(
        id = "main_tabs",
        tabPanel("Results", 
                 box(
                   width = 12,
                   verbatimTextOutput("query_status"),
                   hr(),
                   div(
                     style = "display: flex; justify-content: space-between; margin-bottom: 10px;",
                     downloadButton("download_results", "Download Results"),
                     div(
                       selectInput("results_page_length", "Rows per page:", 
                                   choices = c(10, 15, 25, 50, 100),
                                   selected = 15, width = "150px")
                     )
                   ),
                   DTOutput("results_table")
                 )),
        tabPanel("Database Info", 
                 box(
                   width = 12,
                   h4("Database Structure"),
                   verbatimTextOutput("db_info")
                 )),
        tabPanel("Query History",
                 box(
                   width = 12,
                   h4("Recently Executed Queries"),
                   div(
                     style = "margin-bottom: 10px;",
                     actionButton("clear_history_btn", "Clear History", 
                                  icon = icon("trash"), class = "btn-sm btn-danger"),
                     actionButton("rerun_query_btn", "Rerun Selected Query", 
                                  icon = icon("redo"), class = "btn-sm btn-info")
                   ),
                   DTOutput("query_history_table")
                 )),
        tabPanel("Help",
                 box(
                   width = 12,
                   h4("DuckDB SQL Query Interface Help"),
                   tags$ul(
                     tags$li(tags$b("Connection:"), "Connect to an in-memory database or a file-based DuckDB database."),
                     tags$li(tags$b("Database Explorer:"), "Browse schemas and tables in your connected database."),
                     tags$li(tags$b("SQL Queries:"), "Enter SQL queries in the text area and click 'Run Query'.")
                   ),
                   
                   h4("SQL Reference"),
                   tags$ul(
                     tags$li(tags$b("Basic Queries:"), 
                             tags$ul(
                               tags$li("List all tables: SELECT * FROM sqlite_master;"),
                               tags$li("Create a table: CREATE TABLE tablename (col1 INT, col2 VARCHAR);"),
                               tags$li("Insert data: INSERT INTO tablename VALUES (1, 'value');"),
                               tags$li("Query data: SELECT * FROM tablename WHERE condition;")
                             )
                     ),
                     tags$li(tags$b("Working with Spaces in Names:"), 
                             tags$ul(
                               tags$li("Tables with spaces: SELECT * FROM \"My Table\";"),
                               tags$li("Columns with spaces: SELECT \"First Name\", \"Last Name\" FROM customers;"),
                               tags$li("Table aliases: SELECT t.\"Product Name\" FROM \"Product Items\" t;"),
                               tags$li("Joins with spaces: SELECT c.name, o.\"order date\" FROM customers c JOIN \"Order Details\" o ON c.id = o.customer_id;")
                             )
                     ),
                     tags$li(tags$b("Case Sensitivity:"), "DuckDB identifiers are case-insensitive by default, but become case-sensitive when quoted."),
                     tags$li(tags$b("Special Characters:"), "Use double quotes for any identifier with spaces, special characters, or to preserve case sensitivity.")
                   ),
                   
                   h4("Importing Data"),
                   tags$ul(
                     tags$li(tags$b("CSV Import:"), "Upload a CSV file and specify a table name to import data."),
                     tags$li(tags$b("Parquet Import:"), "Upload a Parquet file and specify a table name to import data."),
                     tags$li(tags$b("Excel Import:"), "Upload an Excel file (.xlsx or .xls) and specify a table name to import data. You can optionally specify a sheet name.")
                   ),
                   
                   h4("Examples with Spaces in Names"),
                   tags$pre(
                     HTML("-- Select from a table with spaces in its name\nSELECT * FROM \"My Table\";\n\n-- Select columns with spaces\nSELECT \"First Name\", \"Last Name\" FROM \"Customer Data\";\n\n-- Join tables with spaces in names and columns\nSELECT c.\"First Name\", o.\"Order Date\", p.\"Product Name\"\nFROM \"Customers\" c\nJOIN \"Orders\" o ON c.\"Customer ID\" = o.\"Customer ID\"\nJOIN \"Products\" p ON o.\"Product ID\" = p.\"Product ID\"\nORDER BY o.\"Order Date\";")
                   )
                 ))
      ),
      width = 9
    )
  )
)

# Server logic
server <- function(input, output, session) {
  # Set up shinyFiles
  volumes <- c(Home = fs::path_home(), getVolumes()())
  shinyFileChoose(input, "file_select", roots = volumes, filetypes = c("duckdb", "db"))
  
  # Helper function to properly quote identifiers with spaces
  quoteIdentifier <- function(identifier) {
    paste0("\"", gsub("\"", "\"\"", identifier), "\"")
  }
  
  # Reactive values to store connection and query results
  values <- reactiveValues(
    conn = NULL,
    query_result = NULL,
    db_info = NULL,
    status = "Not connected to any database.",
    selected_history_row = NULL,
    query_history = data.frame(
      Timestamp = character(),
      Query = character(),
      Status = character(),
      stringsAsFactors = FALSE
    ),
    recent_connections = character(),
    tables_list = list(),
    selected_table = NULL
  )
  
  # Load recent connections from a settings file (if exists)
  observe({
    if (file.exists("duckdb_recent_connections.rds")) {
      values$recent_connections <- readRDS("duckdb_recent_connections.rds")
    }
  })
  
  # Output for recent connections UI
  output$recent_connections <- renderUI({
    if (length(values$recent_connections) > 0) {
      lapply(values$recent_connections, function(conn) {
        actionLink(paste0("recent_", gsub("[^a-zA-Z0-9]", "_", conn)), 
                   label = conn, 
                   class = "recent-conn")
      })
    }
  })
  
  # Handle clicks on recent connections
  observeEvent(input$recent_connections, {
    clicked <- input$recent_connections
    if (!is.null(clicked)) {
      updateTextInput(session, "db_path", value = clicked)
    }
  })
  
  # Check if there are any recent connections
  output$has_recent_connections <- reactive({
    if (length(values$recent_connections) > 0) {
      "true"
    } else {
      "false"
    }
  })
  outputOptions(output, "has_recent_connections", suspendWhenHidden = FALSE)
  
  # Observe file selection
  observeEvent(input$file_select, {
    if (!is.null(input$file_select)) {
      file_selected <- parseFilePaths(volumes, input$file_select)
      if (nrow(file_selected) > 0) {
        updateTextInput(session, "db_path", value = as.character(file_selected$datapath))
      }
    }
  })
  
  # Connect to database
  observeEvent(input$connect_btn, {
    tryCatch({
      # Disconnect if already connected
      if (!is.null(values$conn)) {
        dbDisconnect(values$conn, shutdown = TRUE)
        values$conn <- NULL
      }
      
      # Connect to the specified database
      if (input$conn_type == "memory") {
        values$conn <- dbConnect(duckdb())
        values$status <- "Connected to in-memory DuckDB database."
      } else {
        db_path <- input$db_path
        if (!file.exists(db_path) && !input$create_db) {
          values$status <- paste0("Database file does not exist: ", db_path)
          return()
        }
        values$conn <- dbConnect(duckdb(), dbdir = db_path)
        values$status <- paste0("Connected to DuckDB database at: ", db_path)
        
        # Add to recent connections
        if (!db_path %in% values$recent_connections) {
          values$recent_connections <- unique(c(db_path, values$recent_connections))
          # Keep only the 5 most recent connections
          if (length(values$recent_connections) > 5) {
            values$recent_connections <- values$recent_connections[1:5]
          }
          # Save to file
          saveRDS(values$recent_connections, "duckdb_recent_connections.rds")
        }
      }
      
      # Change button states
      shinyjs::disable("connect_btn")
      shinyjs::enable("disconnect_btn")
      
      # Refresh database explorer
      refreshDatabaseExplorer()
      
    }, error = function(e) {
      values$status <- paste0("Error connecting to database: ", e$message)
    })
  })
  
  # Disconnect from database
  observeEvent(input$disconnect_btn, {
    if (!is.null(values$conn)) {
      dbDisconnect(values$conn, shutdown = TRUE)
      values$conn <- NULL
      values$status <- "Disconnected from database."
      values$query_result <- NULL
      values$db_info <- NULL
      values$tables_list <- list()
      
      # Change button states
      shinyjs::enable("connect_btn")
      shinyjs::disable("disconnect_btn")
    }
  })
  
  # Function to refresh database explorer
  refreshDatabaseExplorer <- function() {
    if (is.null(values$conn)) {
      values$tables_list <- list()
      return()
    }
    
    tryCatch({
      # Print debug information
      print("Refreshing database explorer...")
      
      # Get schemas
      schemas_query <- "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name;"
      schemas <- dbGetQuery(values$conn, schemas_query)
      print(paste("Found schemas:", paste(schemas$schema_name, collapse=", ")))
      
      # Get tables for each schema
      tables_list <- list()
      
      for (schema in schemas$schema_name) {
        tables_query <- paste0(
          "SELECT table_name FROM information_schema.tables ",
          "WHERE table_schema = '", schema, "' ",
          "ORDER BY table_name;"
        )
        tables <- dbGetQuery(values$conn, tables_query)
        
        print(paste("Schema:", schema, "Tables count:", nrow(tables)))
        
        if (nrow(tables) > 0) {
          tables_list[[schema]] <- tables$table_name
          print(paste("Tables in schema", schema, ":", paste(tables$table_name, collapse=", ")))
        }
      }
      
      # Force evaluation of query results and update reactive value
      values$tables_list <- tables_list
      
      # Force additional query to ensure DuckDB registers all tables
      all_tables_query <- "SELECT table_schema, table_name FROM information_schema.tables ORDER BY table_schema, table_name;"
      all_tables <- dbGetQuery(values$conn, all_tables_query)
      print(paste("Total tables found:", nrow(all_tables)))
      
    }, error = function(e) {
      print(paste("Error in refreshDatabaseExplorer:", e$message))
      values$status <- paste0("Error retrieving database structure: ", e$message)
    })
  }
  
  # Refresh database explorer button
  observeEvent(input$refresh_explorer_btn, {
    refreshDatabaseExplorer()
  })
  
  # Render database explorer UI
  output$db_structure_ui <- renderUI({
    if (is.null(values$conn)) {
      return(p("Connect to a database to see its structure."))
    }
    
    if (length(values$tables_list) == 0) {
      return(p("No tables found in the database."))
    }
    
    schema_items <- lapply(names(values$tables_list), function(schema) {
      schema_div <- div(class = "schema-item", 
                        tags$span(icon("folder"), schema))
      
      table_items <- lapply(values$tables_list[[schema]], function(table) {
        table_id <- paste0("table_", schema, "_", table)
        div(class = "table-item",
            id = table_id,
            icon("table"), table,
            onclick = sprintf("Shiny.setInputValue('selected_table', {schema: '%s', table: '%s'});", 
                              schema, table)
        )
      })
      
      tagList(schema_div, div(style = "margin-left: 15px;", table_items))
    })
    
    return(tagList(schema_items))
  })
  
  # Handle table click in explorer
  observeEvent(input$selected_table, {
    if (!is.null(input$selected_table)) {
      values$selected_table <- input$selected_table
      schema <- input$selected_table$schema
      table <- input$selected_table$table
      
      # Generate a SELECT query for the table using the helper function
      if (schema == "main") {
        query <- paste0("SELECT * FROM ", quoteIdentifier(table), " LIMIT 100;")
      } else {
        query <- paste0("SELECT * FROM ", quoteIdentifier(schema), ".", quoteIdentifier(table), " LIMIT 100;")
      }
      
      # Update the query input
      updateTextAreaInput(session, "sql_query", value = query)
    }
  })
  
  # Execute SQL query
  observeEvent(input$run_query_btn, {
    if (is.null(values$conn)) {
      values$status <- "Not connected to any database. Please connect first."
      return()
    }
    
    query <- input$sql_query
    if (trimws(query) == "") {
      values$status <- "Please enter a SQL query."
      return()
    }
    
    tryCatch({
      # Record start time
      start_time <- Sys.time()
      
      # If explain query is checked, add EXPLAIN to the beginning of the query
      if (input$explain_query && !grepl("^\\s*EXPLAIN", toupper(query))) {
        query_to_run <- paste("EXPLAIN", query)
      } else {
        query_to_run <- query
      }
      
      # Check if query is a SELECT or similar query that returns data
      if (grepl("^\\s*(SELECT|PRAGMA|SHOW|DESCRIBE|EXPLAIN)", toupper(query_to_run))) {
        values$query_result <- dbGetQuery(values$conn, query_to_run)
        if (nrow(values$query_result) > 0) {
          # Record end time and calculate duration
          end_time <- Sys.time()
          duration <- round(as.numeric(difftime(end_time, start_time, units = "secs")), 3)
          
          values$status <- paste0("Query executed successfully. Returned ", 
                                  nrow(values$query_result), " rows in ", 
                                  duration, " seconds.")
        } else {
          values$status <- "Query executed successfully. No rows returned."
        }
      } else {
        # For non-SELECT queries (INSERT, UPDATE, CREATE, etc.)
        result <- dbExecute(values$conn, query_to_run)
        
        # Record end time and calculate duration
        end_time <- Sys.time()
        duration <- round(as.numeric(difftime(end_time, start_time, units = "secs")), 3)
        
        values$status <- paste0("Query executed successfully. Affected ", 
                                result, " rows in ", duration, " seconds.")
        values$query_result <- NULL
        
        # Refresh database explorer after DDL statements
        if (grepl("^\\s*(CREATE|DROP|ALTER)", toupper(query))) {
          refreshDatabaseExplorer()
        }
      }
      
      # Add to query history
      values$query_history <- rbind(data.frame(
        Timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        Query = query,
        Status = "Success",
        stringsAsFactors = FALSE
      ), values$query_history)
      
      # Switch to results tab
      updateTabsetPanel(session, "main_tabs", selected = "Results")
      
    }, error = function(e) {
      values$status <- paste0("Error executing query: ", e$message)
      values$query_result <- NULL
      
      # Add to query history
      values$query_history <- rbind(data.frame(
        Timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        Query = query,
        Status = paste0("Error: ", e$message),
        stringsAsFactors = FALSE
      ), values$query_history)
    })
  })
  
  # Clear results
  observeEvent(input$clear_btn, {
    values$query_result <- NULL
    values$status <- "Results cleared."
  })
  
  # List tables
  observeEvent(input$list_tables_btn, {
    if (is.null(values$conn)) {
      values$status <- "Not connected to any database. Please connect first."
      return()
    }
    
    tryCatch({
      # Get all tables from all schemas
      tables_query <- "
        SELECT 
          table_schema as schema_name,
          table_name
        FROM 
          information_schema.tables
        ORDER BY 
          table_schema, table_name;
      "
      
      all_tables <- dbGetQuery(values$conn, tables_query)
      
      if (nrow(all_tables) > 0) {
        # Group by schema
        schemas <- unique(all_tables$schema_name)
        
        # Format the output
        schema_details <- lapply(schemas, function(schema) {
          schema_tables <- all_tables[all_tables$schema_name == schema, "table_name"]
          
          # Get details for each table in this schema
          table_details <- lapply(schema_tables, function(table) {
            # Get column info
            cols_query <- paste0("
              SELECT 
                column_name, data_type
              FROM 
                information_schema.columns
              WHERE 
                table_schema = '", schema, "' AND 
                table_name = '", table, "'
              ORDER BY 
                ordinal_position;
            ")
            
            cols_info <- dbGetQuery(values$conn, cols_query)
            
            # Format column details
            col_details <- paste(
              sapply(1:nrow(cols_info), function(i) {
                paste0("   - ", quoteIdentifier(cols_info$column_name[i]), " (", cols_info$data_type[i], ")")
              }),
              collapse = "\n"
            )
            
            # Get row count (with schema prefix if not 'main')
            # Use proper quoting for table and column names to handle spaces
            if (schema == "main") {
              count_query <- paste0("SELECT COUNT(*) AS count FROM ", quoteIdentifier(table), ";")
            } else {
              count_query <- paste0("SELECT COUNT(*) AS count FROM ", quoteIdentifier(schema), ".", quoteIdentifier(table), ";")
            }
            
            row_count <- tryCatch({
              dbGetQuery(values$conn, count_query)$count
            }, error = function(e) {
              "unknown"
            })
            
            # Return formatted table details
            paste0("\n  Table: ", quoteIdentifier(table), " (", row_count, " rows)\n  Columns:\n", col_details)
          })
          
          # Return schema section
          paste0("\nSchema: ", quoteIdentifier(schema), table_details)
        })
        
        # Combine all schema sections
        values$db_info <- paste0("Database Structure:\n", 
                                 paste(schema_details, collapse = "\n"))
      } else {
        values$db_info <- "No tables found in the database."
      }
      
      values$status <- "Retrieved database structure information."
      
      # Switch to Database Info tab
      updateTabsetPanel(session, "main_tabs", selected = "Database Info")
      
    }, error = function(e) {
      values$status <- paste0("Error retrieving database information: ", e$message)
    })
  })
  
  # Show schemas
  observeEvent(input$show_schemas_btn, {
    if (is.null(values$conn)) {
      values$status <- "Not connected to any database. Please connect first."
      return()
    }
    
    tryCatch({
      schemas_query <- "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name;"
      schemas <- dbGetQuery(values$conn, schemas_query)
      
      if (nrow(schemas) > 0) {
        values$db_info <- paste0("Schemas in database:\n", 
                                 paste(schemas$schema_name, collapse = "\n"))
      } else {
        values$db_info <- "No schemas found in the database."
      }
      values$status <- "Retrieved schema information."
      
      # Switch to Database Info tab
      updateTabsetPanel(session, "main_tabs", selected = "Database Info")
      
    }, error = function(e) {
      values$status <- paste0("Error retrieving schema information: ", e$message)
    })
  })
  
  # Import file
  observeEvent(input$import_btn, {
    # Check if connected to database
    if (is.null(values$conn)) {
      values$status <- "Not connected to any database. Please connect first."
      return()
    }
    
    # Check if table name is provided
    if (trimws(input$table_name) == "") {
      values$status <- "Please provide a table name for the imported data."
      return()
    }
    
    # Get import type
    import_type <- input$import_file_type
    
    tryCatch({
      table_name <- input$table_name
      quoted_table_name <- quoteIdentifier(table_name)
      
      if (import_type == "csv") {
        # Check if file is selected
        if (is.null(input$csv_file)) {
          values$status <- "Please select a CSV file to import."
          return()
        }
        
        file_path <- normalizePath(input$csv_file$datapath, winslash = "/", mustWork = TRUE)
        has_header <- input$csv_header
        delimiter <- input$csv_delimiter
        
        # Try to load extensions, but don't fail if they're not compatible
        tryCatch({
          dbExecute(values$conn, "INSTALL httpfs; LOAD httpfs;")
          # Import using direct file read if extension loaded successfully
          if (has_header) {
            dbExecute(values$conn, paste0("CREATE OR REPLACE VIEW temp_view AS SELECT * FROM read_csv_auto('", 
                                          file_path, "', header=TRUE, delim='", delimiter, "');"))
            dbExecute(values$conn, paste0("CREATE TABLE ", quoted_table_name, " AS SELECT * FROM temp_view;"))
            dbExecute(values$conn, "DROP VIEW temp_view;")
          } else {
            dbExecute(values$conn, paste0("CREATE OR REPLACE VIEW temp_view AS SELECT * FROM read_csv_auto('", 
                                          file_path, "', header=FALSE, delim='", delimiter, "');"))
            dbExecute(values$conn, paste0("CREATE TABLE ", quoted_table_name, " AS SELECT * FROM temp_view;"))
            dbExecute(values$conn, "DROP VIEW temp_view;")
          }
        }, error = function(e) {
          # If extensions fail, fall back to R import and then dbWriteTable
          print(paste("Extension error, using R fallback method:", e$message))
          values$status <- "Using R to load CSV (extension not available)"
          
          # Read CSV using readr or base R
          if (requireNamespace("readr", quietly = TRUE)) {
            csv_data <- readr::read_delim(file_path, delim = delimiter, col_names = has_header)
          } else {
            csv_data <- read.csv(file_path, header = has_header, sep = delimiter, stringsAsFactors = FALSE)
          }
          
          # Clean column names if needed
          names(csv_data) <- make.names(names(csv_data), unique = TRUE)
          
          # Write to database
          dbWriteTable(values$conn, table_name, csv_data, overwrite = TRUE)
        })
        
        values$status <- paste0("CSV import: Created table ", quoted_table_name)
        file_type_desc <- "CSV"
      } 
      else if (import_type == "parquet") {
        # Check if file is selected
        if (is.null(input$parquet_file)) {
          values$status <- "Please select a Parquet file to import."
          return()
        }
        
        file_path <- normalizePath(input$parquet_file$datapath, winslash = "/", mustWork = TRUE)
        
        # Try to load parquet extension but gracefully handle failure
        success <- tryCatch({
          dbExecute(values$conn, "INSTALL parquet; LOAD parquet;")
          # Import the data if extension loaded successfully
          dbExecute(values$conn, paste0("CREATE OR REPLACE VIEW temp_view AS SELECT * FROM read_parquet('", file_path, "');"))
          dbExecute(values$conn, paste0("CREATE TABLE ", quoted_table_name, " AS SELECT * FROM temp_view;"))
          dbExecute(values$conn, "DROP VIEW temp_view;")
          TRUE
        }, error = function(e) {
          # If extension fails, try alternative method
          print(paste("Parquet extension error:", e$message))
          FALSE
        })
        
        if (!success) {
          # Try using arrow package as fallback
          if (!requireNamespace("arrow", quietly = TRUE)) {
            install.packages("arrow")
            library(arrow)
          } else {
            library(arrow)
          }
          
          values$status <- "Using R Arrow package to load Parquet (extension not available)"
          parquet_data <- arrow::read_parquet(file_path)
          # Write to database
          dbWriteTable(values$conn, table_name, parquet_data, overwrite = TRUE)
        }
        
        values$status <- paste0("Parquet import: Created table ", quoted_table_name)
        file_type_desc <- "Parquet"
      }
      else if (import_type == "excel") {
        # Check if file is selected
        if (is.null(input$excel_file)) {
          values$status <- "Please select an Excel file to import."
          return()
        }
        
        # For Excel files, we need to use readxl package to first read into R
        # and then insert into DuckDB
        if (!requireNamespace("readxl", quietly = TRUE)) {
          install.packages("readxl")
          library(readxl)
        } else {
          library(readxl)
        }
        
        file_path <- input$excel_file$datapath
        has_header <- input$excel_header
        sheet_name <- input$excel_sheet
        
        # Read Excel file
        if (trimws(sheet_name) != "") {
          excel_data <- readxl::read_excel(file_path, sheet = sheet_name, col_names = has_header)
        } else {
          excel_data <- readxl::read_excel(file_path, col_names = has_header)
        }
        
        # Create table and insert data using a more reliable method
        # First convert column names to be SQL compatible if needed
        names(excel_data) <- make.names(names(excel_data), unique = TRUE)
        
        # Use DBI's dbWriteTable which is more reliable
        dbWriteTable(values$conn, table_name, excel_data, overwrite = TRUE)
        
        values$status <- paste0("Excel import: Created table ", quoted_table_name)
        file_type_desc <- "Excel"
      }
      
      # Wait a moment to ensure the database has time to process
      Sys.sleep(0.5)
      
      # Explicitly verify the table was created
      table_check <- dbGetQuery(values$conn, paste0("SELECT * FROM information_schema.tables 
                                                   WHERE table_name = '", table_name, "';"))
      
      if (nrow(table_check) > 0) {
        # Get row count with proper quoting for table name
        row_count <- dbGetQuery(values$conn, paste0("SELECT COUNT(*) AS count FROM ", 
                                                    quoted_table_name, ";"))$count
        
        values$status <- paste0("Successfully imported ", row_count, 
                                " rows from ", file_type_desc, " into table '", table_name, "'.")
        
        # Add to query history
        values$query_history <- rbind(data.frame(
          Timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
          Query = paste0("IMPORT ", file_type_desc, " as ", quoted_table_name),
          Status = "Success",
          stringsAsFactors = FALSE
        ), values$query_history)
        
        # Explicitly refresh the database explorer
        refreshDatabaseExplorer()
        
        # Switch to the database info tab to show the new table
        updateTabsetPanel(session, "main_tabs", selected = "Database Info")
        
        # Generate a query to show the data and put it in the SQL query box
        updateTextAreaInput(session, "sql_query", 
                            value = paste0("SELECT * FROM ", quoted_table_name, " LIMIT 100;"))
      } else {
        values$status <- paste0("Error: Table '", table_name, "' was not created. Check the file format.")
      }
      
    }, error = function(e) {
      values$status <- paste0("Error importing file: ", e$message)
      print(paste("Import error:", e$message))
    })
  })
  
  # Clear query history
  observeEvent(input$clear_history_btn, {
    values$query_history <- data.frame(
      Timestamp = character(),
      Query = character(),
      Status = character(),
      stringsAsFactors = FALSE
    )
    values$status <- "Query history cleared."
  })
  
  # Handle selection in query history
  observeEvent(input$query_history_table_rows_selected, {
    values$selected_history_row <- input$query_history_table_rows_selected
  })
  
  # Rerun selected query from history
  observeEvent(input$rerun_query_btn, {
    if (!is.null(values$selected_history_row) && length(values$selected_history_row) > 0) {
      selected_query <- values$query_history$Query[values$selected_history_row]
      updateTextAreaInput(session, "sql_query", value = selected_query)
    } else {
      values$status <- "Please select a query from the history first."
    }
  })
  
  # Output connection status
  output$connection_status <- renderText({
    values$status
  })
  
  # Output query status
  output$query_status <- renderText({
    values$status
  })
  
  # Output query results
  output$results_table <- renderDT({
    if (!is.null(values$query_result)) {
      page_length <- as.numeric(input$results_page_length)
      
      datatable(values$query_result, options = list(
        pageLength = page_length,
        scrollX = TRUE,
        autoWidth = TRUE,
        searchHighlight = TRUE,
        dom = 'Bfrtip',
        buttons = c('copy', 'csv', 'excel', 'pdf')
      ), filter = 'top', rownames = FALSE)
    }
  })
  
  # Output database info
  output$db_info <- renderText({
    values$db_info
  })
  
  # Output query history
  output$query_history_table <- renderDT({
    if (!is.null(values$query_history) && nrow(values$query_history) > 0) {
      datatable(values$query_history, options = list(
        pageLength = 10,
        scrollX = TRUE,
        order = list(list(0, 'desc')),
        columnDefs = list(
          list(targets = 1, width = '50%')
        )
      ), selection = 'single', rownames = FALSE)
    }
  })
  
  # Download handler for results
  output$download_results <- downloadHandler(
    filename = function() {
      paste("query-results-", format(Sys.time(), "%Y%m%d-%H%M%S"), ".csv", sep = "")
    },
    content = function(file) {
      if (!is.null(values$query_result) && nrow(values$query_result) > 0) {
        write.csv(values$query_result, file, row.names = FALSE)
      } else {
        write.csv(data.frame(message = "No results available"), file, row.names = FALSE)
      }
    }
  )
  
  # Initialize UI state
  observe({
    if (is.null(values$conn)) {
      shinyjs::enable("connect_btn")
      shinyjs::disable("disconnect_btn")
    } else {
      shinyjs::disable("connect_btn")
      shinyjs::enable("disconnect_btn")
    }
  })
  
  # Clean up on session end
  session$onSessionEnded(function() {
    if (!is.null(values$conn)) {
      dbDisconnect(values$conn, shutdown = TRUE)
    }
  })
}

# Run the application
shinyApp(ui = ui, server = server)
