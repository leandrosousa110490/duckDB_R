# Load required libraries
library(shiny)
library(shinydashboard)
library(data.table)
library(DT)
library(readxl)
library(tools)
library(esquisse)
library(dplyr)
library(DBI)
library(odbc)
library(RMySQL)
library(RPostgres)
library(RSQLite)
library(duckdb)  # Add DuckDB library
library(future)
library(promises)
library(memoise)
library(pryr)
library(bit64)
library(arrow)
library(dtplyr)
library(prophet)  # Add prophet library
library(plotly)   # Add plotly library
library(rpivotTable) # Add rpivotTable library
library(shinyjs)  # Add shinyjs library for runjs function
library(later)  # Add later library for tab operations

# Configure parallel processing
future::plan(multicore)
options(future.globals.maxSize = 8000 * 1024^2)  # 8GB limit for future
options(datatable.print.class = TRUE)
options(datatable.optimize = TRUE)

# Helper functions for large data handling
chunk_read_csv <- function(file_path, chunk_size = 100000) {
  con <- file(file_path, "r")
  header <- read.csv(con, nrows = 1, header = TRUE)
  
  chunks <- list()
  while (TRUE) {
    chunk <- tryCatch(
      read.csv(con, nrows = chunk_size, header = FALSE, col.names = names(header)),
      error = function(e) NULL
    )
    if (is.null(chunk) || nrow(chunk) == 0) break
    chunks[[length(chunks) + 1]] <- as.data.table(chunk)
    gc()  # Force garbage collection after each chunk
  }
  close(con)
  rbindlist(chunks)
}

sample_large_dataset <- function(dt, n = 1000) {
  if (nrow(dt) <= n) return(dt)
  dt[sample(nrow(dt), n)]
}

get_memory_usage <- function() {
  mem <- pryr::mem_used()
  paste0("Memory Usage: ", round(mem/1024/1024, 2), " MB")
}

# Cache function for database queries
cached_query <- memoise(function(conn, query) {
  dbGetQuery(conn, query)
})

# Add safe Excel reading function
safe_read_excel <- function(file_path) {
  tryCatch({
    # Read with minimal options
    data <- read_excel(
      file_path,
      sheet = 1,
      col_names = TRUE,
      na = ""
    )
    
    # Convert to data.table if successful
    if(!is.null(data) && nrow(data) > 0 && ncol(data) > 0) {
      return(as.data.table(data))
    }
    return(NULL)
  }, error = function(e) {
    return(NULL)
  })
}

# Compress large datasets
compress_dataset <- function(dt) {
  for (col in names(dt)) {
    if (is.character(dt[[col]])) {
      dt[, (col) := as.factor(get(col))]
    } else if (is.numeric(dt[[col]])) {
      if (all(floor(dt[[col]]) == dt[[col]], na.rm = TRUE)) {
        if (max(dt[[col]], na.rm = TRUE) <= .Machine$integer.max) {
          dt[, (col) := as.integer(get(col))]
        }
      }
    }
  }
  gc()
  return(dt)
}

# Batch processing for data operations
batch_process <- function(dt, fn, batch_size = 50000) {
  total_rows <- nrow(dt)
  batches <- ceiling(total_rows / batch_size)
  result <- vector("list", batches)
  
  withProgress(message = 'Processing data', value = 0, {
    for(i in 1:batches) {
      start_idx <- (i-1) * batch_size + 1
      end_idx <- min(i * batch_size, total_rows)
      batch <- dt[start_idx:end_idx]
      result[[i]] <- fn(batch)
      incProgress(i/batches)
      gc()
    }
  })
  
  rbindlist(result, fill = TRUE)
}

# Optimized data loading function
optimized_read_file <- function(file_path) {
  ext <- tolower(file_ext(file_path))
  withProgress(message = 'Reading file', value = 0, {
    tryCatch({
      if (ext == "csv") {
        # Use Arrow for CSV files
        incProgress(0.3, detail = "Loading data with Arrow...")
        data <- arrow::read_csv_arrow(file_path) %>%
          as.data.table()
        incProgress(0.7)
        return(data)
      } else if (ext %in% c("xls", "xlsx")) {
        # Use readxl for Excel files
        incProgress(0.3, detail = "Loading data with readxl...")
        data <- read_excel(file_path) %>%
          as.data.table()
        incProgress(0.7)
        return(data)
      } else {
        showNotification(paste("Unsupported file type:", ext), type = "error")
        return(NULL)
      }
    }, error = function(e) {
      showNotification(paste("Error reading file:", e$message), type = "error")
      return(NULL)
    })
  })
}

# Memory-efficient data manipulation
safe_merge <- function(dt1, dt2, by.x, by.y, type = "inner") {
  gc()  # Force garbage collection before merge
  result <- tryCatch({
    merge(dt1, dt2, by.x = by.x, by.y = by.y, 
          all.x = type %in% c("left", "full"),
          all.y = type %in% c("right", "full"))
  }, error = function(e) {
    showNotification("Memory limit reached during merge. Try reducing data size.", type = "error")
    return(NULL)
  })
  gc()  # Force garbage collection after merge
  result
}

# UI Definition
ui <- dashboardPage(
  dashboardHeader(
    title = "Advanced Data Dashboard",
    tags$li(class = "dropdown",
            tags$a(id = "memory_usage",
                   style = "padding: 15px; color: white;"))
  ),
  dashboardSidebar(
    sidebarMenu(
      menuItem("Data Sources", tabName = "sources", icon = icon("database"),
               menuSubItem("File Upload", tabName = "file", icon = icon("file")),
               menuSubItem("Database", tabName = "db", icon = icon("server"))
      ),
      menuItem("Data Management", tabName = "data", icon = icon("table")),
      menuItem("Data Manipulation", tabName = "manipulation", icon = icon("tools")),
      menuItem("Visualization", tabName = "viz", icon = icon("chart-bar")),
      menuItem("Pivot Table", tabName = "pivot", icon = icon("table")), # Add Pivot Table menu item
      menuItem("Forecasting", tabName = "forecasting", icon = icon("chart-line"))  # Add Forecasting menu item
    )
  ),
  dashboardBody(
    shinyjs::useShinyjs(),  # Enable shinyjs functionality
    # Add JavaScript for programmatically clicking buttons
    tags$head(
      tags$script(HTML('
        Shiny.addCustomMessageHandler("shiny-eval-code", function(data) {
          eval(data.code);
        });
      '))
    ),
    tabItems(
      # Add this new Database Tab
      tabItem(tabName = "db",
              fluidRow(
                div(
                  class = "alert alert-info",
                  style = "margin: 0 15px 15px 15px;",
                  HTML("<strong>SQL Query System</strong><br>
                       <i class='fa fa-info-circle'></i> <b>Important:</b> Only query results from currently <b>open tabs</b> are available for visualization and data manipulation.<br>
                       If you close a query tab, its results will remain in Data Management but won't appear in visualization options.<br>
                       To make an inactive query result active again, go to Data Management and click 'Load in New Query Tab'.")
                )
              ),
              fluidRow(
                box(width = 12,
                    title = "Database Connection",
                    status = "primary",
                    solidHeader = TRUE,
                    selectInput("db_type", "Database Type",
                                choices = c("MySQL", "PostgreSQL", "SQLite", 
                                            "SQL Server", "Azure SQL", "DuckDB", "Other (ODBC)")),
                    conditionalPanel(
                      condition = "input.db_type === 'DuckDB'",
                      fileInput("duckdb_file", "Select DuckDB Database File or Create New",
                                accept = c(".duckdb", ".db"),
                                placeholder = "Select file or leave empty for in-memory database")
                    ),
                    conditionalPanel(
                      condition = "input.db_type !== 'SQLite' && input.db_type !== 'DuckDB'",
                      textInput("db_host", "Host", value = "localhost"),
                      numericInput("db_port", "Port", value = 3306),
                      textInput("db_user", "Username"),
                      passwordInput("db_pass", "Password")
                    ),
                    conditionalPanel(
                      condition = "input.db_type === 'SQLite'",
                      fileInput("sqlite_file", "Select SQLite Database File",
                                accept = c(".sqlite", ".db", ".sqlite3"))
                    ),
                    conditionalPanel(
                      condition = "input.db_type !== 'SQLite' && input.db_type !== 'DuckDB'",
                      textInput("db_name", "Database Name")
                    ),
                    conditionalPanel(
                      condition = "input.db_type === 'SQL Server' || input.db_type === 'Azure SQL'",
                      textInput("db_driver", "ODBC Driver", 
                                value = "ODBC Driver 17 for SQL Server")
                    ),
                    conditionalPanel(
                      condition = "input.db_type === 'Other (ODBC)'",
                      textInput("db_dsn", "Data Source Name (DSN)"),
                      textInput("db_driver", "Custom Driver Name")
                    ),
                    actionButton("db_connect", "Connect", 
                                 icon = icon("plug"), 
                                 class = "btn-primary"),
                    tags$hr(),
                    uiOutput("db_table_ui")
                )
              ),
              fluidRow(
                box(width = 12,
                    title = "SQL Query Execution",
                    status = "primary",
                    solidHeader = TRUE,
                    conditionalPanel(
                      condition = "output.is_db_connected === true",
                      tags$div(
                        style = "margin-bottom: 15px;",
                        div(
                          style = "margin-bottom: 10px;",
                          actionButton("add_query_tab", "New Query", icon = icon("plus"), class = "btn-primary")
                        ),
                        uiOutput("query_tabs")
                      )
                    ),
                    conditionalPanel(
                      condition = "output.is_db_connected !== true",
                      tags$div(class = "alert alert-warning", 
                               "Please connect to a database first to run SQL queries.")
                    )
                )
              )
      ),
      # File Upload Tab
      tabItem(tabName = "file",
              fluidRow(
                box(width = 12,
                    fileInput("files", "Upload Data Files", 
                              multiple = TRUE,
                              accept = c(".csv", ".xlsx", ".xls")),
                    fileInput("folder", "Select Folder Containing Data Files", 
                              multiple = TRUE,
                              accept = c(".csv", ".xlsx", ".xls")),
                    numericInput("page_size", "Rows per page:", 
                                 value = 500, min = 100, max = 1000)
                )
              )
      ),
      
      # Data Management Tab
      tabItem(tabName = "data",
              fluidRow(
                box(width = 12,
                    div(
                      class = "alert alert-info",
                      HTML("<strong>Data Management</strong><br>
                          <i class='fa fa-database' style='color: #3c8dbc;'></i> Active Query Results: Available for visualization and data manipulation<br>
                          <i class='fa fa-file' style='color: gray;'></i> File Uploads: Available for data management only")
                    ),
                    uiOutput("table_tabs")
                )
              )
      ),
      
      # Data Manipulation Tab
      tabItem(tabName = "manipulation",
              fluidRow(
                div(
                  class = "alert alert-info",
                  style = "margin: 0 15px 15px 15px;",
                  HTML("<strong>Data Manipulation</strong><br>
                      Only data from <b>active query tabs</b> is available for manipulation. 
                      Inactive query results or file uploads are not shown.<br>
                      <i class='fa fa-info-circle'></i> To make a query active, it must be in an open query tab.")
                ),
                box(width = 6,
                    selectInput("manipulation_type", "Select Operation",
                                choices = c("Merge Tables" = "merge",
                                            "Append Tables" = "append",
                                            "Summarize Data" = "summarize",
                                            "Inspect Data Types" = "inspect",
                                            "Remove Columns" = "remove",
                                            "Remove Duplicates" = "remove_duplicates",
                                            "Keep Only Duplicates" = "keep_duplicates",
                                            "Replace Value" = "replace_value",
                                            "Add Conditional Column" = "conditional")),  # Add this line
                    # Merge Panel
                    conditionalPanel(
                      condition = "input.manipulation_type == 'merge'",
                      selectInput("table1", "Select First Table", choices = NULL),
                      selectInput("table2", "Select Second Table", choices = NULL),
                      selectInput("merge_type", "Merge Type",
                                  choices = c("Inner Join", 
                                              "Left Join",
                                              "Right Join",
                                              "Full Join",
                                              "Left Anti Join",
                                              "Right Anti Join")),
                      # Add column selection for both tables
                      selectizeInput("merge_by_table1", "Select Columns from First Table", 
                                     choices = NULL, multiple = TRUE),
                      selectizeInput("merge_by_table2", "Select Columns from Second Table", 
                                     choices = NULL, multiple = TRUE),
                      actionButton("do_merge", "Merge Tables", 
                                   class = "btn-primary")
                    ),
                    # Append Panel (unchanged)
                    conditionalPanel(
                      condition = "input.manipulation_type == 'append'",
                      selectizeInput("tables_to_append", "Select Tables to Append",
                                     choices = NULL, multiple = TRUE),
                      actionButton("do_append", "Append Tables")
                    ),
                    # Enhanced Summarize Panel
                    conditionalPanel(
                      condition = "input.manipulation_type == 'summarize'",
                      selectInput("sum_table", "Select Table", choices = NULL),
                      selectizeInput("sum_group", "Group By Columns", 
                                     choices = NULL, multiple = TRUE),
                      selectizeInput("sum_cols", "Select Columns to Summarize",
                                     choices = NULL, multiple = TRUE),
                      selectizeInput("sum_functions", "Select Summary Functions",
                                     choices = c("Mean" = "mean", 
                                                 "Sum" = "sum", 
                                                 "Count" = "length",
                                                 "Min" = "min",
                                                 "Max" = "max",
                                                 "Median" = "median",
                                                 "Standard Deviation" = "sd"),
                                     multiple = TRUE,
                                     selected = c("mean", "sum", "length")),
                      actionButton("do_summarize", "Create Summary")
                    ),
                    # Add new Inspect Data Types Panel
                    conditionalPanel(
                      condition = "input.manipulation_type == 'inspect'",
                      selectInput("inspect_table", "Select Table", choices = NULL),
                      DTOutput("column_types_table")
                    ),
                    # Add the Remove Columns Panel after the other panels
                    conditionalPanel(
                      condition = "input.manipulation_type == 'remove'",
                      selectInput("remove_table", "Select Table", choices = NULL),
                      selectizeInput("columns_to_remove", "Select Columns to Remove",
                                     choices = NULL, multiple = TRUE),
                      actionButton("do_remove", "Remove Columns",
                                   class = "btn-warning")
                    ),
                    # Add Remove Duplicates Panel
                    conditionalPanel(
                      condition = "input.manipulation_type == 'remove_duplicates'",
                      selectInput("remove_duplicates_table", "Select Table", choices = NULL),
                      selectizeInput("remove_duplicates_columns", "Select Columns to Check for Duplicates",
                                     choices = NULL, multiple = TRUE),
                      actionButton("do_remove_duplicates", "Remove Duplicates", class = "btn-warning")
                    ),
                    # Add Keep Only Duplicates Panel
                    conditionalPanel(
                      condition = "input.manipulation_type == 'keep_duplicates'",
                      selectInput("keep_duplicates_table", "Select Table", choices = NULL),
                      selectizeInput("keep_duplicates_columns", "Select Columns to Check for Duplicates",
                                     choices = NULL, multiple = TRUE),
                      actionButton("do_keep_duplicates", "Keep Only Duplicates", class = "btn-warning")
                    ),
                    # Add Replace Value Panel
                    conditionalPanel(
                      condition = "input.manipulation_type == 'replace_value'",
                      selectInput("replace_value_table", "Select Table", choices = NULL),
                      selectizeInput("replace_value_columns", "Select Columns to Replace Values",
                                     choices = NULL, multiple = TRUE),
                      textInput("old_value", "Old Value"),
                      textInput("new_value", "New Value"),
                      actionButton("do_replace_value", "Replace Value", class = "btn-warning")
                    ),
                    # Update Conditional Column Panel with multiple conditions
                    conditionalPanel(
                      condition = "input.manipulation_type == 'conditional'",
                      selectInput("conditional_table", "Select Table", choices = NULL),
                      textInput("new_column_name", "New Column Name"),
                      
                      # First condition
                      tags$div(
                        style = "border: 1px solid #ddd; padding: 10px; margin: 5px;",
                        selectInput("condition_column_1", "Column to Check", choices = NULL),
                        selectInput("condition_operator_1", "Operator", 
                                    choices = c("equals" = "==",
                                                "not equals" = "!=",
                                                "greater than" = ">",
                                                "less than" = "<",
                                                "greater or equal" = ">=",
                                                "less or equal" = "<=",
                                                "contains" = "contains",
                                                "starts with" = "startswith",
                                                "ends with" = "endswith")),
                        textInput("condition_value_1", "Value to Compare")
                      ),
                      
                      # Logic selector for second condition
                      selectInput("condition_logic", "Add Second Condition?", 
                                  choices = c("None", "AND", "OR")),
                      
                      # Second condition (shown conditionally)
                      conditionalPanel(
                        condition = "input.condition_logic !== 'None'",
                        tags$div(
                          style = "border: 1px solid #ddd; padding: 10px; margin: 5px;",
                          selectInput("condition_column_2", "Second Column to Check", choices = NULL),
                          selectInput("condition_operator_2", "Second Operator", 
                                      choices = c("equals" = "==",
                                                  "not equals" = "!=",
                                                  "greater than" = ">",
                                                  "less than" = "<",
                                                  "greater or equal" = ">=",
                                                  "less or equal" = "<=",
                                                  "contains" = "contains",
                                                  "starts with" = "startswith",
                                                  "ends with" = "endswith")),
                          textInput("condition_value_2", "Second Value to Compare")
                        )
                      ),
                      
                      textInput("true_value", "Value if True"),
                      textInput("false_value", "Value if False"),
                      actionButton("do_conditional", "Add Conditional Column", class = "btn-primary")
                    )
                ),
                box(width = 6,
                    title = "Operation Preview",
                    status = "info",
                    solidHeader = TRUE,
                    DTOutput("manipulation_preview")
                )
              )
      ),
      
      # Visualization Tab
      tabItem(tabName = "viz",
              fluidRow(
                div(
                  class = "alert alert-info",
                  style = "margin: 0 15px 15px 15px;",
                  HTML("<strong>Data Visualization</strong><br>
                      Only data from <b>active query tabs</b> is available for visualization. 
                      Inactive query results or file uploads are not shown.<br>
                      <i class='fa fa-info-circle'></i> To make a query active, it must be in an open query tab.")
                )
              ),
              fluidRow(
                box(width = 12,
                    selectInput("viz_table", "Select Table for Visualization", 
                                choices = NULL),
                    esquisse::esquisse_ui(
                      id = "esquisse"
                    )
                )
              )
      ),
      tabItem(tabName = "pivot",
              fluidRow(
                div(
                  class = "alert alert-info",
                  style = "margin: 0 15px 15px 15px;",
                  HTML("<strong>Pivot Table Analysis</strong><br>
                      Only data from <b>active query tabs</b> is available for pivot analysis. 
                      Inactive query results or file uploads are not shown.<br>
                      <i class='fa fa-info-circle'></i> To make a query active, it must be in an open query tab.")
                )
              ),
              fluidRow(
                box(width = 12,
                    title = "Pivot Table Analysis",
                    status = "primary",
                    solidHeader = TRUE,
                    selectInput("pivot_table", "Select Table for Pivot Analysis", 
                                choices = NULL),
                    actionButton("create_pivot", "Create Pivot Table", 
                                 class = "btn-primary")
                )
              ),
              fluidRow(
                box(width = 12,
                    title = "Interactive Pivot Table",
                    status = "info",
                    solidHeader = TRUE,
                    height = "auto",
                    style = "overflow: visible; min-height: 600px;",
                    div(
                      style = "overflow: visible; width: 100%;",
                      rpivotTableOutput("pivot_output", height = "auto")
                    )
                )
              )
      ),
      tabItem(tabName = "forecasting",
              fluidRow(
                div(
                  class = "alert alert-info",
                  style = "margin: 0 15px 15px 15px;",
                  HTML("<strong>Forecasting Analysis</strong><br>
                      Only data from <b>active query tabs</b> is available for forecasting. 
                      Inactive query results or file uploads are not shown.<br>
                      <i class='fa fa-info-circle'></i> To make a query active, it must be in an open query tab.")
                )
              ),
              fluidRow(
                box(width = 12,
                    title = "Forecasting with Prophet",
                    status = "primary",
                    solidHeader = TRUE,
                    selectInput("forecast_table", "Select Table", choices = NULL),
                    selectInput("date_column", "Select Date Column", choices = NULL),
                    selectInput("value_column", "Select Value Column", choices = NULL),
                    numericInput("forecast_period", "Forecast Period (days)", value = 30, min = 1),
                    actionButton("run_forecast", "Run Forecast", class = "btn-primary")
                )
              ),
              fluidRow(
                box(width = 12,
                    title = "Forecast Results",
                    status = "info",
                    solidHeader = TRUE,
                    div(style = "position: relative;",
                        plotlyOutput("forecast_plot")
                    ),
                    div(style = "margin-top: 20px;",
                        DTOutput("forecast_table")
                    )
                )
              )
      )
    )
  )
)

# Server Logic
server <- function(input, output, session) {
  # Create server environment to store observers
  server_env <- environment()
  
  # Initialize reactive values
  datasets <- reactiveVal(list())  # Change from reactiveValues to reactiveVal
  db_conn <- reactiveVal(NULL)
  
  # Add reactive value for managing query tabs
  query_tabs <- reactiveVal(list())
  active_tab_id <- reactiveVal(NULL)
  next_tab_id <- reactiveVal(1)
  
  # Track active queries
  active_query_datasets <- reactiveVal(list())
  
  # Function to get datasets filtered by source
  get_filtered_datasets <- function(source = NULL) {
    all_datasets <- datasets()
    active_queries <- active_query_datasets()
    
    if (is.null(source)) {
      return(all_datasets)
    } else if (source == "query") {
      # Return only datasets from active query tabs
      # Get the list of currently open tabs
      current_tabs <- query_tabs()
      active_tab_result_names <- sapply(current_tabs, function(tab) tab$result_name)
      active_tab_result_names <- active_tab_result_names[!sapply(active_tab_result_names, is.null)]
      
      # Only return datasets associated with currently open tabs
      return(all_datasets[names(all_datasets) %in% active_tab_result_names])
    } else if (source == "file") {
      # Return only datasets from file uploads
      current_tabs <- query_tabs()
      active_tab_result_names <- sapply(current_tabs, function(tab) tab$result_name)
      active_tab_result_names <- active_tab_result_names[!sapply(active_tab_result_names, is.null)]
      
      return(all_datasets[!names(all_datasets) %in% names(active_queries) | 
                            !names(all_datasets) %in% active_tab_result_names])
    }
    
    return(all_datasets)
  }
  
  # Add a reactive for current data preview
  selected_data <- reactive({
    req(input$manipulation_type)
    
    if (input$manipulation_type == "merge") {
      req(input$table1, input$table2)
      list(
        table1 = datasets()[[input$table1]],
        table2 = datasets()[[input$table2]]
      )
    } else if (input$manipulation_type %in% c("summarize", "inspect", "remove")) {
      req(input$sum_table %||% input$inspect_table %||% input$remove_table)
      datasets()[[input$sum_table %||% input$inspect_table %||% input$remove_table]]
    }
  })
  
  # Add preview output
  output$manipulation_preview <- renderDT({
    req(selected_data())
    if (input$manipulation_type == "merge") {
      head(selected_data()$table1, 5)
    } else {
      head(selected_data(), 5)
    }
  })
  
  # Function to read files based on extension with optimizations
  read_file <- optimized_read_file
  
  
  # Handle file uploads with compression
  observeEvent(input$files, {
    req(input$files)
    withProgress(message = 'Processing files', value = 0, {
      current_data <- datasets()
      success_notifications <- list()
      error_notifications <- list()
      
      for(i in seq_along(input$files$name)) {
        incProgress(i/length(input$files$name), 
                    detail = paste("Processing", input$files$name[i]))
        
        base_name <- tools::file_path_sans_ext(input$files$name[i])
        ext <- tolower(file_ext(input$files$name[i]))
        
        if (ext %in% c("xls", "xlsx")) {
          # Get all available sheets from the Excel file
          sheets <- excel_sheets(input$files$datapath[i])
          showModal(modalDialog(
            title = paste("Select Sheet for", input$files$name[i]),
            selectInput(paste0("sheet_select_", i), "Select Sheet Name:", choices = sheets),
            footer = tagList(
              modalButton("Cancel"),
              actionButton(paste0("ok_sheet_", i), "OK")
            )
          ))
          
          # Create a new environment-persistent observer for this sheet selection
          sheet_observer_name <- paste0("sheet_observer_", i)
          if (!exists(sheet_observer_name, envir = server_env)) {
            sheet_observer <- observeEvent(input[[paste0("ok_sheet_", i)]], {
              removeModal()
              sheet_name <- input[[paste0("sheet_select_", i)]]
              new_data <- tryCatch({
                as.data.table(read_excel(input$files$datapath[i], sheet = sheet_name))
              }, error = function(e) {
                error_notifications <<- c(error_notifications, paste("Error reading sheet from", input$files$name[i]))
                NULL
              })
              
              if (!is.null(new_data)) {
                # Compress dataset
                new_data <- compress_dataset(new_data)
                
                # Handle duplicate names
                local_base_name <- base_name
                if (local_base_name %in% names(current_data)) {
                  counter <- 1
                  while(paste0(local_base_name, "_", counter) %in% names(current_data)) {
                    counter <- counter + 1
                  }
                  local_base_name <- paste0(local_base_name, "_", counter)
                }
                current_data[[local_base_name]] <- new_data
                datasets(current_data)
                updateAllSelectInputs(session)
                showNotification(paste("Loaded", local_base_name), type = "message")
              }
            }, once = TRUE)
            
            assign(sheet_observer_name, sheet_observer, envir = server_env)
          }
        } else {
          new_data <- optimized_read_file(input$files$datapath[i])
          
          if (!is.null(new_data)) {
            # Compress dataset
            new_data <- compress_dataset(new_data)
            
            # Handle duplicate names
            if (base_name %in% names(current_data)) {
              counter <- 1
              while(paste0(base_name, "_", counter) %in% names(current_data)) {
                counter <- counter + 1
              }
              base_name <- paste0(base_name, "_", counter)
            }
            current_data[[base_name]] <- new_data
            success_notifications <- c(success_notifications, paste("Loaded", base_name))
          } else {
            error_notifications <- c(error_notifications, paste("Failed to load data from", input$files$name[i]))
          }
        }
      }
      
      # Only update the datasets once with all the non-Excel files
      datasets(current_data)
      updateAllSelectInputs(session)
      
      # Show a single consolidated notification for successful loads
      if (length(success_notifications) > 0) {
        showNotification(
          HTML(paste(success_notifications, collapse = "<br>")), 
          type = "message"
        )
      }
      
      # Show a single consolidated notification for errors
      if (length(error_notifications) > 0) {
        showNotification(
          HTML(paste(error_notifications, collapse = "<br>")), 
          type = "error"
        )
      }
    })
  })
  
  # Function to update all select inputs
  updateAllSelectInputs <- function(session) {
    # All datasets choices for data management
    all_choices <- names(datasets())
    
    # Only query datasets for visualization and manipulation
    query_choices <- names(get_filtered_datasets("query"))
    
    # File upload choices
    file_choices <- names(get_filtered_datasets("file"))
    
    # Update inputs for data management with all datasets
    updateSelectInput(session, "table1", choices = all_choices)
    updateSelectInput(session, "table2", choices = all_choices)
    updateSelectizeInput(session, "tables_to_append", choices = all_choices)
    
    # Update visualization inputs with only query datasets
    updateSelectInput(session, "viz_table", choices = query_choices)
    updateSelectInput(session, "pivot_table", choices = query_choices)
    updateSelectInput(session, "forecast_table", choices = query_choices)
    
    # Update data manipulation inputs with only query datasets
    updateSelectInput(session, "sum_table", choices = query_choices)
    updateSelectInput(session, "inspect_table", choices = query_choices)
    updateSelectInput(session, "remove_table", choices = query_choices)
    updateSelectInput(session, "remove_duplicates_table", choices = query_choices)
    updateSelectInput(session, "keep_duplicates_table", choices = query_choices)
    updateSelectInput(session, "replace_value_table", choices = query_choices)
    updateSelectInput(session, "conditional_table", choices = query_choices)
  }
  
  # Update merge columns when tables are selected
  observeEvent(c(input$table1, input$table2), {
    req(input$table1, input$table2)
    df1 <- datasets()[[input$table1]]
    df2 <- datasets()[[input$table2]]
    
    if (!is.null(df1) && !is.null(df2)) {
      updateSelectizeInput(session, "merge_by_table1", 
                           choices = names(df1))
      updateSelectizeInput(session, "merge_by_table2", 
                           choices = names(df2))
    }
  })
  
  # Enhanced merge operation
  observeEvent(input$do_merge, {
    req(input$table1, input$table2, input$merge_type, 
        input$merge_by_table1, input$merge_by_table2)
    
    # Validate same number of columns selected
    if (length(input$merge_by_table1) != length(input$merge_by_table2)) {
      showNotification("Please select the same number of columns from both tables", 
                       type = "error")
      return()
    }
    
    df1 <- datasets()[[input$table1]]
    df2 <- datasets()[[input$table2]]
    
    # Create named vector for merge columns
    by_cols <- setNames(input$merge_by_table1, input$merge_by_table2)
    
    tryCatch({
      merged_data <- switch(input$merge_type,
                            "Inner Join" = merge(df1, df2, by.x = input$merge_by_table1, 
                                                 by.y = input$merge_by_table2),
                            "Left Join" = merge(df1, df2, by.x = input$merge_by_table1, 
                                                by.y = input$merge_by_table2, all.x = TRUE),
                            "Right Join" = merge(df1, df2, by.x = input$merge_by_table1, 
                                                 by.y = input$merge_by_table2, all.y = TRUE),
                            "Full Join" = merge(df1, df2, by.x = input$merge_by_table1, 
                                                by.y = input$merge_by_table2, all = TRUE),
                            "Left Anti Join" = df1[!do.call(paste0, df1[input$merge_by_table1]) %in% 
                                                     do.call(paste0, df2[input$merge_by_table2]), ],
                            "Right Anti Join" = df2[!do.call(paste0, df2[input$merge_by_table2]) %in% 
                                                      do.call(paste0, df1[input$merge_by_table1]), ]
      )
      
      # Update datasets
      current_data <- datasets()
      current_data[[input$table1]] <- merged_data
      datasets(current_data)
      updateAllSelectInputs(session)
      showNotification("Merged data successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Merge failed:", e$message), type = "error")
    })
  })
  
  # Update summarize columns when table is selected
  observeEvent(input$sum_table, {
    req(input$sum_table)
    data <- datasets()[[input$sum_table]]
    if (!is.null(data)) {
      # Get numeric columns for summarization
      numeric_cols <- names(data)[sapply(data, is.numeric)]
      updateSelectizeInput(session, "sum_cols", 
                           choices = numeric_cols)
      # All columns can be used for grouping
      updateSelectizeInput(session, "sum_group", 
                           choices = names(data))
    }
  })
  
  # Add reactive value to store folder path
  selected_folder <- reactiveVal(NULL)
  
  # Handle folder loading
  observeEvent(input$folder, {
    req(input$folder)
    withProgress(message = 'Loading folder contents', value = 0, {
      files <- input$folder$datapath
      
      # Check if we have only Excel files
      if(length(files) > 0 && all(tolower(tools::file_ext(files)) %in% c("xls", "xlsx"))) {
        # Get all available sheets from first file
        sheets <- excel_sheets(files[1])
        showModal(modalDialog(
          title = "Select Sheet to Combine",
          selectInput("combine_sheet", "Select Sheet Name:", choices = sheets),
          footer = tagList(
            modalButton("Cancel"),
            actionButton("ok_combine_sheet", "OK")
          )
        ))
      } else {
        # Original folder loading logic for mixed file types
        processFiles(files)
      }
    })
  })
  
  # Add new function to process files
  processFiles <- function(files, sheet_name = NULL) {
    all_data <- list()
    success_notifications <- list()
    error_notifications <- list()
    
    for(i in seq_along(files)) {
      incProgress(i/length(files), 
                  detail = paste("Processing", basename(files[i])))
      
      ext <- tolower(tools::file_ext(files[i]))
      
      if(ext == "csv") {
        data <- optimized_read_file(files[i])
      } else if(ext %in% c("xls", "xlsx")) {
        if(!is.null(sheet_name)) {
          # Use specified sheet name for all Excel files
          data <- tryCatch({
            as.data.table(read_excel(files[i], sheet = sheet_name))
          }, error = function(e) {
            error_notifications <<- c(error_notifications, 
                                      paste("Error reading sheet from", basename(files[i])))
            NULL
          })
        } else {
          data <- safe_read_excel(files[i])
        }
      }
      
      if(!is.null(data) && nrow(data) > 0) {
        all_data[[length(all_data) + 1]] <- data
        success_notifications <- c(success_notifications, 
                                   paste("Loaded", basename(files[i])))
      } else if (is.null(data)) {
        error_notifications <- c(error_notifications, 
                                 paste("Failed to load", basename(files[i])))
      }
      gc()
    }
    
    if(length(all_data) > 0) {
      combined_data <- rbindlist(all_data, fill = TRUE, use.names = TRUE)
      combined_data <- compress_dataset(combined_data)
      
      current_data <- datasets()
      new_name <- "combined_data"
      if(new_name %in% names(current_data)) {
        counter <- 1
        while(paste0(new_name, "_", counter) %in% names(current_data)) {
          counter <- 1
        }
        new_name <- paste0(new_name, "_", counter)
      }
      current_data[[new_name]] <- combined_data
      datasets(current_data)
      updateAllSelectInputs(session)
      
      success_notifications <- c(success_notifications, 
                                 paste("Successfully combined", length(files), "files into", new_name))
    } else {
      error_notifications <- c(error_notifications, "No valid data found in files")
    }
    
    # Show consolidated success notifications
    if (length(success_notifications) > 0) {
      showNotification(
        HTML(paste(success_notifications, collapse = "<br>")), 
        type = "message"
      )
    }
    
    # Show consolidated error notifications
    if (length(error_notifications) > 0) {
      showNotification(
        HTML(paste(error_notifications, collapse = "<br>")), 
        type = "error"
      )
    }
  }
  
  # Add handler for sheet selection
  observeEvent(input$ok_combine_sheet, {
    req(input$combine_sheet, input$folder)
    removeModal()
    
    withProgress(message = 'Loading folder contents', value = 0, {
      files <- input$folder$datapath
      processFiles(files, input$combine_sheet)
    })
  })
  
  # Create table tabs in data management section
  output$table_tabs <- renderUI({
    tables <- names(datasets())
    active_queries <- active_query_datasets()
    
    # Get the currently active query result names from open tabs
    current_tabs <- query_tabs()
    active_tab_result_names <- sapply(current_tabs, function(tab) tab$result_name)
    active_tab_result_names <- active_tab_result_names[!sapply(active_tab_result_names, is.null)]
    
    if (length(tables) == 0) {
      return(tags$div(class = "alert alert-info", "No datasets loaded yet."))
    }
    
    # Create a tabset panel with tables
    do.call(tabsetPanel, c(
      id = "table_select",
      lapply(tables, function(table_name) {
        # Check if this is a query result and if it's active
        is_query <- table_name %in% names(active_queries)
        is_active_query <- table_name %in% active_tab_result_names
        
        # Create tab panel with appropriate icon and styling
        tabPanel(
          title = HTML(paste0(
            table_name,
            if(is_active_query) {
              paste0(" <i class='fa fa-database' style='color: #3c8dbc;' title='Active Query Result'></i>")
            } else if(is_query) {
              paste0(" <i class='fa fa-database' style='color: #999;' title='Inactive Query Result'></i>")
            } else {
              paste0(" <i class='fa fa-file' style='color: gray;' title='File Upload'></i>")
            }
          )),
          fluidRow(
            column(12,
                   if(is_query) {
                     # Show query info for query results
                     query_info <- active_queries[[table_name]]
                     div(
                       class = "well well-sm",
                       style = paste0("margin-bottom: 10px; border-left: 5px solid ", 
                                      if(is_active_query) "#3c8dbc" else "#999"),
                       if(is_active_query) {
                         div(class = "label label-primary", style = "margin-bottom: 5px;", "ACTIVE QUERY")
                       } else {
                         div(class = "label label-default", style = "margin-bottom: 5px;", "INACTIVE QUERY")
                       },
                       p(
                         strong("SQL Query: "), 
                         tags$code(query_info$query)
                       ),
                       p(
                         strong("Executed: "), 
                         format(query_info$timestamp, "%Y-%m-%d %H:%M:%S")
                       ),
                       if(!is_active_query) {
                         div(
                           p(class = "text-muted", "This query result is not from an active tab and is not available for visualization."),
                           actionButton(paste0("reactivate_query_", table_name), 
                                        "Load in New Query Tab", 
                                        icon = icon("sync"),
                                        class = "btn-info btn-sm")
                         )
                       }
                     )
                   } else {
                     # No query info for file uploads
                     NULL
                   },
                   DTOutput(paste0("table_", table_name)),
                   actionButton(paste0("remove_dataset_", table_name), 
                                "Remove Dataset", 
                                class = "btn-danger",
                                style = "margin-top: 10px;")
            )
          )
        )
      })
    ))
  })
  
  # Render data tables
  observe({
    all_datasets <- isolate(names(datasets()))
    for (name in all_datasets) {
      local({
        local_name <- name
        
        # Render the table
        output[[paste0("table_", local_name)]] <- renderDT({
          req(datasets()[[local_name]])
          data <- datasets()[[local_name]]
          
          # Create datatable
          datatable(
            data,
            options = list(
              pageLength = min(10, nrow(data)),
              scrollX = TRUE,
              scrollY = "400px",
              scroller = TRUE
            ),
            style = 'bootstrap',
            rownames = FALSE
          )
        })
        
        # Only create the event handlers once per dataset name
        # by using isolate to avoid re-creating them on each reactive change
        if (!exists(paste0("observer_remove_", local_name), envir = server_env)) {
          # Handle remove dataset button
          remove_observer <- observeEvent(input[[paste0("remove_dataset_", local_name)]], {
            # Confirm before removing
            showModal(modalDialog(
              title = "Confirm Removal",
              "Are you sure you want to remove this dataset?",
              footer = tagList(
                modalButton("Cancel"),
                actionButton(paste0("confirm_remove_", local_name), "Remove", class = "btn-danger")
              )
            ))
          }, ignoreInit = TRUE, once = FALSE)
          
          # Handle confirmation
          confirm_observer <- observeEvent(input[[paste0("confirm_remove_", local_name)]], {
            # Remove from datasets
            current_data <- datasets()
            current_data[[local_name]] <- NULL
            datasets(current_data)
            
            # If it was a query result, remove from active queries
            current_queries <- active_query_datasets()
            if (local_name %in% names(current_queries)) {
              current_queries[[local_name]] <- NULL
              active_query_datasets(current_queries)
            }
            
            removeModal()
            updateAllSelectInputs(session)
            showNotification(paste("Dataset", local_name, "removed"), type = "message")
          }, ignoreInit = TRUE, once = FALSE)
          
          # Store the observers in the server environment
          assign(paste0("observer_remove_", local_name), remove_observer, envir = server_env)
          assign(paste0("observer_confirm_", local_name), confirm_observer, envir = server_env)
        }
        
        # Only create the reactivate event handler once per dataset name
        if (!exists(paste0("observer_reactivate_", local_name), envir = server_env)) {
          # Handle reactivate query button (for inactive queries)
          reactivate_observer <- observeEvent(input[[paste0("reactivate_query_", local_name)]], {
            req(local_name %in% names(active_query_datasets()))
            
            # Get the query info
            query_info <- active_query_datasets()[[local_name]]
            query_text <- query_info$query
            
            # Create a new query tab
            add_query_tab()
            
            # Wait for the tab to be created
            later::later(function() {
              # Get the latest tab id
              latest_tab_id <- next_tab_id() - 1
              
              # Update the query textarea with the original query
              updateTextAreaInput(session, paste0("sql_query_", latest_tab_id), value = query_text)
              
              # Show notification
              showNotification(
                HTML(paste0("Query for <b>", local_name, "</b> loaded in new tab.<br>Click 'Execute Query' to run it.")),
                type = "message",
                duration = 5
              )
            }, 0.2)
          }, ignoreInit = TRUE, once = FALSE)
          
          # Store the observer in the server environment
          assign(paste0("observer_reactivate_", local_name), reactivate_observer, envir = server_env)
        }
      })
    }
  })
  
  # Handle append operation
  observeEvent(input$do_append, {
    req(input$tables_to_append)
    selected_data <- datasets()[input$tables_to_append]
    appended_data <- rbindlist(selected_data, fill = TRUE)
    
    current_data <- datasets()
    current_data[["appended_result"]] <- appended_data
    datasets(current_data)
  })
  
  # Enhanced summarize operation
  observeEvent(input$do_summarize, {
    req(input$sum_table, input$sum_group, input$sum_cols, input$sum_functions)
    data <- datasets()[[input$sum_table]]
    
    tryCatch({
      # Create summary functions list
      summary_functions <- lapply(input$sum_functions, function(fn) {
        switch(fn,
               "mean" = function(x) if(is.numeric(x)) mean(x, na.rm = TRUE) else NA_real_,
               "sum" = function(x) if(is.numeric(x)) sum(x, na.rm = TRUE) else NA_real_,
               "length" = length,
               "min" = function(x) if(is.numeric(x)) min(x, na.rm = TRUE) else NA_real_,
               "max" = function(x) if(is.numeric(x)) max(x, na.rm = TRUE) else NA_real_,
               "median" = function(x) if(is.numeric(x)) median(x, na.rm = TRUE) else NA_real_,
               "sd" = function(x) if(is.numeric(x)) sd(x, na.rm = TRUE) else NA_real_
        )
      })
      names(summary_functions) <- input$sum_functions
      
      # Perform summarization
      summary_data <- data %>%
        group_by(across(all_of(input$sum_group))) %>%
        summarise(across(all_of(input$sum_cols), 
                         summary_functions,
                         .names = "{.col}_{.fn}"),
                  .groups = "drop")
      
      # Update datasets
      current_data <- datasets()
      current_data[[input$sum_table]] <- as.data.table(summary_data)
      datasets(current_data)
      updateAllSelectInputs(session)
      showNotification("Summary data created successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Summarize failed:", e$message), 
                       type = "error")
    })
  })
  
  # Add memory monitoring
  observe({
    invalidateLater(5000)  # Update every 5 seconds
    shinyjs::html("memory_usage", get_memory_usage())
  })
  
  # Optimize visualization data handling
  observeEvent(input$viz_table, {
    req(input$viz_table)
    data <- datasets()[[input$viz_table]]
    
    # Sample data for visualization if too large
    if(nrow(data) > 10000) {
      data <- sample_large_dataset(data, n = 10000)
      showNotification(
        "Dataset sampled to 10,000 rows for visualization", 
        type = "warning"
      )
    }
    
    esquisse::esquisse_server(
      id = "esquisse",
      data = data
    )
  })
  
  # Add Excel sheet handling to server
  observeEvent(input$ok_sheet, {
    req(input$files, input$sheet_select)
    file_path <- input$files$datapath[1]  # Get the current Excel file path
    
    tryCatch({
      data <- as.data.table(read_excel(file_path, 
                                       sheet = input$sheet_select))
      
      # Handle file naming
      base_name <- tools::file_path_sans_ext(input$files$name[1])
      if (input$sheet_select != "Sheet1") {
        base_name <- paste0(base_name, "_", input$sheet_select)
      }
      
      current_data <- datasets()
      if (base_name %in% names(current_data)) {
        counter <- 1
        while(paste0(base_name, "_", counter) %in% names(current_data)) {
          counter <- counter + 1
        }
        base_name <- paste0(base_name, "_", counter)
      }
      
      current_data[[base_name]] <- data
      datasets(current_data)
      updateAllSelectInputs(session)
      removeModal()
      
    }, error = function(e) {
      showNotification(paste("Error loading Excel sheet:", e$message), 
                       type = "error")
    })
  })
  
  # Update the column type inspection output
  output$column_types_table <- renderDT({
    req(input$manipulation_type == "inspect", input$inspect_table)
    
    tryCatch({
      data <- datasets()[[input$inspect_table]]
      
      if (!is.null(data)) {
        # Create a simplified data frame with just column names and types
        col_info <- data.frame(
          Column = names(data),
          Type = sapply(data, function(x) {
            # Get basic type information
            if (is.factor(x)) {
              "Factor"
            } else if (is.numeric(x)) {
              if (all(floor(x[!is.na(x)]) == x[!is.na(x)])) {
                "Integer"
              } else {
                "Numeric"
              }
            } else if (is.character(x)) {
              "Character"
            } else if (inherits(x, "Date")) {
              "Date"
            } else if (inherits(x, "POSIXct")) {
              "DateTime"
            } else if (is.logical(x)) {
              "Logical"
            } else {
              class(x)[1]
            }
          })
        )
        
        # Render the table with minimal options
        datatable(
          col_info,
          options = list(
            pageLength = 25,
            scrollY = "400px",
            dom = 't',  # Show only the table
            ordering = TRUE,
            searching = FALSE
          ),
          style = 'bootstrap',
          rownames = FALSE
        )
      }
    }, error = function(e) {
      showNotification(paste("Error inspecting data types:", e$message), 
                       type = "error")
      return(NULL)
    })
  })
  
  # Database connection logic
  observeEvent(input$db_connect, {
    req(input$db_type)
    
    tryCatch({
      conn <- switch(input$db_type,
                     "MySQL" = dbConnect(
                       RMySQL::MySQL(),
                       host = input$db_host,
                       port = input$db_port,
                       user = input$db_user,
                       password = input$db_pass,
                       dbname = input$db_name
                     ),
                     "PostgreSQL" = dbConnect(
                       RPostgres::Postgres(),
                       host = input$db_host,
                       port = input$db_port,
                       user = input$db_user,
                       password = input$db_pass,
                       dbname = input$db_name
                     ),
                     "SQLite" = {
                       req(input$sqlite_file)
                       dbConnect(RSQLite::SQLite(), 
                                 dbname = input$sqlite_file$datapath)
                     },
                     "DuckDB" = {
                       if(!is.null(input$duckdb_file) && !is.null(input$duckdb_file$datapath)) {
                         dbConnect(duckdb::duckdb(), dbdir = input$duckdb_file$datapath)
                       } else {
                         # In-memory database if no file provided
                         dbConnect(duckdb::duckdb(), dbdir = ":memory:")
                       }
                     },
                     "SQL Server" = dbConnect(
                       odbc::odbc(),
                       Driver = input$db_driver,
                       Server = input$db_host,
                       Database = input$db_name,
                       UID = input$db_user,
                       PWD = input$db_pass,
                       Port = input$db_port
                     ),
                     "Azure SQL" = dbConnect(
                       odbc::odbc(),
                       Driver = input$db_driver,
                       Server = paste0(input$db_host, ",", input$db_port),
                       Database = input$db_name,
                       UID = input$db_user,
                       PWD = input$db_pass,
                       Encrypt = "yes",
                       TrustServerCertificate = "no"
                     ),
                     "Other (ODBC)" = dbConnect(
                       odbc::odbc(),
                       DSN = input$db_dsn,
                       Driver = input$db_driver,
                       UID = input$db_user,
                       PWD = input$db_pass
                     )
      )
      
      db_conn(conn)
      showNotification("Connected successfully!", type = "message")
      
      # After successful connection, update the table list
      tables <- dbListTables(conn)
      updateSelectInput(session, "db_table", choices = tables)
      
    }, error = function(e) {
      showNotification(paste("Connection failed:", e$message), 
                       type = "error", duration = 10)
    })
  })
  
  # Output to check if database is connected
  output$is_db_connected <- reactive({
    !is.null(db_conn())
  })
  outputOptions(output, "is_db_connected", suspendWhenHidden = FALSE)
  
  # Database table selection UI
  output$db_table_ui <- renderUI({
    req(db_conn())
    tables <- dbListTables(db_conn())
    selectInput("db_table", "Select Table", choices = tables)
  })
  
  # Automatically view top 100 rows when table is selected
  observeEvent(input$db_table, {
    req(db_conn(), input$db_table, is.function(active_tab_id), is.function(query_tabs), is.function(next_tab_id))
    
    # Find current active tab id
    tab_id <- active_tab_id()
    if (is.null(tab_id) || length(query_tabs()) == 0) {
      # If no active tab, create one
      add_query_tab()
      tab_id <- next_tab_id() - 1
    }
    
    # Generate query to view top 100 rows
    auto_query <- paste("SELECT * FROM", input$db_table, "LIMIT 100")
    
    # Set the query in the active tab
    updateTextAreaInput(session, paste0("sql_query_", tab_id), value = auto_query)
    
    # Trigger execution of the query in the current tab
    later::later(function() {
      if (is.function(session$sendCustomMessage)) {
        session$sendCustomMessage(
          type = "shiny-eval-code",
          list(code = sprintf("$('#run_query_%s').click()", tab_id))
        )
      }
    }, 0.5)
  })
  
  # Close database connection on exit
  session$onSessionEnded(function() {
    if (!is.null(db_conn())) {
      dbDisconnect(db_conn())
    }
  })
  
  # Update columns for remove duplicates and keep duplicates when table is selected
  observeEvent(input$remove_duplicates_table, {
    req(input$remove_duplicates_table)
    data <- datasets()[[input$remove_duplicates_table]]
    if (!is.null(data)) {
      updateSelectizeInput(session, "remove_duplicates_columns", choices = names(data))
    }
  })
  
  observeEvent(input$keep_duplicates_table, {
    req(input$keep_duplicates_table)
    data <- datasets()[[input$keep_duplicates_table]]
    if (!is.null(data)) {
      updateSelectizeInput(session, "keep_duplicates_columns", choices = names(data))
    }
  })
  
  # Handle remove duplicates operation
  observeEvent(input$do_remove_duplicates, {
    req(input$remove_duplicates_table, input$remove_duplicates_columns)
    
    tryCatch({
      data <- datasets()[[input$remove_duplicates_table]]
      data <- data[!duplicated(data, by = input$remove_duplicates_columns)]
      
      # Update datasets
      current_data <- datasets()
      current_data[[input$remove_duplicates_table]] <- data
      datasets(current_data)
      updateAllSelectInputs(session)
      
      # Show success message
      showNotification("Duplicates removed successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Error removing duplicates:", e$message), type = "error")
    })
  })
  
  # Handle keep only duplicates operation
  observeEvent(input$do_keep_duplicates, {
    req(input$keep_duplicates_table, input$keep_duplicates_columns)
    
    tryCatch({
      data <- datasets()[[input$keep_duplicates_table]]
      data <- data[duplicated(data, by = input$keep_duplicates_columns) | 
                     duplicated(data, by = input$keep_duplicates_columns, fromLast = TRUE)]
      
      # Update datasets
      current_data <- datasets()
      current_data[[input$keep_duplicates_table]] <- data
      datasets(current_data)
      updateAllSelectInputs(session)
      
      # Show success message
      showNotification("Only duplicates kept successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Error keeping only duplicates:", e$message), type = "error")
    })
  })
  
  # Update columns for replace value when table is selected
  observeEvent(input$replace_value_table, {
    req(input$replace_value_table)
    data <- datasets()[[input$replace_value_table]]
    if (!is.null(data)) {
      updateSelectizeInput(session, "replace_value_columns", choices = names(data))
    }
  })
  
  # Handle replace value operation
  observeEvent(input$do_replace_value, {
    req(input$replace_value_table, input$replace_value_columns, input$old_value, input$new_value)
    
    tryCatch({
      data <- datasets()[[input$replace_value_table]]
      for (col in input$replace_value_columns) {
        data[[col]][data[[col]] == input$old_value] <- input$new_value
      }
      
      # Update datasets
      current_data <- datasets()
      current_data[[input$replace_value_table]] <- data
      datasets(current_data)
      updateAllSelectInputs(session)
      
      # Show success message
      showNotification("Values replaced successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Error replacing values:", e$message), type = "error")
    })
  })
  
  # Update columns for forecasting when table is selected
  observeEvent(input$forecast_table, {
    req(input$forecast_table)
    data <- datasets()[[input$forecast_table]]
    if (!is.null(data)) {
      updateSelectInput(session, "date_column", choices = names(data))
      updateSelectInput(session, "value_column", choices = names(data))
    }
  })
  
  # Handle forecasting operation
  observeEvent(input$run_forecast, {
    req(input$forecast_table, input$date_column, input$value_column, input$forecast_period)
    
    tryCatch({
      data <- datasets()[[input$forecast_table]]
      df <- data[, .(ds = as.Date(get(input$date_column)), y = as.numeric(get(input$value_column)))]
      
      # Fit the model
      m <- prophet(df)
      
      # Make future dataframe
      future <- make_future_dataframe(m, periods = input$forecast_period)
      
      # Predict
      forecast <- predict(m, future)
      
      # Update datasets with forecast
      forecast_data <- data.table(ds = forecast$ds, yhat = round(forecast$yhat, 2), yhat_lower = round(forecast$yhat_lower, 2), yhat_upper = round(forecast$yhat_upper, 2))
      current_data <- datasets()
      current_data[["forecast_results"]] <- forecast_data
      datasets(current_data)
      updateAllSelectInputs(session)
      
      # Plot forecast using plotly
      output$forecast_plot <- renderPlotly({
        plot_ly() %>%
          add_lines(x = ~forecast_data$ds, y = ~forecast_data$yhat, name = 'Forecast') %>%
          add_ribbons(x = ~forecast_data$ds, ymin = ~forecast_data$yhat_lower, ymax = ~forecast_data$yhat_upper, name = 'Uncertainty', fillcolor = 'rgba(7, 164, 181, 0.2)', line = list(color = 'transparent')) %>%
          layout(title = 'Forecast', xaxis = list(title = 'Date'), yaxis = list(title = 'Value', tickformat = ".2f"))
      })
      
      # Show forecast table
      output$forecast_table <- renderDT({
        datatable(forecast_data, 
                  options = list(
                    pageLength = 10, 
                    scrollX = TRUE,
                    dom = 'Bfrtip',
                    buttons = list(
                      'copy',
                      list(
                        extend = 'collection',
                        buttons = list(
                          list(
                            extend = 'csv',
                            filename = paste0("forecast_results_", format(Sys.Date(), "%Y%m%d"))
                          ),
                          list(
                            extend = 'excel',
                            filename = paste0("forecast_results_", format(Sys.Date(), "%Y%m%d"))
                          ),
                          list(
                            extend = 'pdf',
                            filename = paste0("forecast_results_", format(Sys.Date(), "%Y%m%d"))
                          )
                        ),
                        text = 'Download'
                      ),
                      'print',
                      list(
                        extend = 'collection',
                        text = 'View',
                        action = JS("function ( e, dt, node, config ) {
                         Shiny.setInputValue('fullscreen_plot', true);
                       }")
                      )
                    )
                  ),
                  extensions = c('Buttons'),
                  callback = JS("
                   table.buttons().container().css('margin-bottom', '10px');
                   table.buttons().container().css('margin-right', '10px');
                 ")
        )
      })
      
      showNotification("Forecast completed successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Error running forecast:", e$message), type = "error")
    })
  })
  
  # Handle full-screen plot
  observeEvent(input$fullscreen_plot, {
    showModal(modalDialog(
      title = "Forecast Plot",
      size = "l",
      easyClose = TRUE,
      plotlyOutput("fullscreen_forecast_plot", height = "600px")
    ))
  })
  
  # Render full-screen plot
  output$fullscreen_forecast_plot <- renderPlotly({
    req(datasets()[["forecast_results"]])
    forecast_data <- datasets()[["forecast_results"]]
    
    plot_ly() %>%
      add_lines(x = ~forecast_data$ds, y = ~forecast_data$yhat, name = 'Forecast') %>%
      add_ribbons(x = ~forecast_data$ds, ymin = ~forecast_data$yhat_lower, ymax = ~forecast_data$yhat_upper, 
                  name = 'Uncertainty', fillcolor = 'rgba(7, 164, 181, 0.2)', line = list(color = 'transparent')) %>%
      layout(title = 'Forecast',
             xaxis = list(title = 'Date'),
             yaxis = list(title = 'Value', tickformat = ".2f"),
             showlegend = TRUE,
             margin = list(l = 50, r = 50, b = 50, t = 50, pad = 4))
  })
  
  # Update condition column choices when table is selected
  observeEvent(input$conditional_table, {
    req(input$conditional_table)
    data <- datasets()[[input$conditional_table]]
    if (!is.null(data)) {
      updateSelectInput(session, "condition_column", 
                        choices = names(data))
    }
  })
  
  # Handle conditional column operation
  observeEvent(input$do_conditional, {
    req(input$conditional_table, input$condition_column, 
        input$condition_operator, input$condition_value,
        input$true_value, input$false_value, input$new_column_name)
    
    tryCatch({
      data <- datasets()[[input$conditional_table]]
      
      # Create the conditional column based on operator
      result <- switch(input$condition_operator,
                       "==" = data[[input$condition_column]] == input$condition_value,
                       "!=" = data[[input$condition_column]] != input$condition_value,
                       ">" = as.numeric(data[[input$condition_column]]) > as.numeric(input$condition_value),
                       "<" = as.numeric(data[[input$condition_column]]) < as.numeric(input$condition_value),
                       ">=" = as.numeric(data[[input$condition_column]]) >= as.numeric(input$condition_value),
                       "<=" = as.numeric(data[[input$condition_column]]) <= as.numeric(input$condition_value),
                       "contains" = grepl(input$condition_value, data[[input$condition_column]], fixed = TRUE),
                       "startswith" = startsWith(as.character(data[[input$condition_column]]), input$condition_value),
                       "endswith" = endsWith(as.character(data[[input$condition_column]]), input$condition_value)
      )
      
      # Add the new column with conditional values
      data[, (input$new_column_name) := ifelse(result, input$true_value, input$false_value)]
      
      # Update datasets
      current_data <- datasets()
      current_data[[input$conditional_table]] <- data
      datasets(current_data)
      updateAllSelectInputs(session)
      
      showNotification("Conditional column added successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Error adding conditional column:", e$message), 
                       type = "error")
    })
  })
  
  # Update both condition column choices when table is selected
  observeEvent(input$conditional_table, {
    req(input$conditional_table)
    data <- datasets()[[input$conditional_table]]
    if (!is.null(data)) {
      updateSelectInput(session, "condition_column_1", choices = names(data))
      updateSelectInput(session, "condition_column_2", choices = names(data))
    }
  })
  
  # Updated handle conditional column operation with multiple conditions
  observeEvent(input$do_conditional, {
    req(input$conditional_table, input$condition_column_1, 
        input$condition_operator_1, input$condition_value_1,
        input$true_value, input$false_value, input$new_column_name)
    
    tryCatch({
      data <- copy(datasets()[[input$conditional_table]])  # Create a copy of the data
      
      # Evaluate first condition
      result1 <- switch(input$condition_operator_1,
                        "==" = data[[input$condition_column_1]] == input$condition_value_1,
                        "!=" = data[[input$condition_column_1]] != input$condition_value_1,
                        ">" = as.numeric(data[[input$condition_column_1]]) > as.numeric(input$condition_value_1),
                        "<" = as.numeric(data[[input$condition_column_1]]) < as.numeric(input$condition_value_1),
                        ">=" = as.numeric(data[[input$condition_column_1]]) >= as.numeric(input$condition_value_1),
                        "<=" = as.numeric(data[[input$condition_column_1]]) <= as.numeric(input$condition_value_1),
                        "contains" = grepl(input$condition_value_1, data[[input$condition_column_1]], fixed = TRUE),
                        "startswith" = startsWith(as.character(data[[input$condition_column_1]]), input$condition_value_1),
                        "endswith" = EndsWith(as.character(data[[input$condition_column_1]]), input$condition_value_1))
      
      final_result <- result1
      
      # If second condition is active, evaluate it and combine with first condition
      if (input$condition_logic != "None") {
        req(input$condition_column_2, input$condition_operator_2, input$condition_value_2)
        
        result2 <- switch(input$condition_operator_2,
                          "==" = data[[input$condition_column_2]] == input$condition_value_2,
                          "!=" = data[[input$condition_column_2]] != input$condition_value_2,
                          ">" = as.numeric(data[[input$condition_column_2]]) > as.numeric(input$condition_value_2),
                          "<" = as.numeric(data[[input$condition_column_2]]) < as.numeric(input$condition_value_2),
                          ">=" = as.numeric(data[[input$condition_column_2]]) >= as.numeric(input$condition_value_2),
                          "<=" = as.numeric(data[[input$condition_column_2]]) <= as.numeric(input$condition_value_2),
                          "contains" = grepl(input$condition_value_2, data[[input$condition_column_2]], fixed = TRUE),
                          "startswith" = startsWith(as.character(data[[input$condition_column_2]]), input$condition_value_2),
                          "endswith" = EndsWith(as.character(data[[input$condition_column_2]]), input$condition_value_2))
        
        # Combine conditions based on logic
        final_result <- if (input$condition_logic == "AND") {
          result1 & result2
        } else {  # OR
          result1 | result2
        }
      }
      
      # Add the new column with conditional values
      data[, (input$new_column_name) := ifelse(final_result, input$true_value, input$false_value)]
      
      # Update datasets with the modified data
      current_data <- datasets()
      current_data[[input$conditional_table]] <- data
      datasets(current_data)
      
      # Show success message
      showNotification("Conditional column added successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Error adding conditional column:", e$message), 
                       type = "error")
    })
  })
  
  # Add Pivot Table functionality
  observeEvent(input$create_pivot, {
    req(input$pivot_table)
    data <- datasets()[[input$pivot_table]]
    
    # Sample data for pivot table if too large
    if(nrow(data) > 20000) {
      data <- sample_large_dataset(data, n = 20000)
      showNotification(
        "Dataset sampled to 20,000 rows for pivot table analysis", 
        type = "warning"
      )
    }
    
    # Render the pivot table
    output$pivot_output <- renderRpivotTable({
      rpivotTable(data,
                  rows = names(data)[1], 
                  cols = if(length(names(data)) > 1) names(data)[2] else NULL,
                  aggregatorName = "Count",
                  rendererName = "Table",
                  width = "100%",
                  height = "auto",
                  onRefresh = htmlwidgets::JS("function(config) { setTimeout(function() { window.dispatchEvent(new Event('resize')); }, 500); }"))
    })
    
    # Force resize of the container after pivot table is rendered
    runjs("
      $(document).ready(function() {
        setTimeout(function() {
          window.dispatchEvent(new Event('resize'));
        }, 1000);
      });
    ")
  })
  
  # Create and manage query tabs
  output$query_tabs <- renderUI({
    tabs <- query_tabs()
    if (length(tabs) == 0) {
      # Add initial query tab when connected
      observeEvent(req(db_conn()), {
        if (length(query_tabs()) == 0) {
          add_query_tab()
        }
      }, once = TRUE)
      
      return(tags$div("No query tabs open. Click 'New Query' to start."))
    }
    
    # Create tabsetPanel for query tabs
    do.call(tabsetPanel, c(
      id = "active_query_tab",
      lapply(tabs, function(tab) {
        tabPanel(
          title = span(
            paste("Query", tab$id),
            tags$button(
              class = "close-tab", type = "button", "×",
              style = "margin-left: 5px; color: red; font-weight: bold;",
              onclick = sprintf("Shiny.setInputValue('close_tab_%s', true, {priority: 'event'})", tab$id)
            )
          ),
          value = as.character(tab$id),
          div(
            style = "padding: 10px;",
            tags$div(
              style = "margin-bottom: 15px;",
              tags$label("Enter SQL Query:"),
              tags$textarea(
                id = paste0("sql_query_", tab$id),
                class = "form-control",
                rows = 5,
                placeholder = "SELECT * FROM your_table LIMIT 100",
                if(!is.null(tab$query)) tab$query else ""
              ),
              tags$div(
                style = "margin-top: 10px; display: flex; justify-content: space-between;",
                actionButton(
                  paste0("run_query_", tab$id),
                  "Execute Query",
                  class = "btn-success"
                ),
                if (!is.null(tab$result_name)) {
                  span(
                    style = "line-height: 34px;",
                    paste("Results stored as:", tab$result_name)
                  )
                }
              )
            ),
            tags$div(
              style = "margin-top: 10px;",
              box(
                width = 12,
                title = "Results",
                status = "info",
                solidHeader = TRUE,
                DTOutput(paste0("query_results_", tab$id))
              )
            )
          )
        )
      })
    ))
  })
  
  # Handle adding new query tab
  observeEvent(input$add_query_tab, {
    req(is.function(query_tabs), is.function(next_tab_id))
    current_tabs <- query_tabs()
    new_id <- next_tab_id()
    
    current_tabs[[length(current_tabs) + 1]] <- list(
      id = new_id,
      query = NULL,
      result_name = NULL
    )
    
    query_tabs(current_tabs)
    next_tab_id(new_id + 1)
    
    # Activate the new tab
    if (is.function(active_tab_id)) {
      active_tab_id(new_id)
    }
  })
  
  # Observe active tab changes
  observeEvent(input$active_query_tab, {
    if (is.function(active_tab_id)) {
      active_tab_id(as.numeric(input$active_query_tab))
    }
  })
  
  # Handle closing tabs
  observe({
    tabs <- query_tabs()
    for (tab in tabs) {
      local({
        local_id <- tab$id
        
        # Create dynamic observer for each tab's close button
        observeEvent(input[[paste0("close_tab_", local_id)]], {
          current_tabs <- query_tabs()
          
          # Safely find tab to close
          tab_index <- which(sapply(current_tabs, function(t) t$id == local_id))
          
          if (length(tab_index) > 0) {
            # Get the tab that's being closed
            tab_to_close <- current_tabs[[tab_index]]
            
            # Remove result from active queries if this tab had results
            if (!is.null(tab_to_close$result_name)) {
              # Update active queries list
              updateAllSelectInputs(session)
            }
            
            # Remove the tab
            current_tabs <- current_tabs[-tab_index]
            
            # Update tabs
            query_tabs(current_tabs)
            
            # If we closed the last tab, add a new one
            if (length(current_tabs) == 0) {
              add_query_tab()
            }
          } else {
            # Handle case where tab isn't found
            showNotification("Tab not found", type = "warning")
          }
        })
      })
    }
  })
  
  # Handle executing queries from individual tabs
  observe({
    req(is.function(db_conn))
    tabs <- query_tabs()
    for (tab in tabs) {
      local({
        local_id <- tab$id
        
        # Create dynamic observer for each tab's execute button
        observeEvent(input[[paste0("run_query_", local_id)]], {
          req(db_conn())
          
          # Get query text from this tab
          query_text <- input[[paste0("sql_query_", local_id)]]
          
          # Execute the query
          tryCatch({
            withProgress(message = 'Executing query...', value = 0.5, {
              query_result <- dbGetQuery(db_conn(), query_text)
              
              # Convert to data.table for consistency
              if(!is.null(query_result) && nrow(query_result) > 0) {
                query_result <- as.data.table(query_result)
                
                # Create a descriptive name for results
                query_short <- tolower(trimws(query_text))
                
                # Try to extract table name from query
                table_name <- NULL
                if (grepl("from\\s+([^\\s,();]+)", query_short)) {
                  table_name <- gsub(".*from\\s+([^\\s,();]+).*", "\\1", query_short)
                  table_name <- gsub("[\"'`\\[\\]]", "", table_name) # Remove quotes and brackets
                }
                
                # Create result name
                if (!is.null(table_name) && nchar(table_name) > 0) {
                  result_name <- paste0("query_", table_name, "_", format(Sys.time(), "%H%M%S"))
                } else {
                  # Use first few words of query
                  query_words <- strsplit(query_short, "\\s+")[[1]]
                  query_prefix <- paste(head(query_words, 3), collapse = "_")
                  result_name <- paste0("query_", query_prefix, "_", format(Sys.time(), "%H%M%S"))
                }
                
                # Ensure name is valid
                result_name <- gsub("[^a-zA-Z0-9_]", "", result_name)
                
                # Store query result in datasets
                current_data <- datasets()
                current_data[[result_name]] <- query_result
                datasets(current_data)
                
                # Store in active query datasets with metadata
                current_queries <- active_query_datasets()
                current_queries[[result_name]] <- list(
                  query = query_text,
                  tab_id = local_id,
                  timestamp = Sys.time(),
                  rows = nrow(query_result),
                  cols = ncol(query_result)
                )
                active_query_datasets(current_queries)
                
                # Update all select inputs with filtered data
                updateAllSelectInputs(session)
                
                # Update this tab's stored data
                current_tabs <- query_tabs()
                tab_index <- which(sapply(current_tabs, function(t) t$id == local_id))
                if (length(tab_index) > 0) {
                  current_tabs[[tab_index]]$query <- query_text
                  current_tabs[[tab_index]]$result_name <- result_name
                  query_tabs(current_tabs)
                }
                
                # Display results in this tab
                output[[paste0("query_results_", local_id)]] <- renderDT({
                  datatable(
                    query_result,
                    options = list(
                      pageLength = 10,
                      scrollX = TRUE,
                      scrollY = "300px",
                      scroller = TRUE
                    ),
                    style = 'bootstrap'
                  )
                })
                
                # Show notification
                showNotification(HTML(paste0(
                  "Query executed successfully. <br>",
                  "Result stored as <b>", result_name, "</b> <br>",
                  "Available in Data Management and Visualization tabs."
                )), type = "message", duration = 5)
              } else {
                showNotification("Query executed but returned no results or affected no rows.", 
                                 type = "warning")
              }
            })
          }, error = function(e) {
            showNotification(paste("Query error:", e$message), 
                             type = "error")
          })
        })
      })
    }
  })
  
  # Helper function for adding a new query tab
  add_query_tab <- function() {
    req(is.function(query_tabs), is.function(next_tab_id))
    current_tabs <- query_tabs()
    new_id <- next_tab_id()
    
    current_tabs[[length(current_tabs) + 1]] <- list(
      id = new_id,
      query = NULL,
      result_name = NULL
    )
    
    query_tabs(current_tabs)
    next_tab_id(new_id + 1)
    
    # Set as active tab after slight delay to allow rendering
    later::later(function() {
      if (exists("session") && is.function(session$sendCustomMessage)) {
        updateTabsetPanel(session, "active_query_tab", selected = as.character(new_id))
      }
    }, 0.1)
  }
  
  # Helper function to refresh the UI after changes to query tabs
  refresh_query_ui <- function() {
    # Update all select inputs to reflect current active queries
    updateAllSelectInputs(session)
    
    # Force refresh of table tabs (data management section)
    output$table_tabs <- renderUI({
      # ... existing code ...
    })
    
    # Invalidate reactive values that depend on active queries
    invalidateLater(100)
  }
}

# Run the application
shinyApp(ui = ui, server = server)
