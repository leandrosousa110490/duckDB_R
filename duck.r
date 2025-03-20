# DuckDB SQL Query Interface
# A Shiny application for executing SQL queries on DuckDB databases

# Ensure required packages are installed
if (!require("prophet")) {
  message("Installing prophet package...")
  install.packages("prophet", dependencies = TRUE)
  library(prophet)
  
  # Verify installation
  if (!require("prophet")) {
    stop("Failed to install prophet. Please install it manually with: install.packages('prophet', dependencies = TRUE)")
  }
}

# Define a direct prediction function for prophet models
# This ensures we can always predict even if the export is missing
prophet_prediction <- function(model, future_df) {
  # This directly calls the predict method without relying on exports
  return(predict(model, future_df))
}

library(shiny)
library(DBI)
library(duckdb)
library(DT)
library(shinythemes)
library(shinyjs)
library(shinyWidgets)
library(shinydashboard)
library(shinyFiles)
library(shinyAce)  # Add shinyAce for SQL syntax highlighting
library(rpivotTable) # Add rpivotTable for pivot tables
library(plotly)  # For data visualization instead of radiant
library(tibble)
library(readr)
library(readxl)
library(arrow)
library(magrittr)  # For the pipe operator
library(esquisse)  # For interactive ggplot visualization
library(dplyr)     # For data manipulation
library(lubridate) # For date/time handling
library(Rcpp)      # Required for prophet to work correctly
library(prophet)   # For time series forecasting

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
      .ace_editor { border: 1px solid #ddd; border-radius: 4px; }
      .ace_keyword { color: #D73A49 !important; font-weight: bold; }
      .ace_function { color: #6F42C1 !important; }
      .ace_string { color: #032F62 !important; }
      .ace_numeric { color: #005CC5 !important; }
      .ace_operator { color: #D73A49 !important; }
      .ace_comment { color: #6A737D !important; font-style: italic; }
      .radiant-container { width: 100%; height: 800px; }
      .pivot-container { padding: 20px; }
      .esquisse-container { height: 800px; overflow: hidden; }
      .summary-stats { padding: 15px; background-color: #f9f9f9; border-radius: 5px; margin-bottom: 15px; }
      .summary-value { font-size: 24px; font-weight: bold; color: #2c3e50; }
      .summary-label { font-size: 14px; color: #7b8a8b; }
      
      /* Full-width content */
      .content-container {
        padding: 15px;
      }
      
      /* Header styling */
      .app-header {
        background-color: #2c3e50;
        color: white;
        padding: 15px;
        margin-bottom: 20px;
        border-radius: 5px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
      }
      
      /* Status bar */
      .status-bar {
        background-color: #f8f9fa;
        padding: 10px 15px;
        border-radius: 5px;
        margin-bottom: 15px;
        border-left: 4px solid #2c3e50;
      }
      
      /* Tab styling */
      .nav-tabs > li > a {
        background-color: #f8f9fa;
        color: #2c3e50;
        border-radius: 4px 4px 0 0;
      }
      .nav-tabs > li.active > a, 
      .nav-tabs > li.active > a:hover, 
      .nav-tabs > li.active > a:focus {
        background-color: #ffffff;
        color: #2c3e50;
        font-weight: bold;
      }
    "))
  ),
  
  # App Header
  div(class = "app-header",
      div(class = "container-fluid",
          div(class = "row",
              div(class = "col-md-10",
                  h2(icon("database"), "DuckDB SQL Query Interface")
              ),
              div(class = "col-md-2 text-right",
                  br(),
                  actionButton("connect_btn", "Connect", icon = icon("plug"), 
                               class = "btn-success"),
                  actionButton("disconnect_btn", "Disconnect", icon = icon("unlink"), 
                               class = "btn-danger")
              )
          )
      )
  ),
  
  # Status Bar
  div(class = "status-bar", textOutput("connection_status")),
  
  # Main Content
  div(class = "content-container",
      tabsetPanel(
        id = "main_tabs",
        
        # Query Tab (replaces sidebar)
        tabPanel("Query Editor", 
                 # Top section with database tools and connection options
                 fluidRow(
                   # Database connection section
                   column(4,
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
                            )
                          )
                   ),
                   
                   # Database explorer section
                   column(4,
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
                          )
                   ),
                   
                   # Database tools section
                   column(4,
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
                          )
                   )
                 ),
                 
                 # SQL Query editor section
                 fluidRow(
                   column(12,
                          box(
                            width = 12,
                            title = "SQL Query", 
                            status = "primary", 
                            solidHeader = TRUE,
                            
                            aceEditor(
                              outputId = "sql_query",
                              value = "SELECT * FROM sqlite_master;",
                              mode = "sql",
                              theme = "sqlserver",
                              height = "250px",
                              fontSize = 14,
                              autoComplete = "enabled",
                              highlightActiveLine = TRUE,
                              showLineNumbers = TRUE,
                              hotkeys = list(runKey = list(win = "Ctrl-Enter", mac = "Cmd-Enter")),
                              wordWrap = TRUE
                            ),
                            
                            div(
                              style = "display: flex; justify-content: space-between; align-items: center; margin-top: 10px;",
                              div(
                                style = "display: flex; align-items: center;",
                                checkboxInput("explain_query", "Explain Query Plan", value = FALSE),
                                span(style = "margin-left: 15px; color: #6c757d; font-size: 0.9em;", "Press Ctrl+Enter to run")
                              ),
                              div(
                                style = "display: flex; justify-content: flex-end;",
                                actionButton("format_query_btn", "Format SQL", icon = icon("indent"), 
                                             class = "btn-sm btn-info", style = "margin-right: 10px;"),
                                actionButton("run_query_btn", "Run Query", icon = icon("play"), 
                                             class = "btn-primary")
                              )
                            )
                          )
                   )
                 ),
                 
                 # Query results section
                 fluidRow(
                   column(12,
                          box(
                            width = 12,
                            title = "Query Results", 
                            status = "info", 
                            solidHeader = TRUE,
                            
                            # Query status and controls
                            verbatimTextOutput("query_status"),
                            hr(),
                            div(
                              style = "display: flex; justify-content: space-between; margin-bottom: 10px;",
                              div(
                                style = "display: flex; gap: 10px;",
                                downloadButton("download_results", "Download Results"),
                                actionButton("clear_btn", "Clear Results", icon = icon("eraser"), 
                                             class = "btn-warning")
                              ),
                              div(
                                selectInput("results_page_length", "Rows per page:", 
                                            choices = c(10, 15, 25, 50, 100),
                                            selected = 15, width = "150px")
                              )
                            ),
                            
                            # Results table
                            DTOutput("results_table")
                          )
                   )
                 )
        ),
        
        # Pivot Table Tab
        tabPanel("Pivot Table",
                 box(
                   width = 12,
                   h4("Interactive Pivot Table"),
                   p("Drag and drop fields to create custom pivot tables and charts. This view uses the current query results."),
                   hr(),
                   conditionalPanel(
                     condition = "output.has_query_results",
                     div(
                       class = "pivot-container",
                       rpivotTableOutput("pivot_table")
                     )
                   ),
                   conditionalPanel(
                     condition = "!output.has_query_results",
                     div(
                       style = "text-align: center; padding: 30px; color: #6c757d;",
                       icon("info-circle", style = "font-size: 48px;"),
                       h4("No Data Available"),
                       p("Run a SQL query first to load data for the pivot table.")
                     )
                   )
                 )),
        
        # Data Visualization Tab
        tabPanel("Data Visualization",
                 box(
                   width = 12,
                   h4("Interactive Data Visualization"),
                   p("Visualize your query results with interactive charts."),
                   hr(),
                   conditionalPanel(
                     condition = "output.has_query_results",
                     fluidRow(
                       column(3,
                              selectInput("plot_type", "Plot Type:", 
                                          choices = c("Bar Chart" = "bar",
                                                      "Line Chart" = "line",
                                                      "Scatter Plot" = "scatter",
                                                      "Box Plot" = "box",
                                                      "Histogram" = "histogram"),
                                          selected = "bar")
                       ),
                       column(3,
                              uiOutput("x_axis_ui")
                       ),
                       column(3,
                              uiOutput("y_axis_ui")
                       ),
                       column(3,
                              uiOutput("color_by_ui")
                       )
                     ),
                     hr(),
                     h4("Summary Statistics"),
                     fluidRow(
                       column(4,
                              selectInput("summary_column", "Select Column for Summary:", 
                                          choices = NULL)
                       ),
                       column(4,
                              selectInput("summary_function", "Summary Function:", 
                                          choices = c("Count" = "count",
                                                      "Sum" = "sum",
                                                      "Mean" = "mean",
                                                      "Median" = "median",
                                                      "Min" = "min",
                                                      "Max" = "max",
                                                      "Standard Deviation" = "sd"),
                                          selected = "mean")
                       ),
                       column(4,
                              checkboxInput("group_by_summary", "Group By", value = FALSE),
                              conditionalPanel(
                                condition = "input.group_by_summary == true",
                                selectInput("group_by_column", "Group By Column:", choices = NULL)
                              )
                       )
                     ),
                     fluidRow(
                       column(12,
                              uiOutput("summary_stats"),
                              DT::dataTableOutput("summary_table")
                       )
                     ),
                     hr(),
                     plotlyOutput("data_plot", height = "600px")
                   ),
                   conditionalPanel(
                     condition = "!output.has_query_results",
                     div(
                       style = "text-align: center; padding: 30px; color: #6c757d;",
                       icon("info-circle", style = "font-size: 48px;"),
                       h4("No Data Available"),
                       p("Run a SQL query first to load data for visualization.")
                     )
                   )
                 )),
        
        # Database Info Tab
        tabPanel("Database Info", 
                 box(
                   width = 12,
                   h4("Database Structure"),
                   verbatimTextOutput("db_info")
                 )),
        
        # Esquisse Tab
        tabPanel("Esquisse",
                 box(
                   width = 12,
                   h4("Interactive ggplot Builder"),
                   p("Create custom visualizations using the interactive ggplot builder. This view uses the current query results."),
                   hr(),
                   conditionalPanel(
                     condition = "output.has_query_results",
                     div(
                       class = "esquisse-container",
                       esquisse::esquisse_ui("esquisse")
                     )
                   ),
                   conditionalPanel(
                     condition = "!output.has_query_results",
                     div(
                       style = "text-align: center; padding: 30px; color: #6c757d;",
                       icon("info-circle", style = "font-size: 48px;"),
                       h4("No Data Available"),
                       p("Run a SQL query first to load data for the ggplot builder.")
                     )
                   )
                 )),
        
        # Prophet Tab
        tabPanel("Prophet",
                 box(
                   width = 12,
                   h4("Time Series Forecasting with Prophet"),
                   p("Forecast your time series data using Facebook's Prophet algorithm."),
                   hr(),
                   conditionalPanel(
                     condition = "output.has_query_results",
                     fluidRow(
                       column(3,
                              uiOutput("date_column_ui")
                       ),
                       column(3,
                              uiOutput("value_column_ui")
                       ),
                       column(3,
                              numericInput("forecast_periods", "Forecast Periods:", 
                                           value = 30, min = 1, max = 365)
                       ),
                       column(3,
                              checkboxInput("yearly_seasonality", "Yearly Seasonality", value = TRUE),
                              checkboxInput("weekly_seasonality", "Weekly Seasonality", value = TRUE)
                       )
                     ),
                     hr(),
                     
                     # Advanced Forecasting Options (collapsible box)
                     fluidRow(
                       column(12,
                              box(
                                width = 12,
                                title = "Advanced Options",
                                status = "info", 
                                solidHeader = TRUE,
                                collapsible = TRUE,
                                collapsed = TRUE,
                                
                                fluidRow(
                                  column(4,
                                         sliderInput("changepoint_prior_scale", "Changepoint Prior Scale:",
                                                     min = 0.001, max = 0.5, value = 0.05, step = 0.001,
                                                     ticks = FALSE)
                                  ),
                                  column(4,
                                         sliderInput("seasonality_prior_scale", "Seasonality Prior Scale:",
                                                     min = 0.01, max = 10, value = 10, step = 0.01,
                                                     ticks = FALSE)
                                  ),
                                  column(4,
                                         sliderInput("changepoint_range", "Changepoint Range:",
                                                     min = 0.1, max = 0.95, value = 0.8, step = 0.05,
                                                     ticks = FALSE)
                                  )
                                ),
                                fluidRow(
                                  column(4,
                                         numericInput("n_changepoints", "Number of Changepoints:", 
                                                      value = 25, min = 0, max = 100, step = 1)
                                  ),
                                  column(4,
                                         checkboxInput("data_preprocessing", "Pre-process Data", value = TRUE),
                                         conditionalPanel(
                                           condition = "input.data_preprocessing == true",
                                           checkboxInput("remove_outliers", "Remove Outliers", value = FALSE)
                                         )
                                  ),
                                  column(4,
                                         selectInput("growth", "Growth Model:", 
                                                     choices = c("Linear" = "linear", 
                                                                 "Logistic" = "logistic"),
                                                     selected = "linear"),
                                         conditionalPanel(
                                           condition = "input.growth == 'logistic'",
                                           numericInput("cap", "Capacity Cap:", 
                                                        value = 1, min = 0)
                                         )
                                  )
                                ),
                                fluidRow(
                                  column(6,
                                         selectInput("seasonality_mode", "Seasonality Mode:", 
                                                     choices = c("Additive" = "additive", 
                                                                 "Multiplicative" = "multiplicative"),
                                                     selected = "additive")
                                  ),
                                  column(6,
                                         checkboxInput("daily_seasonality", "Daily Seasonality", value = FALSE),
                                         conditionalPanel(
                                           condition = "input.daily_seasonality == true",
                                           numericInput("daily_seasonality_fourier", "Daily Fourier Terms:", 
                                                        value = 10, min = 1, max = 20)
                                         )
                                  )
                                ),
                                p(style = "font-size: 0.9em; color: #6c757d; margin-top: 10px;",
                                  "Adjust these parameters to improve forecast accuracy. Higher changepoint_prior_scale makes trend more flexible. Higher seasonality_prior_scale makes seasonality more flexible.")
                              )
                       )
                     ),
                     hr(),
                     div(
                       class = "text-center",
                       actionButton("run_forecast_btn", "Run Forecast", icon = icon("chart-line"), 
                                    class = "btn-primary"),
                       conditionalPanel(
                         condition = "input.run_forecast_btn > 0 && output.is_forecasting",
                         div(
                           style = "margin-top: 15px;",
                           p("Generating forecast..."),
                           div(class = "progress-striped active",
                               div(class = "progress-bar progress-bar-info", 
                                   style = "width: 100%"))
                         )
                       )
                     ),
                     hr(),
                     plotlyOutput("prophet_plot", height = "600px"),
                     
                     # Add model metrics display
                     fluidRow(
                       column(12,
                              uiOutput("forecast_metrics"),
                              hr()
                       )
                     ),
                     
                     # Component plots when available
                     fluidRow(
                       column(12,
                              conditionalPanel(
                                condition = "input.show_components && output.has_query_results",
                                h4("Forecast Components"),
                                plotlyOutput("prophet_components", height = "400px")
                              ),
                              checkboxInput("show_components", "Show Forecast Components", value = FALSE)
                       )
                     ),
                     
                     DT::dataTableOutput("forecast_table")
                   ),
                   conditionalPanel(
                     condition = "!output.has_query_results",
                     div(
                       style = "text-align: center; padding: 30px; color: #6c757d;",
                       icon("info-circle", style = "font-size: 48px;"),
                       h4("No Data Available"),
                       p("Run a SQL query with date/time and numeric columns first to use the forecasting tool.")
                     )
                   )
                 )),
        
        # Query History Tab
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
        
        # Help Tab
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
                               tags$li(
                                 HTML("<code><span style='color:#D73A49;font-weight:bold;'>SELECT</span> * <span style='color:#D73A49;font-weight:bold;'>FROM</span> sqlite_master;</code>")
                               ),
                               tags$li(
                                 HTML("<code><span style='color:#D73A49;font-weight:bold;'>CREATE TABLE</span> tablename (<span style='color:#6F42C1;'>col1</span> <span style='color:#005CC5;'>INT</span>, <span style='color:#6F42C1;'>col2</span> <span style='color:#005CC5;'>VARCHAR</span>);</code>")
                               ),
                               tags$li(
                                 HTML("<code><span style='color:#D73A49;font-weight:bold;'>INSERT INTO</span> tablename <span style='color:#D73A49;font-weight:bold;'>VALUES</span> (1, <span style='color:#032F62;'>'value'</span>);</code>")
                               ),
                               tags$li(
                                 HTML("<code><span style='color:#D73A49;font-weight:bold;'>SELECT</span> * <span style='color:#D73A49;font-weight:bold;'>FROM</span> tablename <span style='color:#D73A49;font-weight:bold;'>WHERE</span> condition;</code>")
                               )
                             )
                     ),
                     tags$li(tags$b("Working with Spaces in Names:"), 
                             tags$ul(
                               tags$li(
                                 HTML("<code><span style='color:#D73A49;font-weight:bold;'>SELECT</span> * <span style='color:#D73A49;font-weight:bold;'>FROM</span> <span style='color:#032F62;'>\"My Table\"</span>;</code>")
                               ),
                               tags$li(
                                 HTML("<code><span style='color:#D73A49;font-weight:bold;'>SELECT</span> <span style='color:#032F62;'>\"First Name\"</span>, <span style='color:#032F62;'>\"Last Name\"</span> <span style='color:#D73A49;font-weight:bold;'>FROM</span> customers;</code>")
                               ),
                               tags$li(
                                 HTML("<code><span style='color:#D73A49;font-weight:bold;'>SELECT</span> t.<span style='color:#032F62;'>\"Product Name\"</span> <span style='color:#D73A49;font-weight:bold;'>FROM</span> <span style='color:#032F62;'>\"Product Items\"</span> t;</code>")
                               ),
                               tags$li(
                                 HTML("<code><span style='color:#D73A49;font-weight:bold;'>SELECT</span> c.name, o.<span style='color:#032F62;'>\"order date\"</span> <span style='color:#D73A49;font-weight:bold;'>FROM</span> customers c <span style='color:#D73A49;font-weight:bold;'>JOIN</span> <span style='color:#032F62;'>\"Order Details\"</span> o <span style='color:#D73A49;font-weight:bold;'>ON</span> c.id = o.customer_id;</code>")
                               )
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
                   
                   h4("SQL Editor Keyboard Shortcuts"),
                   tags$ul(
                     tags$li(tags$b("Run Query:"), "Ctrl+Enter"),
                     tags$li(tags$b("Format SQL:"), "Click the 'Format SQL' button to beautify your query"),
                     tags$li(tags$b("Auto-complete:"), "Ctrl+Space to trigger auto-completion")
                   ),
                   
                   h4("Advanced Analysis Features"),
                   tags$ul(
                     tags$li(tags$b("Pivot Table:"), "Create interactive pivot tables and charts from your query results. Drag and drop columns to pivot, filter, and aggregate your data."),
                     tags$li(tags$b("Data Visualization:"), "Use interactive charts to visualize your query results. Choose from various chart types and apply filtering and grouping."),
                     tags$li(tags$b("Summary Statistics:"), "Calculate statistical measures like sum, count, median, mean, and standard deviation for numeric data columns."),
                     tags$li(tags$b("Esquisse:"), "Build custom ggplot2 visualizations using an intuitive drag-and-drop interface. Perfect for creating publication-quality graphics."),
                     tags$li(tags$b("Prophet:"), "Perform time series forecasting on your data using Facebook's Prophet algorithm. Predict future values based on historical patterns.")
                   ),
                   
                   h4("Examples with Spaces in Names"),
                   tags$div(
                     class = "sql-example-container",
                     style = "background-color: #f8f9fa; padding: 15px; border-radius: 4px; font-family: monospace; overflow-x: auto;",
                     HTML("
                       <div><span style='color:#6A737D;'>-- Select from a table with spaces in its name</span></div>
                       <div><span style='color:#D73A49;font-weight:bold;'>SELECT</span> * <span style='color:#D73A49;font-weight:bold;'>FROM</span> <span style='color:#032F62;'>\"My Table\"</span>;</div>
                       <br>
                       <div><span style='color:#6A737D;'>-- Select columns with spaces</span></div>
                       <div><span style='color:#D73A49;font-weight:bold;'>SELECT</span> <span style='color:#032F62;'>\"First Name\"</span>, <span style='color:#032F62;'>\"Last Name\"</span> <span style='color:#D73A49;font-weight:bold;'>FROM</span> <span style='color:#032F62;'>\"Customer Data\"</span>;</div>
                       <br>
                       <div><span style='color:#6A737D;'>-- Join tables with spaces in names and columns</span></div>
                       <div><span style='color:#D73A49;font-weight:bold;'>SELECT</span> c.<span style='color:#032F62;'>\"First Name\"</span>, o.<span style='color:#032F62;'>\"Order Date\"</span>, p.<span style='color:#032F62;'>\"Product Name\"</span></div>
                       <div><span style='color:#D73A49;font-weight:bold;'>FROM</span> <span style='color:#032F62;'>\"Customers\"</span> c</div>
                       <div><span style='color:#D73A49;font-weight:bold;'>JOIN</span> <span style='color:#032F62;'>\"Orders\"</span> o <span style='color:#D73A49;font-weight:bold;'>ON</span> c.<span style='color:#032F62;'>\"Customer ID\"</span> = o.<span style='color:#032F62;'>\"Customer ID\"</span></div>
                       <div><span style='color:#D73A49;font-weight:bold;'>JOIN</span> <span style='color:#032F62;'>\"Products\"</span> p <span style='color:#D73A49;font-weight:bold;'>ON</span> o.<span style='color:#032F62;'>\"Product ID\"</span> = p.<span style='color:#032F62;'>\"Product ID\"</span></div>
                       <div><span style='color:#D73A49;font-weight:bold;'>ORDER BY</span> o.<span style='color:#032F62;'>\"Order Date\"</span>;</div>
                     ")
                   )
                 ))
      )
  )
)

# Server logic
server <- function(input, output, session) {
  # Set up shinyFiles
  volumes <- c(Home = fs::path_home(), getVolumes()())
  shinyFileChoose(input, "file_select", roots = volumes, filetypes = c("duckdb", "db"))
  
  # Define a reactive value to track if query results are available
  output$has_query_results <- reactive({
    return(!is.null(values$query_result) && nrow(values$query_result) > 0)
  })
  outputOptions(output, "has_query_results", suspendWhenHidden = FALSE)
  
  # Define a reactive value to track if forecasting is in progress
  output$is_forecasting <- reactive({
    return(values$forecasting)
  })
  outputOptions(output, "is_forecasting", suspendWhenHidden = FALSE)
  
  # Add SQL formatting function
  formatSQL <- function(sql_query) {
    # Basic SQL formatting (this is a simple version)
    # Replace this with a more robust SQL formatter if needed
    
    # Convert SQL keywords to uppercase
    keywords <- c(
      # Standard SQL keywords
      "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", 
      "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET", "UNION", "ALL", 
      "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "TABLE",
      "ALTER", "DROP", "INDEX", "VIEW", "AND", "OR", "NOT", "NULL", "IS", "IN",
      "BETWEEN", "LIKE", "ASC", "DESC", "DISTINCT", "AS", "WITH", "CASE", "WHEN",
      "THEN", "ELSE", "END", "ON", "USING", "EXISTS",
      
      # DuckDB specific keywords
      "PRAGMA", "COPY", "DESCRIBE", "EXPLAIN", "EXPORT", "IMPORT", "INSTALL", 
      "LOAD", "ATTACH", "DETACH", "PREPARE", "EXECUTE", "VACUUM", "CHECKPOINT",
      "READ_CSV", "READ_CSV_AUTO", "READ_PARQUET", "READ_JSON", "READ_JSON_AUTO",
      "BOOL", "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT", "HUGEINT",
      "REAL", "FLOAT", "DOUBLE", "DECIMAL", "VARCHAR", "CHAR", "TEXT", "DATE", "TIME",
      "TIMESTAMP", "BLOB", "PARTITION", "CAST", "TRY_CAST", "OVER"
    )
    
    formatted_sql <- sql_query
    
    # Add newlines after common clause keywords
    clause_keywords <- c("SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "LIMIT")
    for (keyword in clause_keywords) {
      pattern <- paste0("(?i)\\b", keyword, "\\b")
      replacement <- paste0("\n", toupper(keyword), " ")
      formatted_sql <- gsub(pattern, replacement, formatted_sql, perl = TRUE)
    }
    
    # Add newlines and indentation for joins
    join_keywords <- c("LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "OUTER JOIN", "JOIN")
    for (keyword in join_keywords) {
      pattern <- paste0("(?i)\\b", keyword, "\\b")
      replacement <- paste0("\n  ", toupper(keyword), " ")
      formatted_sql <- gsub(pattern, replacement, formatted_sql, perl = TRUE)
    }
    
    # Convert all other keywords to uppercase
    for (keyword in keywords) {
      pattern <- paste0("(?i)\\b", keyword, "\\b")
      replacement <- toupper(keyword)
      formatted_sql <- gsub(pattern, replacement, formatted_sql, perl = TRUE)
    }
    
    # Convert comma followed by a non-space to comma space
    formatted_sql <- gsub(",([^ ])", ", \\1", formatted_sql)
    
    # Remove multiple consecutive spaces
    formatted_sql <- gsub("[ ]+", " ", formatted_sql)
    
    # Remove trailing whitespace
    formatted_sql <- gsub("[ \t]+\n", "\n", formatted_sql)
    
    # Ensure there's no leading whitespace
    formatted_sql <- gsub("^\n+", "", formatted_sql)
    
    return(formatted_sql)
  }
  
  # Format SQL button handler
  observeEvent(input$format_query_btn, {
    if (trimws(input$sql_query) != "") {
      # Format the SQL query
      formatted_sql <- formatSQL(input$sql_query)
      # Update the ace editor with formatted SQL
      updateAceEditor(session, "sql_query", value = formatted_sql)
    }
  })
  
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
    selected_table = NULL,
    prophet_plot_data = NULL,
    prophet_forecast_table = NULL,
    forecasting = FALSE
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
      
      # Generate a SELECT query for the table using the helper function with SQL keywords in uppercase
      if (schema == "main") {
        query <- paste0("SELECT * FROM ", quoteIdentifier(table), " LIMIT 100;")
      } else {
        query <- paste0("SELECT * FROM ", quoteIdentifier(schema), ".", quoteIdentifier(table), " LIMIT 100;")
      }
      
      # Format the SQL for better display
      formatted_query <- formatSQL(query)
      
      # Update the query input
      updateAceEditor(session, "sql_query", value = formatted_query)
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
          
          # Make the analysis tabs available when results are returned
          shinyjs::removeClass(selector = ".nav-tabs a[data-value='Pivot Table']", class = "disabled")
          shinyjs::removeClass(selector = ".nav-tabs a[data-value='Data Visualization']", class = "disabled")
          
          # Force reactivity update for tabs
          output$has_query_results <- reactive({
            return(TRUE)
          })
        } else {
          values$status <- "Query executed successfully. No rows returned."
          
          # Force reactivity update for tabs
          output$has_query_results <- reactive({
            return(FALSE)
          })
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
        
        # Force reactivity update for tabs
        output$has_query_results <- reactive({
          return(FALSE)
        })
        
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
      
      # Stay on the Query Editor tab to show results
      # (No need to switch tabs, results are already visible)
      
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
        formatted_query <- formatSQL(paste0("SELECT * FROM ", quoted_table_name, " LIMIT 100;"))
        updateAceEditor(session, "sql_query", value = formatted_query)
      } else {
        values$status <- paste0("Error: Table '", table_name, "' was not created. Check the file format.")
      }
      
    }, error = function(e) {
      values$status <- paste0("Error importing file: ", e$message)
      print(paste("Import error:", e$message))
    })
  })
  
  # Clear results
  observeEvent(input$clear_btn, {
    values$query_result <- NULL
    values$status <- "Results cleared."
    
    # Force reactivity update for tabs
    output$has_query_results <- reactive({
      return(FALSE)
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
      updateAceEditor(session, "sql_query", value = selected_query)
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
  
  # Render pivot table
  output$pivot_table <- renderRpivotTable({
    req(values$query_result)
    
    rpivotTable(
      data = values$query_result,
      rows = colnames(values$query_result)[1],  # Default to first column for rows
      cols = if(ncol(values$query_result) > 1) colnames(values$query_result)[2] else NULL,  # Default to second column for cols if available
      aggregatorName = "Count",
      rendererName = "Table",
      width = "100%",
      height = "600px"
    )
  })
  
  # Dynamic UI for visualization
  output$x_axis_ui <- renderUI({
    req(values$query_result)
    selectInput("x_axis", "X-Axis:", 
                choices = colnames(values$query_result),
                selected = colnames(values$query_result)[1])
  })
  
  output$y_axis_ui <- renderUI({
    req(values$query_result)
    selectInput("y_axis", "Y-Axis:", 
                choices = colnames(values$query_result),
                selected = if(ncol(values$query_result) > 1) colnames(values$query_result)[2] else colnames(values$query_result)[1])
  })
  
  output$color_by_ui <- renderUI({
    req(values$query_result)
    selectInput("color_by", "Color By:", 
                choices = c("None" = "none", colnames(values$query_result)),
                selected = "none")
  })
  
  # Update the data visualization plots
  output$data_plot <- renderPlotly({
    req(values$query_result, input$plot_type, input$x_axis, input$y_axis)
    
    plot_data <- values$query_result
    plot_type <- input$plot_type
    
    # Basic plot parameters
    p <- plot_ly(plot_data)
    
    # Apply color if selected
    if (!is.null(input$color_by) && input$color_by != "none") {
      color_var <- input$color_by
      
      if (plot_type == "bar") {
        p <- plot_ly(plot_data, x = ~get(input$x_axis), y = ~get(input$y_axis), 
                     color = ~get(color_var), type = "bar")
      } else if (plot_type == "line") {
        p <- plot_ly(plot_data, x = ~get(input$x_axis), y = ~get(input$y_axis), 
                     color = ~get(color_var), type = "scatter", mode = "lines")
      } else if (plot_type == "scatter") {
        p <- plot_ly(plot_data, x = ~get(input$x_axis), y = ~get(input$y_axis), 
                     color = ~get(color_var), type = "scatter", mode = "markers")
      } else if (plot_type == "box") {
        p <- plot_ly(plot_data, y = ~get(input$y_axis), color = ~get(color_var), type = "box")
      } else if (plot_type == "histogram") {
        p <- plot_ly(plot_data, x = ~get(input$x_axis), color = ~get(color_var), type = "histogram")
      }
    } else {
      if (plot_type == "bar") {
        p <- plot_ly(plot_data, x = ~get(input$x_axis), y = ~get(input$y_axis), type = "bar")
      } else if (plot_type == "line") {
        p <- plot_ly(plot_data, x = ~get(input$x_axis), y = ~get(input$y_axis), 
                     type = "scatter", mode = "lines")
      } else if (plot_type == "scatter") {
        p <- plot_ly(plot_data, x = ~get(input$x_axis), y = ~get(input$y_axis), 
                     type = "scatter", mode = "markers")
      } else if (plot_type == "box") {
        p <- plot_ly(plot_data, y = ~get(input$y_axis), type = "box")
      } else if (plot_type == "histogram") {
        p <- plot_ly(plot_data, x = ~get(input$x_axis), type = "histogram")
      }
    }
    
    # Add layout with responsive sizing
    p <- p %>% layout(
      title = paste("Plot of", input$y_axis, "vs", input$x_axis),
      xaxis = list(title = input$x_axis),
      yaxis = list(title = input$y_axis),
      autosize = TRUE
    )
    
    return(p)
  })
  
  # Update column choices for summary statistics
  observe({
    req(values$query_result)
    
    # Get column names
    col_names <- colnames(values$query_result)
    
    # Detect numeric columns for summary functions
    numeric_cols <- sapply(values$query_result, is.numeric)
    numeric_col_names <- col_names[numeric_cols]
    
    # Update UI elements
    updateSelectInput(session, "summary_column", 
                      choices = numeric_col_names,
                      selected = if(length(numeric_col_names) > 0) numeric_col_names[1] else NULL)
    
    updateSelectInput(session, "group_by_column", 
                      choices = col_names,
                      selected = if(length(col_names) > 0) col_names[1] else NULL)
  })
  
  # Calculate and render summary statistics
  output$summary_stats <- renderUI({
    req(values$query_result, input$summary_column, input$summary_function)
    
    # Check if the selected column exists and is numeric
    if(!input$summary_column %in% colnames(values$query_result) || 
       !is.numeric(values$query_result[[input$summary_column]])) {
      return(div(
        class = "summary-stats",
        "Please select a numeric column for summary statistics."
      ))
    }
    
    # Calculate summary statistic
    if(input$group_by_summary && !is.null(input$group_by_column)) {
      # No calculation here, handled by the summary table
      return(NULL)
    } else {
      # Simple summary without grouping
      col_data <- values$query_result[[input$summary_column]]
      
      result <- switch(input$summary_function,
                       "count" = length(col_data),
                       "sum" = sum(col_data, na.rm = TRUE),
                       "mean" = mean(col_data, na.rm = TRUE),
                       "median" = median(col_data, na.rm = TRUE),
                       "min" = min(col_data, na.rm = TRUE),
                       "max" = max(col_data, na.rm = TRUE),
                       "sd" = sd(col_data, na.rm = TRUE),
                       NA)
      
      # Format the result
      formatted_result <- if(input$summary_function %in% c("mean", "sd")) {
        round(result, 4)
      } else {
        result
      }
      
      # Create a nice UI for the result
      div(
        class = "summary-stats",
        div(class = "summary-value", formatted_result),
        div(class = "summary-label", 
            paste(toupper(substr(input$summary_function, 1, 1)), 
                  substr(input$summary_function, 2, nchar(input$summary_function)), 
                  "of", input$summary_column))
      )
    }
  })
  
  # Render summary table for grouped statistics
  output$summary_table <- renderDT({
    req(values$query_result, input$summary_column, input$summary_function)
    
    # Check if grouping is enabled
    if(!input$group_by_summary || is.null(input$group_by_column)) {
      return(NULL)
    }
    
    # Check if the selected column exists and is numeric
    if(!input$summary_column %in% colnames(values$query_result) || 
       !is.numeric(values$query_result[[input$summary_column]])) {
      return(NULL)
    }
    
    # Create a grouped summary using dplyr
    summary_data <- values$query_result %>%
      group_by_at(input$group_by_column) %>%
      summarize_at(vars(input$summary_column), 
                   list(Value = function(x) {
                     switch(input$summary_function,
                            "count" = length(x),
                            "sum" = sum(x, na.rm = TRUE),
                            "mean" = mean(x, na.rm = TRUE),
                            "median" = median(x, na.rm = TRUE),
                            "min" = min(x, na.rm = TRUE),
                            "max" = max(x, na.rm = TRUE),
                            "sd" = sd(x, na.rm = TRUE),
                            NA)
                   })) %>%
      as.data.frame()
    
    # Return a datatable
    datatable(summary_data,
              options = list(
                pageLength = 10,
                dom = 'Bfrtip',
                buttons = c('copy', 'csv', 'excel')
              ),
              rownames = FALSE) %>%
      formatRound(columns = "Value", digits = 4)
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
    isolate({
      if (!is.null(values$conn)) {
        dbDisconnect(values$conn, shutdown = TRUE)
      }
    })
  })
  
  # Esquisse module for interactive ggplot
  observe({
    req(values$query_result)
    # Call esquisse module with current query results
    esquisse::esquisse_server(
      id = "esquisse",
      data = reactive({values$query_result})
    )
  })
  
  # Prophet forecasting - UI elements
  output$date_column_ui <- renderUI({
    req(values$query_result)
    
    # Detect date/datetime columns
    col_names <- colnames(values$query_result)
    date_cols <- sapply(values$query_result, function(x) {
      inherits(x, "Date") || inherits(x, "POSIXct") || inherits(x, "POSIXlt") ||
        (is.character(x) && all(grepl("^\\d{4}-\\d{2}-\\d{2}", na.omit(x))))
    })
    date_col_names <- col_names[date_cols]
    
    # If no date columns found, allow selecting character columns that might contain dates
    if(length(date_col_names) == 0) {
      char_cols <- sapply(values$query_result, is.character)
      date_col_names <- col_names[char_cols]
    }
    
    selectInput("date_column", "Date Column:", 
                choices = date_col_names,
                selected = if(length(date_col_names) > 0) date_col_names[1] else NULL)
  })
  
  output$value_column_ui <- renderUI({
    req(values$query_result)
    
    # Detect numeric columns
    col_names <- colnames(values$query_result)
    numeric_cols <- sapply(values$query_result, is.numeric)
    numeric_col_names <- col_names[numeric_cols]
    
    selectInput("value_column", "Value Column:", 
                choices = numeric_col_names,
                selected = if(length(numeric_col_names) > 0) numeric_col_names[1] else NULL)
  })
  
  # Prophet forecasting - Generate forecast
  observeEvent(input$run_forecast_btn, {
    req(values$query_result, input$date_column, input$value_column)
    
    # Show status message that forecasting is in progress
    values$status <- "Generating forecast... please wait."
    values$forecasting <- TRUE
    
    # Check that required columns exist
    if(!input$date_column %in% colnames(values$query_result) || 
       !input$value_column %in% colnames(values$query_result)) {
      values$status <- "Error: Selected columns not found in data."
      values$forecasting <- FALSE
      return(NULL)
    }
    
    # Check that value column is numeric
    if(!is.numeric(values$query_result[[input$value_column]])) {
      values$status <- "Error: Value column must be numeric for forecasting."
      values$forecasting <- FALSE
      return(NULL)
    }
    
    # Prepare data for Prophet
    tryCatch({
      # Keep original raw data for reference
      date_column_raw <- values$query_result[[input$date_column]]
      
      # Print debugging for date column
      print(paste("Date column type:", class(date_column_raw)[1]))
      print(paste("First few date values:", paste(head(date_column_raw), collapse=", ")))
      
      # Create data frame with required 'ds' and 'y' columns for Prophet
      # Try multiple methods to ensure proper date conversion
      prophet_data <- data.frame(
        ds = NA,  # Will be filled with proper dates
        y = as.numeric(values$query_result[[input$value_column]])
      )
      
      # First attempt - try as_datetime then convert to Date
      prophet_data$ds <- tryCatch({
        as.Date(lubridate::as_datetime(date_column_raw))
      }, error = function(e) {
        # Return NA on error - we'll try other methods
        rep(NA, length(date_column_raw))
      })
      
      # If first attempt failed, try direct Date conversion with various formats
      if (all(is.na(prophet_data$ds))) {
        date_formats <- c("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y", "%m-%d-%Y")
        
        for (format in date_formats) {
          prophet_data$ds <- tryCatch({
            as.Date(date_column_raw, format = format)
          }, error = function(e) {
            # Continue with next format if this one fails
            prophet_data$ds
          })
          
          # Break out of loop if conversion was successful
          if (!all(is.na(prophet_data$ds))) {
            print(paste("Successful date conversion with format:", format))
            break
          }
        }
      }
      
      # Last resort - try lubridate's flexible parser
      if (all(is.na(prophet_data$ds))) {
        prophet_data$ds <- tryCatch({
          as.Date(lubridate::parse_date_time(date_column_raw, orders = c("ymd", "mdy", "dmy", "ymd HMS", "mdy HMS", "dmy HMS")))
        }, error = function(e) {
          # If this fails, we'll handle it below
          prophet_data$ds
        })
      }
      
      # Final check for valid dates
      if (all(is.na(prophet_data$ds))) {
        values$status <- paste0("Error: Could not convert date column '", input$date_column, 
                                "' to valid dates. Please select a different column or fix date format.")
        print(paste("Date conversion failed. Sample values:", paste(head(date_column_raw), collapse=", ")))
        values$forecasting <- FALSE
        return(NULL)
      }
      
      # Check for partial date conversion failure
      if (any(is.na(prophet_data$ds))) {
        valid_count <- sum(!is.na(prophet_data$ds))
        total_count <- length(prophet_data$ds)
        print(paste("Partial date conversion:", valid_count, "out of", total_count, "dates successfully converted"))
        
        # Remove NA values
        prophet_data <- prophet_data[!is.na(prophet_data$ds), ]
        
        # Check if we have enough data left
        if (nrow(prophet_data) < 2) {
          values$status <- paste0("Error: After removing invalid dates, only ", nrow(prophet_data), 
                                  " valid date(s) remain. Need at least 2 data points.")
          values$forecasting <- FALSE
          return(NULL)
        }
      }
      
      # Remove rows with NA values in either column
      prophet_data <- na.omit(prophet_data)
      
      # Check if we have enough data points
      if (nrow(prophet_data) < 2) {
        values$status <- "Error: Not enough valid data points for forecasting (need at least 2)."
        values$forecasting <- FALSE
        return(NULL)
      }
      
      # Sort data by date (required for Prophet)
      prophet_data <- prophet_data[order(prophet_data$ds), ]
      
      # Verify date range
      min_date <- min(prophet_data$ds)
      max_date <- max(prophet_data$ds)
      date_range <- as.numeric(difftime(max_date, min_date, units = "days"))
      
      print(paste("Date range:", date_range, "days"))
      print(paste("Min date:", min_date))
      print(paste("Max date:", max_date))
      
      # Handle zero date range by adding a small offset
      if (date_range <= 0) {
        # All dates are the same - add a small offset for the last point
        if (nrow(prophet_data) > 1) {
          print("Detected zero date range. Attempting to add artificial date spread.")
          # Make a copy of the data with dates spread out by 1 day increments
          date_seq <- seq(as.Date(min_date), as.Date(min_date) + nrow(prophet_data) - 1, by = "day")
          
          # Create a new data frame with spread-out dates
          spread_data <- prophet_data
          spread_data$ds <- date_seq
          
          # Print what we're doing
          print(paste("Created artificial date range from", min(date_seq), "to", max(date_seq)))
          
          # Use the spread data instead
          prophet_data <- spread_data
          
          # Recalculate date range
          date_range <- as.numeric(difftime(max(prophet_data$ds), min(prophet_data$ds), units = "days"))
          print(paste("New date range:", date_range, "days"))
          
          # Warn the user that we're using artificial dates
          values$status <- paste("Warning: All dates in your data were the same. Using artificial date range for forecasting.")
        } else {
          values$status <- "Error: Invalid date range. All dates are the same and there is only one data point."
          values$forecasting <- FALSE
          return(NULL)
        }
      }
      
      # Print debugging information
      print(paste("Forecast input:", nrow(prophet_data), "rows"))
      print(paste("Date range:", min(prophet_data$ds), "to", max(prophet_data$ds), 
                  "(", date_range, "days)"))
      print(paste("Value range:", min(prophet_data$y), "to", max(prophet_data$y),
                  "std dev:", sd(prophet_data$y)))
      
      # Data preprocessing if enabled
      if(input$data_preprocessing) {
        # Handle missing values if any
        if(any(is.na(prophet_data$y))) {
          prophet_data <- prophet_data[!is.na(prophet_data$y), ]
          print(paste("Removed", sum(is.na(prophet_data$y)), "rows with missing values"))
        }
        
        # Outlier removal if enabled
        if(input$remove_outliers) {
          print("Removing outliers...")
          # Calculate IQR for outlier detection
          q1 <- quantile(prophet_data$y, 0.25)
          q3 <- quantile(prophet_data$y, 0.75)
          iqr <- q3 - q1
          lower_bound <- q1 - 1.5 * iqr
          upper_bound <- q3 + 1.5 * iqr
          
          # Filter out outliers
          outlier_count <- sum(prophet_data$y < lower_bound | prophet_data$y > upper_bound)
          prophet_data <- prophet_data[prophet_data$y >= lower_bound & prophet_data$y <= upper_bound, ]
          print(paste("Removed", outlier_count, "outliers"))
        }
        
        # Ensure we still have enough data
        if(nrow(prophet_data) < 2) {
          values$status <- "Error: Not enough data points after preprocessing."
          values$forecasting <- FALSE
          return(NULL)
        }
      }
      
      # Prepare logistic growth cap/floor if needed
      growth_params <- list()
      if(input$growth == "logistic") {
        # Add capacity cap
        prophet_data$cap <- input$cap
        growth_params$cap <- input$cap
      }
      
      # Create and fit Prophet model with appropriate parameters
      m <- prophet::prophet(
        prophet_data,
        growth = input$growth,
        changepoint.prior.scale = input$changepoint_prior_scale,
        seasonality.prior.scale = input$seasonality_prior_scale,
        changepoint.range = input$changepoint_range,
        n.changepoints = input$n_changepoints,
        yearly.seasonality = if(date_range > 360) input$yearly_seasonality else FALSE,
        weekly.seasonality = if(date_range > 14) input$weekly_seasonality else FALSE,
        daily.seasonality = input$daily_seasonality,
        seasonality.mode = input$seasonality_mode,
        uncertainty.samples = 1000
      )
      
      # Add custom seasonality if needed
      if(input$daily_seasonality && exists("daily_seasonality_fourier")) {
        m <- prophet::add_seasonality(m, 
                                      name = 'daily', 
                                      period = 1, 
                                      fourier.order = input$daily_seasonality_fourier)
      }
      
      # Create future dataframe for prediction
      future <- prophet::make_future_dataframe(m, periods = input$forecast_periods)
      
      # Add cap for logistic growth if needed
      if(input$growth == "logistic") {
        future$cap <- input$cap
      }
      
      # Make prediction with the model
      # The proper way to get predictions from a prophet model
      forecast <- tryCatch({
        # Use an alternative approach - directly access the predict function from the prophet model
        if ("predict" %in% getNamespaceExports("prophet")) {
          # If predict is exported from prophet, use it
          prophet::predict(m, future)
        } else if ("predictive_samples" %in% getNamespaceExports("prophet")) {
          # If predictive_samples is available, use that instead
          print("Using predictive_samples approach")
          samples <- prophet::predictive_samples(m, future)
          
          # Extract predictions and confidence intervals
          yhat <- apply(samples$samples, 2, mean)
          yhat_lower <- apply(samples$samples, 2, function(x) quantile(x, 0.025))
          yhat_upper <- apply(samples$samples, 2, function(x) quantile(x, 0.975))
          
          # Create a data frame similar to what predict would return
          data.frame(
            ds = future$ds,
            yhat = yhat,
            yhat_lower = yhat_lower,
            yhat_upper = yhat_upper
          )
        } else {
          # Fall back to standard S3 method
          predict(m, future)
        }
      }, error = function(e) {
        print(paste("First prediction attempt failed:", e$message))
        
        # Try with the generic predict if available
        tryCatch({
          predict(m, future)
        }, error = function(e2) {
          print(paste("Second prediction attempt failed:", e2$message))
          NULL
        })
      })
      
      # If still an error, try one more approach
      if(is.null(forecast) || inherits(forecast, "try-error")) {
        # Try loading the specific predict method for prophet models
        # This is a last-resort hack, but might work in some cases
        tryCatch({
          print("Trying alternate prediction method...")
          
          if (exists("prophet_fit", where = asNamespace("prophet"))) {
            print("Using prophet_fit method")
            fitted <- asNamespace("prophet")$prophet_fit(m, prophet_data)
            predicted <- asNamespace("prophet")$prophet_predict(fitted, future)
            forecast <- predicted
          } else if (exists("fit.prophet", where = asNamespace("prophet"))) {
            print("Using fit.prophet method")
            predict_fn <- get("fit.prophet", asNamespace("prophet"))
            forecast <- predict_fn(m, future)
          } else {
            # Last approach - check if we can find the right methods
            print("Trying generic prediction with available methods")
            methods_available <- methods(class = class(m)[1])
            print(paste("Available methods:", paste(methods_available, collapse=", ")))
            
            if ("predict.prophet" %in% methods_available) {
              forecast <- predict.prophet(m, future)
            } else {
              print("No suitable prediction method found")
              forecast <- NULL
            }
          }
        }, error = function(e) {
          print(paste("Final prediction attempt failed:", e$message))
          forecast <- NULL
        })
      }
      
      # If we still don't have a forecast, report error
      if(is.null(forecast) || inherits(forecast, "try-error")) {
        values$status <- "Error: Could not generate forecast. See logs for details."
        values$forecasting <- FALSE
        return(NULL)
      }
      
      # Ensure we have a reasonable forecast
      if(nrow(forecast) <= nrow(prophet_data)) {
        values$status <- "Error: Forecast couldn't be generated beyond input data."
        values$forecasting <- FALSE
        return(NULL)
      }
      
      # Calculate model performance metrics on the training data
      # This helps users understand the model's accuracy
      fitted_values <- forecast$yhat[1:nrow(prophet_data)]
      actual_values <- prophet_data$y
      
      # Calculate common error metrics
      rmse <- sqrt(mean((fitted_values - actual_values)^2, na.rm = TRUE))
      mae <- mean(abs(fitted_values - actual_values), na.rm = TRUE)
      mape <- 100 * mean(abs((actual_values - fitted_values) / actual_values), na.rm = TRUE)
      
      # Store metrics for display
      values$forecast_metrics <- list(
        rmse = rmse,
        mae = mae,
        mape = mape
      )
      
      print(paste("Model metrics - RMSE:", round(rmse, 2), 
                  "MAE:", round(mae, 2), 
                  "MAPE:", round(mape, 2), "%"))
      
      # Prepare data for plots with proper structure for plotly
      actual_data <- data.frame(
        date = prophet_data$ds,
        value = prophet_data$y,
        type = "Actual"
      )
      
      # Only include future dates in forecast portion
      forecast_indices <- which(forecast$ds > max(prophet_data$ds))
      if(length(forecast_indices) == 0) {
        values$status <- "Error: No future dates in forecast."
        values$forecasting <- FALSE
        return(NULL)
      }
      
      forecast_data <- data.frame(
        date = forecast$ds[forecast_indices],
        value = forecast$yhat[forecast_indices],
        type = "Forecast"
      )
      
      # Also include confidence intervals
      forecast_data$lower <- forecast$yhat_lower[forecast_indices]
      forecast_data$upper <- forecast$yhat_upper[forecast_indices]
      
      # Combine for plotting
      plot_data <- rbind(
        actual_data,
        # Just use the needed columns for the binding
        forecast_data[, c("date", "value", "type")]
      )
      
      # Create forecast table for display with formatted date
      forecast_table <- forecast[forecast_indices, ] %>%
        dplyr::select(ds, yhat, yhat_lower, yhat_upper) %>%
        dplyr::rename(
          Date = ds,
          Forecast = yhat,
          Lower_Bound = yhat_lower,
          Upper_Bound = yhat_upper
        )
      
      # Store results in reactive values
      values$prophet_plot_data <- plot_data
      values$prophet_forecast_data <- forecast_data  # Store with confidence intervals
      values$prophet_forecast_table <- forecast_table
      values$prophet_model <- m  # Store the full model for component plots
      values$full_forecast <- forecast  # Store full forecast with components
      
      values$status <- "Forecast generated successfully."
      values$forecasting <- FALSE
      
    }, error = function(e) {
      print(paste("Prophet forecasting error:", e$message))
      traceback()  # Print the full error trace for debugging
      
      # Print information about the prophet package
      print("Prophet package information:")
      print(packageVersion("prophet"))
      print("Prophet namespace exports:")
      print(getNamespaceExports("prophet"))
      
      # Check if required functions exist
      print("Prophet functions check:")
      print(paste("prophet function exists:", exists("prophet", where = asNamespace("prophet"))))
      print(paste("predict method exists for prophet class:", 
                  "prophet" %in% rownames(methods(predict))))
      
      values$status <- paste0("Error in forecasting: ", e$message)
      values$forecasting <- FALSE
    })
  })
  
  # Render Prophet plot
  output$prophet_plot <- renderPlotly({
    req(values$prophet_plot_data)
    
    # Make sure we have data in the correct format
    if(!"date" %in% colnames(values$prophet_plot_data) || 
       !"value" %in% colnames(values$prophet_plot_data) ||
       !"type" %in% colnames(values$prophet_plot_data)) {
      return(plotly_empty(type = "scatter", mode = "markers") %>% 
               layout(title = "Error: Invalid forecast data format"))
    }
    
    # Split data by type for separate traces
    actual_data <- values$prophet_plot_data[values$prophet_plot_data$type == "Actual",]
    forecast_data <- values$prophet_plot_data[values$prophet_plot_data$type == "Forecast",]
    
    if(nrow(actual_data) == 0 || nrow(forecast_data) == 0) {
      return(plotly_empty(type = "scatter", mode = "markers") %>% 
               layout(title = "Error: Missing actual or forecast data"))
    }
    
    # Create base plot
    p <- plot_ly() 
    
    # Add actual data trace
    p <- p %>% add_trace(
      data = actual_data,
      x = ~date, 
      y = ~value,
      type = "scatter",
      mode = "markers+lines",
      name = "Actual",
      marker = list(color = "blue", size = 5)
    )
    
    # Add forecast trace
    p <- p %>% add_trace(
      data = forecast_data,
      x = ~date, 
      y = ~value,
      type = "scatter",
      mode = "lines",
      name = "Forecast",
      line = list(color = "red", width = 2)
    )
    
    # Add confidence interval if available
    if(exists("prophet_forecast_data", values) && 
       !is.null(values$prophet_forecast_data) &&
       "lower" %in% colnames(values$prophet_forecast_data) &&
       "upper" %in% colnames(values$prophet_forecast_data)) {
      
      ci_data <- values$prophet_forecast_data
      
      # Add confidence interval as a filled area
      p <- p %>% add_ribbons(
        data = ci_data,
        x = ~date,
        ymin = ~lower,
        ymax = ~upper,
        name = "95% Confidence",
        fillcolor = "rgba(255, 0, 0, 0.2)",
        line = list(color = "transparent"),
        showlegend = TRUE
      )
    }
    
    # Enhance layout
    p <- p %>% layout(
      title = "Time Series Forecast",
      xaxis = list(
        title = "Date",
        rangeselector = list(
          buttons = list(
            list(count = 1, label = "1m", step = "month", stepmode = "backward"),
            list(count = 6, label = "6m", step = "month", stepmode = "backward"),
            list(count = 1, label = "1y", step = "year", stepmode = "backward"),
            list(step = "all")
          )
        ),
        rangeslider = list(type = "date"),
        type = "date"
      ),
      yaxis = list(title = input$value_column),
      showlegend = TRUE,
      hoverlabel = list(bgcolor = "white", font = list(size = 12)),
      hovermode = "closest"
    )
    
    return(p)
  })
  
  # Render forecast table
  output$forecast_table <- renderDT({
    req(values$prophet_forecast_table)
    
    datatable(values$prophet_forecast_table,
              options = list(
                pageLength = 10,
                dom = 'Bfrtip',
                buttons = c('copy', 'csv', 'excel')
              ),
              rownames = FALSE) %>%
      formatRound(columns = c("Forecast", "Lower_Bound", "Upper_Bound"), digits = 2)
  })
  
  # Render forecast metrics
  output$forecast_metrics <- renderUI({
    req(values$forecast_metrics)
    
    metrics <- values$forecast_metrics
    
    fluidRow(
      column(12,
             h4("Forecast Accuracy Metrics"),
             p("These metrics indicate how well the model fits the historical data. Lower values indicate better accuracy.")
      ),
      column(4,
             div(class = "summary-stats",
                 div(class = "summary-value", round(metrics$rmse, 2)),
                 div(class = "summary-label", "RMSE (Root Mean Square Error)")
             )
      ),
      column(4,
             div(class = "summary-stats",
                 div(class = "summary-value", round(metrics$mae, 2)),
                 div(class = "summary-label", "MAE (Mean Absolute Error)")
             )
      ),
      column(4,
             div(class = "summary-stats",
                 div(class = "summary-value", paste0(round(metrics$mape, 2), "%")),
                 div(class = "summary-label", "MAPE (Mean Absolute Percentage Error)")
             )
      )
    )
  })
  
  # Render Prophet component plots
  output$prophet_components <- renderPlotly({
    req(values$prophet_plot_data, values$prophet_forecast_data, input$show_components)
    
    # Component plots are only available if we have the full Prophet model components
    if(!exists("prophet_model", values) || is.null(values$prophet_model)) {
      return(plot_ly() %>% 
               layout(title = "Component plots unavailable. Re-run forecast to generate components."))
    }
    
    # Extract components from the forecast
    m <- values$prophet_model
    forecast <- values$full_forecast
    
    # Check if components are available
    if(!all(c("trend", "yearly", "weekly") %in% colnames(forecast))) {
      # If components aren't available yet, generate plot showing this
      return(plot_ly() %>% 
               layout(title = "Component data not available for this forecast."))
    }
    
    # Create subplot with trend, yearly and weekly components if available
    plot_list <- list()
    
    # Trend component
    trend_plot <- plot_ly(data = forecast, x = ~ds, y = ~trend, 
                          type = "scatter", mode = "lines", name = "Trend") %>%
      layout(title = "Trend Component", showlegend = FALSE)
    plot_list[[1]] <- trend_plot
    
    # Yearly seasonality if available
    if("yearly" %in% colnames(forecast) && sum(!is.na(forecast$yearly)) > 0) {
      yearly_data <- forecast[!duplicated(format(forecast$ds, "%m-%d")), ]
      yearly_data <- yearly_data[order(format(yearly_data$ds, "%m-%d")), ]
      
      yearly_plot <- plot_ly(data = yearly_data, x = ~ds, y = ~yearly, 
                             type = "scatter", mode = "lines", name = "Yearly") %>%
        layout(title = "Yearly Seasonality", showlegend = FALSE)
      plot_list[[length(plot_list) + 1]] <- yearly_plot
    }
    
    # Weekly seasonality if available
    if("weekly" %in% colnames(forecast) && sum(!is.na(forecast$weekly)) > 0) {
      weekly_data <- forecast[!duplicated(format(forecast$ds, "%u")), ]
      weekly_data <- weekly_data[order(format(weekly_data$ds, "%u")), ]
      
      weekly_plot <- plot_ly(data = weekly_data, x = ~ds, y = ~weekly, 
                             type = "scatter", mode = "lines", name = "Weekly") %>%
        layout(title = "Weekly Seasonality", showlegend = FALSE)
      plot_list[[length(plot_list) + 1]] <- weekly_plot
    }
    
    # Combine plots in a subplot
    subplot(plot_list, nrows = length(plot_list), shareX = TRUE) %>%
      layout(title = "Forecast Components",
             showlegend = FALSE)
  })
}

# Run the application
shinyApp(ui = ui, server = server)
