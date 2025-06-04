#!/usr/bin/env Rscript

# Load necessary libraries
library(ggplot2)
library(dplyr)
library(optparse)

# Define command-line options
option_list = list(
  make_option(c("-o", "--out"), type = "character", default = ".", 
              help = "Output path for plots"),
  make_option(c("-p", "--path"), type = "character", default = ".", 
              help = "Path to metrics files"),
  
  make_option(c("-f", "--filepattern"), type = "character", default = "*.log", 
              help = "File pattern to match metrics files")
)

# Parse command-line options
opt_parser = OptionParser(option_list = option_list)
opt = parse_args(opt_parser)

metrics_files <- list.files(opt$path, pattern = opt$filepattern, full.names = TRUE)

print(paste("Found", length(metrics_files), "metrics files"))

# Prepare file paths
metrics_data <- list()

# Read and process each file
for (file in metrics_files) {
  if (!file.exists(file)) {
    warning("File not found: ", file)
    next
  }

  # Read and clean data
  df <- read.table(file, header = FALSE, skip = 3, 
                   col.names = c("Time", "UID", "PID", "%usr", "%system", "%guest", "%wait", "%CPU", 
                                 "CPU", "minflt/s", "majflt/s", "VSZ", "RSS", "%MEM", 
                                 "kB_rd_s", "kB_wr_s", "kB_ccwr_s", "iodelay", "command"),
                   stringsAsFactors = FALSE)
  
  # Convert numeric columns and handle NA values
  df[2:(ncol(df) - 1)] <- lapply(df[2:(ncol(df) - 1)], function(x) as.numeric(as.character(x)))
  df[is.na(df)] <- 0
  # Convert RSS kiB to MiB
  df["RSS"] <- df["RSS"] / 1024
  
  metrics_data[[basename(file)]] <- df
}

# Verify data is loaded
if (length(metrics_data) < 1) {
  stop("No valid data loaded. Check file paths or contents.", call. = FALSE)
}

# Define function to create and save plots
create_plot <- function(data_list, y_column, title, y_label, file_suffix, max_y) {
  plot <- ggplot() +
    lapply(names(data_list), function(node) {
      geom_line(data = data_list[[node]], aes(x = 1:nrow(data_list[[node]]), y = !!sym(y_column), color = node))
    }) +
    labs(title = title, x = "Time", y = y_label) +
    ylim(c(0, max_y))

  # Save plot
  ggsave(filename = file.path(opt$out, paste0(file_suffix, ".png")), plot = plot, width = 8, height = 6)
}

print("Creating plots")
# Generate and save all required plots
create_plot(metrics_data, "X.CPU", "CPU Usage Over Time", "% CPU", "cpu_usage_plot", 150)
create_plot(metrics_data, "X.MEM", "Memory Usage Over Time", "% MEM", "mem_usage_plot", 15)
create_plot(metrics_data, "RSS", "Resident Set Size in MiB Over Time", "RSS", "rss_plot", 500)
create_plot(metrics_data, "kB_rd_s", "IO Read Usage Over Time", "kB_rd_s", "io_read_usage_plot", 80)
create_plot(metrics_data, "kB_wr_s", "IO Write Usage Over Time", "kB_wr_s", "io_write_usage_plot", 25000)
