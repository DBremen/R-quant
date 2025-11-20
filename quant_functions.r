# --- Library Dependencies ---
# These packages must be installed in the Colab R environment
# before sourcing this file (we will include the installation step later).
library(tidyverse)
library(knitr)
library(scales)

# ==============================================================================
# FUNCTION 1: Adaptive Volatility Buffer
# ==============================================================================

#' Applies a no-trading buffer based on the asset's pre-calculated lagged volatility.
#'
#' The final weight only changes if the absolute difference between the initial 
#' target weight and the last transacted weight exceeds the adaptive buffer, which
#' is calculated as (Lagged Volatility * Multiplier).
#'
#' @param df A dataframe containing columns 'date', 'ticker', 'weight' (target), 
#'           and 'lag_volatility' (annualized 60-day volatility in decimal form).
#' @param vol_multiplier The factor by which volatility is multiplied to define the buffer width 
#'                       (e.g., 2.0, meaning a 2-sigma trade threshold).
#'
#' @return The input dataframe augmented with 'effective_buffer' and 'final_weight' columns.
mq_apply_adaptive_trading_buffer <- function(df, vol_multiplier = 2.0) {
  
  # Ensure the data is sorted by date within each ticker group for proper sequential processing
  df_sorted <- df %>%
    arrange(ticker, date) %>%
    
    # Calculate the asset-specific, ABSOLUTE buffer size for each row
    mutate(
      effective_buffer = lag_volatility * vol_multiplier
    )
  
  # Initialize the final_weight column (will be filled sequentially)
  df_sorted$final_weight <- NA_real_
  
  # Use group_split and group_modify to process each ticker's series independently
  df_output <- df_sorted %>%
    group_by(ticker) %>%
    group_modify(~ {
      
      # Initialize tracking variable for the last weight that triggered a trade
      last_transacted_weight <- NA_real_
      
      # Process rows sequentially for the current ticker group
      for (i in 1:nrow(.x)) {
        current_weight <- .x$weight[i]
        trade_buffer <- .x$effective_buffer[i] 
        
        # 1. First Day Logic (Always transacts)
        if (is.na(last_transacted_weight)) {
          .x$final_weight[i] <- current_weight
          last_transacted_weight <- current_weight
          
        } else {
          
          # 2. Absolute Buffer Check Logic
          weight_drift_abs <- abs(current_weight - last_transacted_weight)
          
          # Handle NA trade_buffer by forcing a trade (TRUE)
          is_outside_buffer <- is.na(trade_buffer) | (weight_drift_abs > trade_buffer)
          
          if (is_outside_buffer) {
            # Transaction occurs
            .x$final_weight[i] <- current_weight
            last_transacted_weight <- current_weight
            
          } else {
            # No transaction
            .x$final_weight[i] <- last_transacted_weight
          }
        }
      }
      return(.x)
    }) %>%
    ungroup()
  
  return(df_output)
}


mq_xts_to_tidy <- function(..., date_col = "date") {
  # Capture all XTS objects passed to the function
  xts_list <- list(...)

  # Get the names of the objects (tickers)
  obj_names <- as.character(substitute(list(...)))[-1]

  # If names weren't provided via named arguments, use the object names
  if (is.null(names(xts_list)) || all(names(xts_list) == "")) {
    names(xts_list) <- obj_names
  }

  # Convert each XTS object to a tibble and combine
  result <- map2_dfr(xts_list, names(xts_list), function(xts_obj, ticker) {
    # Extract the date index
    dates <- index(xts_obj)

    # Get the close and adjusted close columns
    close_col <- grep("\\.Close$", colnames(xts_obj), value = TRUE)
    adj_col <- grep("\\.Adjusted$", colnames(xts_obj), value = TRUE)

    if (length(close_col) == 0) {
      warning("No Close column found for ", ticker, ". Skipping.")
      return(NULL)
    }

    if (length(adj_col) == 0) {
      warning("No Adjusted column found for ", ticker, ". Skipping.")
      return(NULL)
    }

    # Create tibble with date, ticker, close, and adjusted close
    tibble(
      !!sym(date_col) := dates,
      ticker = ticker,
      close = as.numeric(xts_obj[, close_col]),
      adjusted = as.numeric(xts_obj[, adj_col])
    )
  })

  return(result)
}




mq_get_csv_Yahoo <- function(link) {
  #link <- 'https://docs.google.com/spreadsheets/d/1_Aa0upRaSAWAtnTuI3lkdNBXZ2dM3GouMiB2w09DFCY/export?format=csv'
  cat(paste("Downloading CSV from:", link, "\n"))

  new_data_raw <- tryCatch(
    {
      read.csv(link, header = TRUE, stringsAsFactors = FALSE)
    },
    error = function(e) {
      message("Error reading CSV from the provided link. Please ensure it's a direct CSV download link.")
      message("Original error: ", e$message)
      return(NULL)
    }
  )

  if (!is.null(new_data_raw)) {
    # Select, rename, and add ticker column
    processed_new_data <- new_data_raw %>%
      select(Date, close, adjclose) %>%
      rename(
        date = Date,
        close = close,
        adjusted = adjclose
      ) %>%
      mutate(ticker = "GLD.DE") %>%
      # Ensure date column is in Date format
      mutate(date = as.Date(date, format='%m/%d/%Y'))

    # Reorder columns to match prices_eu structure (date, ticker, close, adjusted)
    processed_new_data <- processed_new_data %>%
      select(date, ticker, close, adjusted)

    # Append to prices_eu
    # Ensure prices_eu is defined and accessible. It's defined in another cell.
    # This assumes prices_eu is in the global R environment.
    prices <- rbind(prices, processed_new_data)

  } else {
    cat("Failed to process CSV data from the link.\n")
  }
}

# ==============================================================================
# FUNCTION 2: Data Completeness Check
# ==============================================================================

#' Finds the earliest date where data exists for ALL unique tickers in the dataframe.
#'
#' This is useful for determining the start of a clean, complete backtesting history.
#'
#' @param df A dataframe containing at least 'date' and 'ticker' columns.
#' @return The earliest Date object (or POSIXct) on which all unique tickers are present.
mq_find_first_full_date <- function(df) {
  
  # 1. Count the total number of unique tickers in the entire dataset
  n_total_tickers <- df %>% 
    pull(ticker) %>% 
    n_distinct()
  
  # 2. Group by date and count how many unique tickers are present on that day
  full_dates <- df %>%
    group_by(date) %>%
    summarise(
      n_tickers_present = n_distinct(ticker),
      .groups = 'drop'
    ) %>%
    
    # 3. Filter for dates where the count matches the total required count
    filter(n_tickers_present == n_total_tickers) %>%
    
    # 4. Pull the dates and find the minimum (earliest) one
    pull(date)
  
  if (length(full_dates) > 0) {
    return(min(full_dates))
  } else {
    # If no date has all tickers, return NA
    warning("No date found where all tickers are present.")
    return(NA_Date_)
  }
}
