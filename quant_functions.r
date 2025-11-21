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
#' @return The input dataframe augmented with 'effective_buffer' and 'weight_buffer_vol' columns.
mq_apply_adaptive_trading_buffer <- function(df, vol_multiplier = 2.0) {
  
  # Ensure the data is sorted by date within each ticker group for proper sequential processing
  df_sorted <- df %>%
    arrange(ticker, date) %>%
    
    # Calculate the asset-specific, ABSOLUTE buffer size for each row
    mutate(
      effective_buffer = lag_volatility * vol_multiplier
    )
  
  # Initialize the final_weight column (will be filled sequentially)
  df_sorted$weight_buffer_vol <- NA_real_
  
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
          .x$weight_buffer_vol[i] <- current_weight
          last_transacted_weight <- current_weight
          
        } else {
          
          # 2. Absolute Buffer Check Logic
          weight_drift_abs <- abs(current_weight - last_transacted_weight)
          
          # Handle NA trade_buffer by forcing a trade (TRUE)
          is_outside_buffer <- is.na(trade_buffer) | (weight_drift_abs > trade_buffer)
          
          if (is_outside_buffer) {
            # Transaction occurs
            .x$weight_buffer_vol[i] <- current_weight
            last_transacted_weight <- current_weight
            
          } else {
            # No transaction
            .x$weight_buffer_vol[i] <- last_transacted_weight
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

#' Plot Ticker attributes with Free Y-Axis Scaling
#'
#' Creates a faceted line plot of time series price data, where each ticker
#' has its own panel and an independent (free) y-axis scale.
#'
#' @param data A data frame containing at least 'date', 'ticker', and the
#'             column specified by 'y_col'.
#' @param y_col A string specifying the column name for the Y-axis (e.g., "adjusted", "close").
#'              Defaults to "adjusted".
#' @return A ggplot object.
#' @examples
#' # Example 1: Plotting the default 'adjusted' column
#' mq_plot_ticker_attribute(mock_data)
#'
#' # Example 2: Plotting the 'close' column
#' mq_plot_ticker_attribute(mock_data, y_col = "close")
mq_plot_ticker_attribute <- function(data, y_col = "adjusted") {

  # Use !!sym() from rlang to convert the string in y_col into a valid
  # column name for ggplot's aesthetic mapping.
  y_var <- sym(y_col)
  
  # Capitalize the Y-axis label for the title
  y_label <- paste0(toupper(substring(y_col, 1, 1)), substring(y_col, 2))

  p <- data %>%
    ggplot(aes(x = date, y = !!y_var, color = ticker)) +
    geom_line(linewidth = 1) +

    # Faceting is the key to separate Y-axes.
    # scales = "free_y" ensures each ticker gets its own vertical scale.
    facet_wrap(~ticker, scales = "free_y", ncol = 1, strip.position = "right") +

    # Customize the labels, dynamically using the y_col name
    labs(
      title = paste("Ticker Comparison (Faceted with Free Y-Axis Scale)"),
      subtitle = paste0("Analysis of ", y_label, " Price"),
      x = "Date",
      y = paste0(y_label, " Price"),
      color = "Ticker" # Legend title
    ) +

    theme_minimal(base_size = 14) +
    theme(
      plot.title = element_text(face = "bold", hjust = 0.5),
      strip.text.y.right = element_text(angle = 0, face = "bold"), # Facet labels
      panel.spacing = unit(1, "lines"), # Spacing between the facets
      legend.position = "none" # Hide the legend since colors are duplicated in facets
    )

  return(p)
}

#' Add Annualized Rolling Volatility to Price Data
#'
#' Calculates the daily log returns and then computes the annualized rolling
#' standard deviation (volatility) over a specified window. It also creates a
#' lagged volatility column for forecasting or risk modeling purposes.
#'
#' @param data A data frame containing at least 'date', 'ticker', and the
#'             'adjusted' price column.
#' @param roll_days The number of trading days to use for the rolling standard
#'                  deviation calculation. Defaults to 60 days (approx. 3 months).
#' @return The original data frame with three new columns:
#'         - `log_return`: The daily log return.
#'         - `volatility`: The annualized rolling standard deviation.
#'         - `lag_volatility`: The previous day's volatility.
#' @examples
#' # --- Mock Data Setup for Example ---
#' set.seed(42)
#' dates <- seq(as.Date("2023-01-01"), by = "day", length.out = 100)
#'
#' # Create two tickers with slightly different behavior
#' mock_data <- tibble(
#'   date = rep(dates, 2),
#'   ticker = rep(c("AAPL", "GOOG"), each = 100),
#'   adjusted = c(
#'     100 * cumprod(1 + rnorm(100, 0.001, 0.015)), # Ticker 1
#'     150 * cumprod(1 + rnorm(100, 0.0005, 0.01))  # Ticker 2
#'   )
#' )
#'
#' # Run the function with a 30-day rolling window
#' volatility_data <- mq_add_volatility(mock_data, roll_days = 30)
#'
#' # Inspect the first rows where volatility is calculated (after 30 days)
#' head(volatility_data)
mq_add_volatility <- function(data, roll_days = 60) {
  
  # Check if 'roll' library is loaded
  if (!"roll" %in% .packages()) {
    stop("The 'roll' package is required for high-speed rolling calculations. Please install and load it: install.packages('roll') and library(roll).")
  }
  
  data <- data %>%
    # 1. Group by ticker to ensure calculations are independent
    group_by(ticker) %>%
    
    # 2. Ensure data is ordered by date within each group
    arrange(date) %>%
    
    # 3. Calculate daily log returns (log(P_t / P_{t-1}))
    mutate(log_return = log(adjusted/dplyr::lag(adjusted))) %>%
    
    # 4. Remove the first NA value per ticker which results from the lag calculation
    na.omit() %>%
    
    # 5. Calculate rolling standard deviation, and annualize it by multiplying by sqrt(252)
    mutate(volatility = roll::roll_sd(log_return, roll_days) * sqrt(252)) %>%
    
    # 6. Create the lagged volatility column (the volatility measure from the previous day)
    mutate(lag_volatility = dplyr::lag(volatility)) %>%
    
    # Always a good practice to ungroup after grouped manipulations
    ungroup()

  return(data)
}

#' Add Volatility Targeting Weights
#'
#' Calculates the portfolio weight for each ticker on a given day based on a
#' fixed, per-ticker target volatility and the previous day's realized volatility.
#' The resulting weight is the factor needed to scale the current position to
#' achieve the desired risk level: Weight = Target_Vol / Lagged_Realized_Vol.
#'
#' @param data A data frame containing 'ticker' and 'lag_volatility' columns.
#'             (Typically the output of mq_add_volatility).
#' @param target_vols A named numeric vector where names are ticker symbols
#'                    and values are the desired annual volatility targets (e.g., 0.05).
#' @return The original data frame with a new column `weight_vol_target`.
#' @examples
#' # 2. Define Target Volatilities
#' target_vol_ticker <- c(AAPL = 0.15, GOOG = 0.10)
#'
#' # 3. Calculate Weights
#' weighted_data <- mq_add_vol_targeting_weights(vol_data, target_vol_ticker)
#'
mq_add_vol_targeting_weights <- function(data, target_vols) {
  
  # Ensure the target_vols vector is named correctly
  if (is.null(names(target_vols))) {
    stop("The 'target_vols' parameter must be a named vector (e.g., c(TICKER1 = 0.05, ...)).")
  }
  
  # Perform the calculation. R's vector matching automatically looks up the
  # target volatility based on the 'ticker' column name.
  data <- data %>%
    mutate(
      target_vol = target_vols[ticker],
      weight_rp = target_vol / lag_volatility
    ) %>%
    # Clean up the temporary target_vol column
    select(-target_vol)
  
  return(data)
}

#' Calculate Log Returns Grouped by Trading Day of the Month (TDM)
#'
#' Filters price data for specified stock and bond ETFs, removes months
#' with fewer than 15 trading days (to ensure complete monthly cycles),
#' calculates the Trading Day of the Month (TDM), and computes the daily
#' log return.
#'
#' @param prices A data frame containing at least 'date', 'ticker', and 'close'
#'               columns.
#' @param stock_ticker The ticker symbol for the stock ETF (default: 'SXR8.DE').
#' @param bond_ticker The ticker symbol for the bond ETF (default: 'PRAS.DE').
#' @return A data frame containing 'date', 'ticker', 'tdm' (Trading Day of the Month),
#'         and 'log_return', filtered for complete months and the specified tickers.
#' @examples
#' tdm_returns <- mq_get_return_by_tdm(prices_data_example)
#' head(tdm_returns)
mq_get_return_by_tdm <- function(prices, stock_ticker = 'SXR8.DE', bond_ticker = 'PRAS.DE') {
  
  # 1. Filter, extract date components, and ensure arrangement
  flow_prices <- prices %>%
    dplyr::filter(ticker %in% c(stock_ticker, bond_ticker)) %>%
    mutate(
      year = lubridate::year(date),
      month = lubridate::month(date)
    ) %>%
    arrange(date)
    
  # 2. Identify and flag incomplete months (fewer than 15 trading days)
  incomplete_months <- flow_prices %>%
    group_by(ticker, year, month) %>%
    summarise(trading_days = n(), .groups = "drop") %>%
    filter(trading_days < 15)
  
  # 3. Filter out incomplete months and calculate Trading Day of the Month (TDM)
  flow_prices <- flow_prices %>%
    anti_join(incomplete_months, by = c("ticker", "year", "month")) %>%
    group_by(ticker, year, month) %>%
    mutate(tdm = row_number()) %>%
    ungroup()
  
  # 4. Calculate log returns
  # Grouping by ticker ensures returns are only calculated within the same series.
  final_data <- flow_prices %>%
    group_by(ticker) %>%
    arrange(date) %>%
    mutate(log_return = log(close / dplyr::lag(close, n = 1))) %>%
    na.omit() %>%
    ungroup() %>%
    
    # 5. Select final output columns
    select(date, ticker, tdm, log_return)
  
  return(final_data)
}



#' Calculate Equity-Bond Outperformance Split by Trading Day of the Month (TDM)
#'
#' Takes the TDM-calculated log returns and determines the equity (stock)
#' outperformance over the bond ticker, splitting the month into two parts
#' defined by a cutoff trading day.
#'
#' @param tdm_returns A data frame, typically the output of mq_get_return_by_tdm,
#'                    containing 'date', 'ticker', 'tdm', and 'log_return'.
#' @param cutoff_tdm The trading day of the month (TDM) used to split the month.
#'                   (Default: 14)
#' @param stock_ticker The ticker symbol for the stock ETF (default: 'SXR8.DE').
#' @param bond_ticker The ticker symbol for the bond ETF (default: 'PRAS.DE').
#' @return A data frame with 'year', 'month', and 'equity_outperformance' (which
#'         corresponds to the first part of the month's outperformance).
mq_get_stock_bond_outperformance <- function(tdm_returns, cutoff_tdm = 14, stock_ticker = 'SXR8.DE', bond_ticker = 'PRAS.DE') {
  
  # Ensure the necessary tickers are present as column names for the final calculation
  # by dynamically assigning variable names.
  stock_sym <- sym(stock_ticker)
  bond_sym <- sym(bond_ticker)
  
  flow_df <- tdm_returns %>%
    # 1. Widen data to compare log_returns side-by-side for simultaneous days
    pivot_wider(id_cols = c(date, tdm), names_from = ticker, values_from = log_return) %>%
    na.omit() %>%
    
    # 2. Pivot back to long format (This step is often for preparation if more
    #    columns were involved, but we proceed as per the pipeline structure)
    pivot_longer(
      cols = c(!!stock_sym, !!bond_sym), 
      names_to = "ticker", 
      values_to = "log_return"
    ) %>%
    
    # 3. Create month split and date components
    mutate(
      month_split = case_when(tdm <= cutoff_tdm ~ 1, TRUE ~ 2),
      month = lubridate::month(date),
      year = lubridate::year(date)
    ) %>%
    
    # 4. Sum log returns for each part of the month (which is equivalent to
    #    multiplying simple returns, but we summarize in log space first)
    group_by(ticker, year, month, month_split) %>%
    summarise(partial_return = sum(log_return, na.rm = TRUE), .groups = "drop") %>%
    
    # 5. Pivot wide again to get partial returns for stock and bond side-by-side
    pivot_wider(names_from = ticker, values_from = partial_return) %>%
    
    # 6. Calculate equity-bond outperformance: log(1 + (R_stock - R_bond))
    #    R = exp(log_return) - 1
    mutate(
      eq_bond_outperf = log(
        1 + (exp(!!stock_sym) - 1) - (exp(!!bond_sym) - 1)
      )
    ) %>%
    
    # 7. Pivot wide one final time to separate the two monthly parts
    pivot_wider(
      id_cols = c(year, month), 
      names_from = month_split, 
      names_prefix = "part_mnth_", 
      values_from = eq_bond_outperf
    ) %>%
    
    # 8. Rename the first part to the final output column name
    rename("equity_outperformance" = part_mnth_1) %>%
    
    # 9. Select the final columns (part_mnth_2 contains the second half, 
    #    which is implicitly dropped by the select unless you need it)
    select(year, month, equity_outperformance)
  
  return(flow_df)
}

#' Plots Strategy Cumulative Returns and Rolling Volatility
#'
#' Takes a data frame containing daily positions and log returns, calculates
#' the strategy return, rolling volatility, and cumulative returns, then
#' generates a combined plot of performance and risk.
#'
#' @param flow_positions A data frame containing at least 'date', 'position',
#'                       and 'log_return' columns, where 'position' is the
#'                       weight allocated to the asset and 'log_return' is the
#'                       asset's log return for that day.
#' @param window The rolling window size for volatility calculation (default: 60 days).
#' @param annual_factor The annualization factor for volatility (default: 252 trading days).
#' @return A combined plot object (patchwork class) showing cumulative returns
#'         and annualized rolling volatility.
mq_plot_return_vol <- function(flow_positions, window = 60, annual_factor = 252) {
  
  # Ensure the necessary package is available for the core calculation
  if (!requireNamespace("roll", quietly = TRUE)) {
    stop("The 'roll' package is required for rolling standard deviation calculation. Please install it with install.packages('roll').")
  }
  
  # 1. Calculate Strategy Returns and Rolling Volatility
  flow_strategy <- flow_positions %>%
    # Ensure date is properly grouped for daily strategy return calculation
    group_by(date) %>%
    # Calculate daily strategy log return: sum(position * asset_log_return)
    summarise(strat_ret = sum(position * log_return, na.rm = TRUE), .groups = "drop") %>%
    
    # Calculate rolling volatility: SD over 'window' days, then annualized
    mutate(
      # FIX: Changed 'n = window' to 'width = window' to match the roll::roll_sd argument name.
      volatility = roll::roll_sd(strat_ret, width = window) * sqrt(annual_factor)
    )

  # 2. Prepare Data and Create Cumulative Return Plot
  return_plot <- flow_strategy %>%
    mutate(cum_ret = cumsum(strat_ret)) %>%
    ggplot(aes(x = date, y = cum_ret)) +
    geom_line(color = "#1D4ED8", linewidth = 0.8) + # Tailwind blue-700 for returns
    labs(
      title = "Cumulative Strategy Log Returns",
      y = "Cumulative Log Return",
      x = NULL
    ) +
    theme_minimal(base_size = 14) +
    theme(
      plot.title = element_text(face = "bold", hjust = 0),
      axis.title.y = element_text(margin = margin(r = 10)),
      panel.grid.minor = element_blank()
    )

  # 3. Create Rolling Volatility Plot
  vol_plot <- flow_strategy %>%
    # Volatility is already annualized
    ggplot(aes(x = date, y = volatility)) +
    geom_line(color = "#B91C1C", linewidth = 0.8) + # Tailwind red-700 for risk
    labs(
      title = "Annualized Rolling Volatility",
      y = "Annualized Volatility",
      x = "Date"
    ) +
    theme_minimal(base_size = 14) +
    theme(
      plot.title = element_text(face = "bold", hjust = 0),
      axis.title.y = element_text(margin = margin(r = 10)),
      panel.grid.minor = element_blank()
    )

  # 4. Combine Plots using patchwork
  combined_plot <- return_plot / vol_plot + 
    plot_layout(heights = c(2, 1)) +
    plot_annotation(
      title = glue::glue("Strategy Performance: Cumulative Returns and Rolling Volatility"),
      caption = glue::glue("Annualized volatility calculated over a {window}-day rolling window."),
      theme = theme(plot.title = element_text(size = 18, face = "bold", hjust = 0.5))
    )
  
  return(combined_plot)
}



#' Applies a no-trading buffer to a series of target weights.
#'
#' The final weight only changes if the initial weight deviates from the last
#' transacted weight by more than the specified buffer percentage.
#'
#' @param df A dataframe containing columns 'date', 'ticker', and 'weight'.
#' @param trade_buffer The percentage buffer (e.g., 0.01 for 1%).
#'
#' @return The input dataframe augmented with a 'weight_buffer_weight' column.
mq_apply_trading_buffer_weight <- function(df, trade_buffer) {

  # Ensure the data is sorted by date within each ticker group for proper sequential processing
  df_sorted <- df %>%
    arrange(ticker, date)

  # Initialize the final_weight column (will be filled sequentially)
  df_sorted$weight_buffer_weight <- NA_real_

  # Use group_split and map to process each ticker's series independently
  # We use a loop structure within purrr::map to handle the sequential update logic.
  df_output <- df_sorted %>%
    group_by(ticker) %>%
    group_modify(~ {

      # Initialize tracking variable for the last weight that triggered a trade
      last_transacted_weight <- NA_real_

      # Process rows sequentially for the current ticker group
      for (i in 1:nrow(.x)) {
        # --- READING THE USER'S WEIGHT COLUMN ---
        current_weight <- .x$weight[i]

        # --- 1. First Day Logic (Always transacts) or Zero Weight ---
        if (is.na(last_transacted_weight) || last_transacted_weight == 0) {

          # The first valid weight becomes the starting transacted weight
          .x$weight_buffer_weight[i] <- current_weight
          last_transacted_weight <- current_weight

        } else {

          # --- 2. Buffer Check Logic ---

          # Calculate the upper and lower bounds based on the last transacted weight
          lower_bound <- last_transacted_weight * (1 - trade_buffer)
          upper_bound <- last_transacted_weight * (1 + trade_buffer)

          # Check if the current weight is OUTSIDE the buffer bounds
          is_outside_buffer <- current_weight < lower_bound || current_weight > upper_bound

          if (is_outside_buffer) {
            # Weight is outside the buffer -> TRANSACTION OCCURS
            .x$weight_buffer_weight[i] <- current_weight
            # Update the last transacted weight for the next day's check
            last_transacted_weight <- current_weight

          } else {
            # Weight is INSIDE the buffer -> NO TRANSACTION
            # The final weight remains the previous transacted weight
            .x$weight_buffer_weight[i] <- last_transacted_weight
          }
        }
      }
      # Return the modified group data frame
      return(.x)
    }) %>%
    ungroup()

  return(df_output)
}


#' Downloads and processes the 13-week T-Bill interest rate, aligning it 
#' perfectly with the dates present in the input portfolio data frame (port_df).
#'
#' @param port_df A tibble/data.frame containing a column named 'date' (as Date type).
#' @return A matrix with two columns: date (numeric) and rate (numeric daily risk-free rate).
mq_get_tbill_rate_matrix <- function(port_df) {
  
  # Check for 'date' column in the input data frame
  if (!"date" %in% names(port_df)) {
    stop("Input 'port_df' must contain a column named 'date'.")
  }
  
  # 1. Determine the required date range from port_df
  start_date <- as.character(min(port_df$date, na.rm = TRUE))
  end_date <- as.character(max(port_df$date, na.rm = TRUE))

  
  # 2. Download T-Bill data (^IRX)
  tryCatch({
    # Download data covering the portfolio's date range
    getSymbols("^IRX", auto.assign = TRUE)
  }, error = function(e) {
    stop(paste("Failed to download ^IRX data. Error:", e$message))
  })
  
  # Ensure IRX object was created
  if (!exists("IRX") || !is.xts(IRX)) {
     stop("^IRX data could not be retrieved by quantmod::getSymbols.")
  }

  # 3. Process T-Bill data
  rates_df <- mq_xts_to_tidy(IRX) %>%
    dplyr::arrange(date) %>%
    dplyr::select(date, close) %>%
    # Ensure rate is numeric and handle any NA values by carrying the last observation forward
    dplyr::mutate(
      dplyr::across(.cols = close, .fns = as.numeric),
      rate = close / (365 * 100) # Convert annual percentage to daily decimal rate
    ) %>%
    tidyr::fill(rate, .direction = "down") %>%
    dplyr::select(date, rate)

  # 4. Join rates data to port_df and finalize the matrix
  risk_free_rate_matrix <- port_df %>%
    # Select only the dates from the portfolio to use as the base for the join
    dplyr::select(date) %>% 
    # Left join to attach the calculated rate for each portfolio date
    dplyr::left_join(rates_df, by = "date") %>%
    # Fill any remaining NA rates (if a market day falls on a non-rate day)
    tidyr::fill(rate, .direction = "down") %>%
    # Handle the very first date if it was NA (rare, but prevents issues)
    tidyr::fill(rate, .direction = "up") %>% 
    
    # Final step: select date and rate, and convert to matrix
    # The 'date' column is converted to its underlying numeric value (days since epoch)
    data.matrix() 
  
  return(risk_free_rate_matrix)
}

# R function to calculate Sharpe Ratio (assuming risk-free rate of zero)
# and using log returns for volatility and mean return calculation.

# This function requires the 'dplyr' and 'tidyr' packages.
# library(dplyr)
# library(tidyr)

mq_get_backtest_sharpe <- function(backtest_results) {
  
  # Calculate the daily Net Asset Value (NAV) exposure
  NAV <- backtest_results %>%
    dplyr::group_by(date) %>%
    dplyr::summarise(exposure = sum(exposure, na.rm = TRUE)) %>%
    dplyr::mutate(ticker = "NAV")

  # Combine NAV with the original data for processing (though only NAV is filtered next)
  NAV_combined <- backtest_results %>%
    dplyr::select(ticker, date, exposure) %>%
    dplyr::bind_rows(NAV) %>%
    dplyr::arrange(date, ticker)

  # Calculate performance metrics
  perf <- NAV_combined %>%
    dplyr::filter(ticker == "NAV") %>%
    dplyr::arrange(date) %>%
    
    # Calculate daily log returns
    dplyr::mutate(returns = log(exposure/dplyr::lag(exposure))) %>%
    stats::na.omit() %>%
    
    # Calculate annualized metrics (assuming 252 trading days)
    dplyr::summarise(
      `Ave.Ann.Return %` = round(100 * mean(returns, na.rm = TRUE) * 252, 2),
      `Ann.Volatility %` = round(100 * stats::sd(returns, na.rm = TRUE) * sqrt(252), 2),
      # Sharpe Ratio = Annual Return / Annual Volatility (assuming R_f = 0)
      Sharpe = round(`Ave.Ann.Return %` / `Ann.Volatility %`, 2)
    )

    # Return the Sharpe Ratio value
    perf %>% dplyr::pull(Sharpe)

}