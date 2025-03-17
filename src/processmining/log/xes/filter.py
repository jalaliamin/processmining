def add_start_end_events(df, case_col, activity_col, timestamp_col, lifecycle_col=None):
    import pandas as pd
    # Sort the data frame by case identifier and timestamp
    df = df.sort_values(by=[case_col, timestamp_col]).copy()
    
    # Identify the starting and ending rows for each group
    starting_rows = df.groupby(case_col).apply(lambda x: x.iloc[0])
    ending_rows = df.groupby(case_col).apply(lambda x: x.iloc[-1])
    
    # Calculate the starting and ending times for each group
    starting_times = starting_rows[timestamp_col] - pd.Timedelta(minutes=1)
    ending_times = ending_rows[timestamp_col] + pd.Timedelta(minutes=1)
    
    # Create the beginning and ending rows    
    beginning_rows = starting_rows.copy()
    beginning_rows[activity_col] = 'BEGIN'
    beginning_rows[timestamp_col] = starting_times
      
    ending_rows = ending_rows.copy()
    ending_rows[activity_col] = 'END'
    ending_rows[timestamp_col] = ending_times

    if lifecycle_col is not None: 
      # Identify the starting lifecycles
      lifecycles = df[lifecycle_col].unique()
      for cycle in lifecycles:
        beginning_rows[lifecycle_col] = cycle
        ending_rows[lifecycle_col] = cycle
      
      
      # Add the beginning and ending rows to the data frame
      df = pd.concat([df, beginning_rows, ending_rows], ignore_index=True)
    
    # Sort the data frame again by case identifier and timestamp
    df = df.sort_values(by=[case_col, timestamp_col])
    
    return df

