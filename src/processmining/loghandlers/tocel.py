import pandas as pd
import numpy as np

import json
import zipfile

from datetime import datetime, timedelta
# from typing import Optional
import io

from processmining.loghandlers.constants import *



class TOCEL:
    def __init__(self, events=None, objects=None, e2o=None, o2o=None):
        self.events = events.copy(deep=True) if events is not None else None
        self.objects = objects.copy(deep=True) if objects is not None else None
        self.e2o = e2o.copy(deep=True) if e2o is not None else None
        self.o2o = o2o.copy(deep=True) if o2o is not None else None
        
    def save_to_tocelfile(self, file_path):
        dataframes = {
            lbl_csv_events: self.events.copy(deep=True) if self.events is not None else None,
            lbl_csv_objects: self.objects.copy(deep=True) if self.objects is not None else None,
            lbl_csv_e2o: self.e2o.copy(deep=True) if self.e2o is not None else None,
            lbl_csv_o2o: self.o2o.copy(deep=True) if self.o2o is not None else None
        }

        # Function to format datetime columns
        def format_datetime_columns(df):
            for col in df.select_dtypes(include=['datetime64[ns]']).columns:
                df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')  
            return df
            
        with zipfile.ZipFile(file_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for filename, dataframe in dataframes.items():
                if dataframe is not None:  
                    dataframe = format_datetime_columns(dataframe) 
                    with io.BytesIO() as buffer:
                        dataframe.to_csv(buffer, index=False)
                        zf.writestr(filename, buffer.getvalue())
        
    @classmethod
    def load_from_tocelfile(cls, zip_file_path):
        events, objects, e2o, o2o = None, None, None, None

        dataframe_mapping = {
            lbl_csv_events: "events",
            lbl_csv_objects: "objects",
            lbl_csv_e2o: "e2o",
            lbl_csv_o2o: "o2o"
        }

        with zipfile.ZipFile(zip_file_path, 'r') as zf:
            for filename in zf.namelist():
                if filename in dataframe_mapping:
                    with zf.open(filename) as file:
                        df = pd.read_csv(file)
                        if filename == lbl_csv_events:
                            events = df
                        elif filename == lbl_csv_objects:
                            objects = df
                        elif filename == lbl_csv_e2o:
                            e2o = df
                        elif filename == lbl_csv_o2o:
                            o2o = df
        
        # Time conversion
        df_dts = [events, objects, e2o, o2o]
        cols_timestamps = [lbl_timestamp, lbl_timestamp_validfrom, lbl_timestamp_validto]
        for df in df_dts:
            if df is not None and not df.empty:
                for col in cols_timestamps:
                    if col in df.columns:
                        df[col] = pd.to_datetime(
                            df[col], 
                            format='%Y-%m-%dT%H:%M:%S.%fZ'  # Match the given timestamp format
                        )
                
        return cls(events, objects, e2o, o2o)

    @classmethod
    def cast_from_ocel(cls, ocel):
        events = ocel.events.copy(deep=True) if ocel.events is not None else None
        objects = ocel.objects.copy(deep=True) if ocel.objects is not None else None
        e2o = ocel.e2o.copy(deep=True) if ocel.e2o is not None else None
        o2o = ocel.o2o.copy(deep=True) if ocel.o2o is not None else None
        object_changes = ocel.object_changes.copy(deep=True) if ocel.object_changes is not None else None

        if objects is not None and not objects.empty :
            melted_changes = (
                object_changes.melt(
                    id_vars=[lbl_object_id, lbl_object_type, lbl_object_attribute_changed_name, lbl_timestamp],
                    value_vars=list(set(ocel.objects.columns) - {lbl_event_id, lbl_object_id, lbl_target_object_id, lbl_object_type, lbl_qual, lbl_event_type, lbl_timestamp}), 
                    var_name='field',
                    value_name='value'
                )
                .dropna(subset=['value'])
                .sort_values(by=[lbl_object_id, 'field', lbl_timestamp])
            )
            
            pivoted_dataframe = (
                melted_changes.pivot_table(
                    index=[lbl_object_id, lbl_object_type, lbl_timestamp], 
                    columns='field', 
                    values='value', 
                    aggfunc='last'
                )
                .reset_index()
                .sort_values(by=[lbl_object_id, lbl_object_type, lbl_timestamp])
            )
            
            # Forward-fill missing fields grouped by lbl_object_id
            fields_to_fill = [col for col in pivoted_dataframe.columns if col not in [lbl_object_id, lbl_object_type, lbl_timestamp]]
            pivoted_dataframe[fields_to_fill] = pivoted_dataframe.groupby(lbl_object_id)[fields_to_fill].ffill()
            
            # Add 'objectsnapshot' column that increments for each row within each group of lbl_object_id
            pivoted_dataframe[lbl_object_snapshot_id] = (
                pivoted_dataframe[lbl_object_id].astype(str) + ':' + 
                pivoted_dataframe.groupby(lbl_object_id).cumcount().add(1).astype(str)
            )
            
            # Rename and calculate validity columns
            pivoted_dataframe.rename(columns={lbl_timestamp: lbl_timestamp_validfrom}, inplace=True)
            pivoted_dataframe[lbl_timestamp_validto] = pivoted_dataframe.groupby(lbl_object_id)[lbl_timestamp_validfrom].shift(-1)- timedelta(seconds=1)
            
            pivoted_dataframe = pivoted_dataframe.rename_axis("index", axis="columns")
            
            # Create a default timestamp for rows without changes
            default_timestamp = pd.Timestamp("1970-01-01T00:00:00Z") 
            
            # Identify rows in `ocel.objects` that has no value for any attribute
            empty_objects = ocel.objects[[lbl_object_id, lbl_object_type]].drop_duplicates()
            empty_objects = empty_objects.loc[~empty_objects[lbl_object_id].isin(pivoted_dataframe[lbl_object_id])]
            
            default_rows = empty_objects
            default_rows[lbl_timestamp_validfrom] = default_timestamp
            default_rows[lbl_object_snapshot_id] = default_rows[lbl_object_id]
            
            # Combine the default rows with the pivoted dataframe
            pivoted_dataframe = pivoted_dataframe.dropna(axis=1, how='all')
            default_rows = default_rows.dropna(axis=1, how='all')
            
            # Perform concatenation
            final_dataframe = pd.concat([pivoted_dataframe, default_rows], ignore_index=True)
            # final_dataframe = pd.concat([pivoted_dataframe, default_rows], ignore_index=True)
            
            objects = final_dataframe

        # transforming e2o s
        if e2o is not None and not e2o.empty :
            # Joining the DataFrames on lbl_object_id
            merged_df = pd.merge(e2o, objects, on=[lbl_object_id, lbl_object_type], suffixes=('_e2o', '_objects'))

            merged_df[lbl_timestamp_validfrom] = pd.to_datetime(merged_df[lbl_timestamp_validfrom], utc=True)
            merged_df[lbl_timestamp] = pd.to_datetime(merged_df[lbl_timestamp], utc=True)
            # Filtering based on the conditions
            filtered_df = merged_df[
                (merged_df[lbl_timestamp_validfrom] <= merged_df[lbl_timestamp])
            ]
            
            # Identifying the maximum lbl_timestamp_validfrom for each lbl_object_id group
            idx = filtered_df.groupby([lbl_event_id, lbl_event_type, lbl_object_id, lbl_qual])[lbl_timestamp_validfrom].idxmax()
            
            # Filtering rows to keep only those with the maximum lbl_timestamp_validfrom per lbl_object_id
            result_df = filtered_df.loc[idx]
            
            result_df = result_df[list(ocel.e2o.columns)+[lbl_object_snapshot_id]]
            
            e2o = result_df

        # # transforming o2o s
        if o2o is not None and not o2o.empty:
            merged_df = pd.merge(o2o, objects, on=[lbl_object_id])[[lbl_object_id, lbl_target_object_id, lbl_qual, lbl_object_snapshot_id, lbl_timestamp_validfrom, lbl_timestamp_validto]]
            merged_df = merged_df.rename(columns={lbl_object_id: lbl_object_id + lbl_source_postfix, lbl_object_snapshot_id: lbl_object_snapshot_id + lbl_source_postfix, lbl_target_object_id: lbl_object_id + lbl_target_postfix,  lbl_timestamp_validfrom: lbl_timestamp_validfrom + lbl_source_postfix, lbl_timestamp_validto: lbl_timestamp_validto + lbl_source_postfix})
            
            merged_df = pd.merge(merged_df, objects, left_on=[lbl_object_id + lbl_target_postfix], right_on=[lbl_object_id])
            merged_df = merged_df.rename(columns={lbl_object_snapshot_id: lbl_object_snapshot_id + lbl_target_postfix, lbl_timestamp_validfrom: lbl_timestamp_validfrom + lbl_target_postfix, lbl_timestamp_validto: lbl_timestamp_validto + lbl_target_postfix})
            
            merged_df = merged_df[[lbl_object_id + lbl_source_postfix, lbl_object_snapshot_id + lbl_source_postfix, lbl_timestamp_validfrom + lbl_source_postfix, lbl_timestamp_validto + lbl_source_postfix, lbl_object_id + lbl_target_postfix, lbl_object_snapshot_id + lbl_target_postfix, lbl_timestamp_validfrom + lbl_target_postfix, lbl_timestamp_validto + lbl_target_postfix, lbl_qual]]
            
            # correcting timezones
            merged_df[lbl_timestamp_validfrom + lbl_source_postfix] = pd.to_datetime(merged_df[lbl_timestamp_validfrom + lbl_source_postfix], utc=True)
            merged_df[lbl_timestamp_validto + lbl_source_postfix] = pd.to_datetime(merged_df[lbl_timestamp_validto + lbl_source_postfix], utc=True)
            merged_df[lbl_timestamp_validfrom + lbl_target_postfix] = pd.to_datetime(merged_df[lbl_timestamp_validfrom + lbl_target_postfix], utc=True)
            merged_df[lbl_timestamp_validto + lbl_target_postfix] = pd.to_datetime(merged_df[lbl_timestamp_validto + lbl_target_postfix], utc=True)
            
            # defining rules to relate o2os on snapshot level
            # 1. we assume objects can be connecetd if they co-exists (have to make such assumtpuon as they are not in ocel
            merged_df = merged_df[
                (
                    (merged_df[lbl_timestamp_validfrom + lbl_source_postfix] <= merged_df[lbl_timestamp_validto + lbl_target_postfix]) |
                    merged_df[lbl_timestamp_validto + lbl_target_postfix].isna()
                ) &
                (
                    (merged_df[lbl_timestamp_validto + lbl_source_postfix] >= merged_df[lbl_timestamp_validfrom + lbl_target_postfix]) |
                    merged_df[lbl_timestamp_validto + lbl_source_postfix].isna()
                )
            ]
            
            # for coexisted snapshots, we assume such relation in valid_from & to for relation: 
            merged_df[lbl_timestamp_validfrom] = merged_df[[lbl_timestamp_validfrom + lbl_source_postfix, lbl_timestamp_validfrom + lbl_target_postfix]].max(axis=1)
            
            merged_df = merged_df.sort_values(
                by=[lbl_object_id + lbl_source_postfix, lbl_object_id + lbl_target_postfix, lbl_qual, lbl_timestamp_validfrom]
            )
            merged_df[lbl_timestamp_validto] = (
                merged_df.groupby([lbl_object_id + lbl_source_postfix, lbl_object_id + lbl_target_postfix, lbl_qual])[lbl_timestamp_validfrom]
                .shift(-1) - pd.Timedelta(microseconds=1)  # Subtract 1 second from the next valid_from
            )
            
            merged_df = merged_df[[lbl_object_id + lbl_source_postfix, lbl_object_snapshot_id + lbl_source_postfix, lbl_object_id + lbl_target_postfix, lbl_object_snapshot_id + lbl_target_postfix, lbl_qual, lbl_timestamp_validfrom, lbl_timestamp_validto]]
            merged_df = merged_df.rename(columns={lbl_object_id + lbl_source_postfix: lbl_object_id, lbl_object_snapshot_id + lbl_source_postfix: lbl_object_snapshot_id, lbl_object_id + lbl_target_postfix: lbl_target_object_id, lbl_object_snapshot_id + lbl_target_postfix:lbl_target_object_snapshot_id})
            
            o2o = merged_df

        # Time conversion
        df_dts = [events, objects, e2o, o2o]
        cols_timestamps = [lbl_timestamp, lbl_timestamp_validfrom, lbl_timestamp_validto]
        
        for df in df_dts:
            if df is not None and not df.empty:
                for col in cols_timestamps:
                    if col in df.columns:
                        # Convert to datetime
                        df[col] = pd.to_datetime(
                            df[col], 
                            format='%Y-%m-%dT%H:%M:%S.%fZ',  # Match the given timestamp format
                            utc=True  # Handle tz-aware timestamps
                        )
                        # Remove timezone information
                        df[col] = df[col].dt.tz_localize(None)
            
        return cls(events, objects, e2o, o2o)

    def cast_to_ocel(self):
        events = objects = e2o = o2o = object_changes = None
        
        if self.events is not None and not self.events.empty:
            events = self.events

        if self.objects is not None and not self.objects.empty:
            aggs = {col:'last' for col in self.objects if col not in [lbl_object_snapshot_id, lbl_timestamp_validfrom, lbl_timestamp_validto]}
            objects = self.objects.copy().groupby(lbl_object_id, as_index=False).agg(aggs)

        if self.e2o is not None and not self.e2o.empty:
            e2o = self.e2o.copy().drop(columns=[lbl_object_snapshot_id])

        if self.o2o is not None and not self.o2o.empty:
            aggs = {col:'last' for col in self.o2o if col not in [ lbl_object_snapshot_id,lbl_target_object_snapshot_id, lbl_timestamp_validfrom, lbl_timestamp_validto]}
            o2o = self.o2o.copy().groupby([lbl_object_id, lbl_target_object_id, lbl_qual], as_index=False).agg(aggs)


        if self.objects is not None and not self.objects.empty:
            # Sort the dataframe
            df = self.objects.copy().sort_values(by=[lbl_object_id, lbl_timestamp_validfrom])
            df[lbl_timestamp] = df[lbl_timestamp_validfrom]
            df = df.drop(columns=[lbl_timestamp_validfrom, lbl_timestamp_validto, lbl_object_snapshot_id])
            
            # List of columns to track changes (all except the main columns)
            rest_columns = [col for col in df.columns if col not in [lbl_object_id, lbl_object_type, lbl_timestamp]]
            
            # Initialize a list to store rows of the new dataframe
            new_rows = []
            
            # Group by lbl_object_id
            for oid, group in df.groupby(lbl_object_id):
                group = group.sort_values(lbl_timestamp)
                prev_row = None
            
                # Explicitly handle the first row in the group
                first_row = group.iloc[0]
                for col in rest_columns:
                    if pd.notna(first_row[col]):  # Record non-NaN values
                        new_row = {key: np.nan for key in df.columns}  # Initialize NaN row
                        new_row[lbl_object_id] = first_row[lbl_object_id]
                        new_row[lbl_object_type] = first_row[lbl_object_type]
                        new_row[lbl_timestamp] = first_row[lbl_timestamp]
                        new_row[lbl_object_attribute_changed_name] = col
                        new_row[col] = first_row[col]  # Set value
                        new_rows.append(new_row)
                
                prev_row = first_row
            
                # Iterate over the rest of the rows in the group
                for idx, row in group.iloc[1:].iterrows():  # Skip first row (already handled)
                    for col in rest_columns:
                        # Compare current row with the previous row for changes
                        if row[col] != prev_row[col] and pd.notna(row[col]):  # Check if value changed and is not NaN
                            new_row = {key: np.nan for key in df.columns}  # Initialize NaN row
                            new_row[lbl_object_id] = row[lbl_object_id]
                            new_row[lbl_object_type] = row[lbl_object_type]
                            new_row[lbl_timestamp] = row[lbl_timestamp]
                            new_row[lbl_object_attribute_changed_name] = col
                            new_row[col] = row[col]  # Set new value
                            new_rows.append(new_row)
                    
                    # Update previous row
                    prev_row = row
            
            # Create the new DataFrame
            object_changes = pd.DataFrame(new_rows)

        from .ocel import OCEL
        return OCEL(events, objects, e2o, o2o, object_changes)