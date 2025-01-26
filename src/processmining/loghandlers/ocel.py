import pandas as pd
import numpy as np
import json
import zipfile
from datetime import datetime
from typing import Optional
import io

from processmining.loghandlers.constants import *

class OCEL:


    def __init__(self, events=None, objects=None, e2o=None, o2o=None, object_changes=None):
        self.events = events.copy(deep=True) if events is not None else None
        self.objects = objects.copy(deep=True) if objects is not None else None
        self.e2o = e2o.copy(deep=True) if e2o is not None else None
        self.o2o = o2o.copy(deep=True) if o2o is not None else None
        self.object_changes = object_changes.copy(deep=True) if object_changes is not None else None

    def save_to_ocelfile(self, file_path):
        # Dictionary to hold the DataFrames and their corresponding filenames
        dataframes = {
            lbl_csv_events: self.events.copy(deep=True) if self.events is not None else None,
            lbl_csv_objects: self.objects.copy(deep=True) if self.objects is not None else None,
            lbl_csv_e2o: self.e2o.copy(deep=True) if self.e2o is not None else None,
            lbl_csv_o2o: self.o2o.copy(deep=True) if self.o2o is not None else None,
            lbl_csv_object_changes: self.object_changes.copy(deep=True) if self.object_changes is not None else None
        }

        # Function to format datetime columns
        def format_datetime_columns(df):
            for col in df.select_dtypes(include=['datetime64[ns]']).columns:
                df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')  # Format datetime columns
            return df
            
        # Save the DataFrames as CSV files inside the zip file
        with zipfile.ZipFile(file_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for filename, dataframe in dataframes.items():
                if dataframe is not None:  # Check if the dataframe is not None
                    dataframe = format_datetime_columns(dataframe) 
                    with io.BytesIO() as buffer:
                        dataframe.to_csv(buffer, index=False)
                        zf.writestr(filename, buffer.getvalue())
                        
    
    def cast_to_pm4py(self):        
        import pm4py
        events = self.events.copy(deep=True) if self.events is not None else None
        objects = self.objects.copy(deep=True) if self.objects is not None else None
        e2o = self.e2o.copy(deep=True) if self.e2o is not None else None
        o2o = self.o2o.copy(deep=True) if self.o2o is not None else None
        object_changes = self.object_changes.copy(deep=True) if self.object_changes is not None else None
        
        return pm4py.objects.ocel.obj.OCEL(events, objects, e2o, None, None, o2o, None, object_changes)
        
    @classmethod
    def cast_from_pm4py(cls, pm4py_ocel):
        melted_df = pd.melt(
            pm4py_ocel.objects,
            id_vars=[lbl_object_id, lbl_object_type],
            var_name=lbl_object_attribute_changed_name,
            value_name='value'
        )
        melted_df = melted_df[melted_df['value'].notna()]
        
        rows = []
        for index, row in melted_df.iterrows():
            new_row = {col: np.nan for col in pm4py_ocel.objects.columns}
            new_row[lbl_object_id] = row[lbl_object_id]
            new_row[lbl_object_type] = row[lbl_object_type]
            new_row[lbl_object_attribute_changed_name] = row[lbl_object_attribute_changed_name]
            new_row[row[lbl_object_attribute_changed_name]] = row['value']
            rows.append(new_row)
        
        melted_df = pd.DataFrame(rows)
        
        common_rows = pd.merge(melted_df, pm4py_ocel.object_changes, on=[lbl_object_id, lbl_object_type, lbl_object_attribute_changed_name], how='inner')
        filtered_df1 = melted_df[~melted_df.set_index([lbl_object_id, lbl_object_type, lbl_object_attribute_changed_name]).index.isin(common_rows.set_index([lbl_object_id, lbl_object_type, lbl_object_attribute_changed_name]).index)]
        merged_df = pd.concat([filtered_df1, pm4py_ocel.object_changes], ignore_index=True)
        
        default_timestamp = pd.Timestamp("1970-01-01T00:00:00Z") 
        # default_timestamp = pd.Timestamp("1970-01-01") 
        with pd.option_context('future.no_silent_downcasting', True):
            merged_df[lbl_timestamp] = (
                merged_df[lbl_timestamp].infer_objects().fillna(default_timestamp)
            )        
        
        df_dts = [pm4py_ocel.events, pm4py_ocel.relations, merged_df]
        for df in df_dts:
            if df is not None and not df.empty:
                df[lbl_timestamp] = pd.to_datetime(
                df[lbl_timestamp], 
                format='%Y-%m-%dT%H:%M:%S.%fZ'
            )
        return cls(pm4py_ocel.events, pm4py_ocel.objects, pm4py_ocel.relations, pm4py_ocel.o2o, merged_df)
    
    @classmethod
    def load_from_ocelfile(cls, zip_file_path):
        events, objects, e2o, o2o, object_changes = None, None, None, None, None

        dataframe_mapping = {
            lbl_csv_events: "events",
            lbl_csv_objects: "objects",
            lbl_csv_e2o: "e2o",
            lbl_csv_o2o: "o2o",
            lbl_csv_object_changes: "object_changes"
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
                        elif filename == lbl_csv_object_changes:
                            object_changes = df
        
        # Time conversion
        df_dts = [events, object_changes, e2o]
        for df in df_dts:
            if df is not None and not df.empty:
                df[lbl_timestamp] = pd.to_datetime(
                df[lbl_timestamp], 
                format='%Y-%m-%dT%H:%M:%S.%fZ'  # Match the given timestamp format
            )
                
        return cls(events, objects, e2o, o2o, object_changes)

    def save_to_jsonfile(self, file_path):        
        def get_dataframe_col_type(dtype):
            dtype = str(dtype)
            if dtype == 'object':
                return "string"
            elif dtype == 'datetime64[ns]':
                return "date"
            elif dtype == 'float64':
                return "float"
            return "unknown"
        
        def build_object_types(objects: pd.DataFrame):
            object_types = []
            for ot in objects[lbl_object_type].unique():
                attributes = set(objects[objects[lbl_object_type] == ot].dropna(axis=1).columns) - \
                             {lbl_event_id, lbl_object_id, lbl_target_object_id, lbl_object_type, lbl_qual, lbl_event_type, lbl_timestamp}
                object_types.append({
                    "name": ot,
                    "attributes": [{"name": att, "type": get_dataframe_col_type(objects[att].dtype)} for att in attributes]
                })
            return object_types
        
        def build_event_types(events: pd.DataFrame):
            """
            Build the event types based on the input events DataFrame.
            """
            event_types = []
            for et in events[lbl_event_type].unique():
                attributes = set(events[events[lbl_event_type] == et].dropna(axis=1).columns) - \
                             {lbl_event_id, lbl_object_id, lbl_target_object_id, lbl_object_type, lbl_qual, lbl_event_type, lbl_timestamp}
                event_types.append({
                    "name": et,
                    "attributes": [{"name": att, "type": get_dataframe_col_type(events[att].dtype)} for att in attributes]
                })
            return event_types
        
        def build_objects(objects: pd.DataFrame, object_changes: Optional[pd.DataFrame], o2o: Optional[pd.DataFrame]):
            object_list = []
            for _, row in objects.iterrows():
                obj_descr = {
                    "id": row[lbl_object_id],
                    "type": row[lbl_object_type],
                    "attributes": [],
                    "relationships": []
                }
                # Add attribute changes
                if object_changes is not None and not object_changes.empty:
                    obj_changes = object_changes[object_changes[lbl_object_id] == row[lbl_object_id]]
                    for _, change in obj_changes.iterrows():
                        field = change[lbl_object_attribute_changed_name]
                        timestamp = change[lbl_timestamp]
                        obj_descr["attributes"].append({
                            "name": field,
                            "time": timestamp.strftime('%Y-%m-%dT%H:%M:%S.') + f'{timestamp.microsecond // 1000:03d}Z',
                            "value": change[field]
                        })
                # Add relationships
                if o2o is not None and not o2o.empty:
                    related_objects = o2o[o2o[lbl_object_id] == row[lbl_object_id]]
                    for _, rel in related_objects.iterrows():
                        obj_descr["relationships"].append({
                            "objectId": rel[lbl_target_object_id],
                            "qualifier": rel[lbl_qual]
                        })
                object_list.append(obj_descr)
            return object_list
        
        def build_events(events: pd.DataFrame, e2o: Optional[pd.DataFrame]):
            event_list = []
            for _, row in events.iterrows():
                event_descr = {
                    "id": row[lbl_event_id],
                    "type": row[lbl_event_type],
                    "time": row[lbl_timestamp].strftime('%Y-%m-%dT%H:%M:%S.') + f'{row[lbl_timestamp].microsecond // 1000:03d}Z',
                    "attributes": [],
                    "relationships": []
                }
                # Add attributes
                attributes = set(row.dropna().keys()) - {lbl_event_id, lbl_object_id, lbl_target_object_id, lbl_object_type, lbl_qual, lbl_event_type, lbl_timestamp}
                for att in attributes:
                    event_descr["attributes"].append({"name": att, "value": row[att]})
                # Add relationships
                if e2o is not None and not e2o.empty:
                    related_objects = e2o[e2o[lbl_event_id] == row[lbl_event_id]]
                    for _, rel in related_objects.iterrows():
                        event_descr["relationships"].append({
                            "objectId": rel[lbl_object_id],
                            "qualifier": rel[lbl_qual]
                        })
                event_list.append(event_descr)
            return event_list
        
        def process_data(file_path: str, events: Optional[pd.DataFrame], objects: pd.DataFrame,
                         e2o: Optional[pd.DataFrame], o2o: Optional[pd.DataFrame], object_changes: Optional[pd.DataFrame]):
            result = {
                "objectTypes": build_object_types(objects),
                "eventTypes": build_event_types(events) if events is not None else [],
                "objects": build_objects(objects, object_changes, o2o),
                "events": build_events(events, e2o) if events is not None else []
            }
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2)

        process_data(file_path, self.events, self.objects, self.e2o, self.o2o, self.object_changes)
        
    @classmethod
    def load_from_jsonfile(cls, file_path):
        file = open(file_path, "r", encoding="utf-8")
        ocel_json = json.load(file)
        file.close()

        log = {}
        log["ocel:events"] = {}
        log["ocel:objects"] = {}
        log["ocel:objectChanges"] = []
        log["ocel:global-log"] = {}
        log["ocel:global-event"] = {}
        log["ocel:global-object"] = {}

        # import events
        for event in ocel_json.get("events", []):
            event_details = {
                lbl_event_type: event.get("type"),
                lbl_timestamp: event.get("time"),
                "ocel:vmap": {},
                "ocel:typedOmap": [],
            }
        
            attributes = event.get("attributes", [])
            if attributes:
                event_details["ocel:vmap"] = {attr["name"]: attr["value"] for attr in attributes}
        
            relationships = event.get("relationships", [])
            if relationships:
                event_details["ocel:typedOmap"] = [
                    {lbl_object_id: rel["objectId"], lbl_qual: rel["qualifier"]}
                    for rel in relationships
                ]
        
            event_details["ocel:omap"] = list({obj[lbl_object_id] for obj in event_details["ocel:typedOmap"]})
        
            log["ocel:events"][event["id"]] = event_details

        # import objects
        for obj in ocel_json["objects"]:
            object_details = {
                lbl_object_type: obj.get("type"),
                "ocel:ovmap": {},
                "ocel:o2o": [],
            }
            
            attributes = obj.get("attributes", [])
            for attribute in attributes:
                attribute_name = attribute["name"]
                attribute_value = attribute["value"]
                attribute_time = attribute["time"]
                
                log["ocel:objectChanges"].append({
                        lbl_object_id: obj["id"],
                        lbl_object_type: obj["type"],
                        lbl_object_attribute_changed_name: attribute_name,
                        attribute_name: attribute_value,
                        lbl_timestamp: attribute_time
                    })
                object_details["ocel:ovmap"][attribute_name] = attribute_value
        
            # Process relationships if they exist
            relationships = obj.get("relationships", [])
            if relationships:
                object_details["ocel:o2o"] = [
                    {lbl_object_id: relation["objectId"], lbl_qual: relation["qualifier"]}
                    for relation in relationships
                ]
        
            log["ocel:objects"][obj["id"]] = object_details

        # setting data
        events = []
        objects = []
        
        e2o = []
        o2o = []
        
        object_changes = []
        
        result = {}
        # setting objects
        for obj_id, obj in log['ocel:objects'].items():
            obj_type = obj[lbl_object_type]
            result[obj_id] = obj_type
        
            obj_data = {
                lbl_object_id: obj_id,
                lbl_object_type: obj_type,
                **obj['ocel:ovmap']  
            }
            
            if 'ocel:o2o' in obj:
                for related_obj in obj['ocel:o2o']:
                    target_id = related_obj[lbl_object_id]
                    qualifier = related_obj[lbl_qual]
                    o2o.append({
                        lbl_object_id: obj_id,
                        lbl_target_object_id: target_id,
                        lbl_qual: qualifier
                    })
            
            objects.append(obj_data)
        
        #settting events
        for event_id, event_data in log['ocel:events'].items():
            event_details = {
                lbl_event_id: event_id,
                lbl_timestamp: event_data[lbl_timestamp],
                lbl_event_type: event_data[lbl_event_type],
                **event_data["ocel:vmap"] 
            }
        
            event_relationships = {}
            for related_object in event_data["ocel:omap"]:
                if related_object in result:
                    event_relationships[related_object] = {
                        lbl_event_id: event_id,
                        lbl_event_type: event_data[lbl_event_type],
                        lbl_timestamp: event_data[lbl_timestamp],
                        lbl_object_id: related_object,
                        lbl_object_type: result[related_object]
                    }
            
            if "ocel:typedOmap" in event_data:
                for relationship in event_data["ocel:typedOmap"]:
                    if lbl_object_id in relationship:
                        object_key = relationship[lbl_object_id]
                        if object_key in event_relationships:
                            event_relationships[object_key][lbl_qual] = relationship[lbl_qual]
            
            for related_object_id, relationship_details in event_relationships.items():
                e2o.append(relationship_details)
            
            events.append(event_details)

        #loading object changes
        if "ocel:objectChanges" in log:
            object_changes = log["ocel:objectChanges"]
        
        events  = pd.DataFrame.from_records(events)
        objects = pd.DataFrame.from_records(objects)
        e2o = pd.DataFrame.from_records(e2o)
        object_changes = pd.DataFrame.from_records(object_changes)  if object_changes else None
        o2o = pd.DataFrame.from_records(o2o)  if o2o else None

        events = events.sort_values(by=[lbl_timestamp, lbl_event_id])
        e2o = e2o.sort_values(by=[lbl_timestamp, lbl_object_id])

        if object_changes is not None and not object_changes.empty:      
            object_id_mapping = objects[[lbl_object_id, lbl_object_type]].to_dict("records")
            object_id_mapping = {entry[lbl_object_id]: entry[lbl_object_type] for entry in object_id_mapping}
            
            object_changes[lbl_object_type] = object_changes[lbl_object_id].map(object_id_mapping)

        # Time conversion
        df_dts = [events, object_changes, e2o]
        for df in df_dts:
            if df is not None and not df.empty:
                df[lbl_timestamp] = pd.to_datetime(
                df[lbl_timestamp], 
                format='%Y-%m-%dT%H:%M:%S.%fZ'  # Match the given timestamp format
            )
        

        return cls(events, objects, e2o, o2o, object_changes)