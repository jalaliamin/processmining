# =========================
# tocel.py
# =========================
from __future__ import annotations

import io
import zipfile
from typing import Dict, Optional, Set

import numpy as np
import pandas as pd

from processmining.loghandlers.constants import *


class TOCEL:
    """
    Temporal Object-Centric Event Log (TOCEL) container and transformation utilities.

    This class provides:
      1) A lightweight in-memory representation of a TOCEL log using pandas DataFrames.
      2) ZIP-based persistence for TOCEL logs (CSV files inside a ZIP archive).
      3) A freezing transformation from TOCEL to OCEL aligned with the formal semantics
         described in your specification.

    Data model
    ----------
    TOCEL is represented with up to four pandas DataFrames:

    - events:
        Event table (point-based timestamps).
        Expected to contain at least:
            * lbl_event_id, lbl_event_type, lbl_timestamp
        plus arbitrary event attributes.

    - objects:
        Object snapshot table with temporal validity.
        Each row represents a snapshot valid in the interval:
            [lbl_timestamp_validfrom, lbl_timestamp_validto]
        Expected to contain at least:
            * lbl_object_id, lbl_object_type, lbl_timestamp_validfrom
        and typically:
            * lbl_timestamp_validto, lbl_object_snapshot_id
        plus arbitrary object attributes.

        The loader and transformer also accept the following common OCEL-style column names:
            * "ocel:oid"                  -> lbl_object_id
            * "ocel:type"                 -> lbl_object_type
            * "ocel:timestamp:valid_from" -> lbl_timestamp_validfrom
            * "ocel:timestamp:valid_to"   -> lbl_timestamp_validto
            * "ocel:osid"                 -> lbl_object_snapshot_id

    - e2o:
        Event-to-object relation table.
        Expected columns depend on your constants; typically:
            * lbl_event_id, lbl_event_type, lbl_timestamp
            * lbl_object_id, lbl_object_type
            * lbl_qual (optional)
        If lbl_object_snapshot_id exists, it is removed in the OCEL result.

    - o2o:
        Object-to-object relation table (temporal in TOCEL).
        If validity columns exist, they are removed and the relation is collapsed to a union
        when producing OCEL.

    Freezing (TOCEL -> OCEL)
    ------------------------
    The transformation is exposed via :meth:`cast_to_ocel` and supports two modes:

    1) Snapshot freezing (freeze_time is provided; t ≠ ⊥)
       - Objects are projected to a snapshot view at time t:
           objtype'(o) = objtype(o, (t,t))
         Operationally, the snapshot row for each object is selected via an "as-of" rule:
           select the snapshot with maximum valid_from <= t (fallback to earliest snapshot)
       - No distinguished "dynamic" type is introduced in this mode.
       - object_changes captures the full history (all snapshots), and object_changes.object_type
         is rewritten to the resolved type-at-t to satisfy OCEL consistency.
       - If e2o includes an object type column, it is also rewritten to resolved type-at-t.

    2) Global freezing (freeze_time is None; t = ⊥)
       - Objects with a changing object type over time are assigned a distinguished supertype
         `dynamic_type_name` in OCEL to avoid object duplication.
       - The last temporal object type is stored in the OCEL objects table in the attribute
         column `type_attr_name`.
       - object_changes captures full attribute history for all objects and additionally
         captures the type evolution of dynamic objects as changes of `type_attr_name`.
       - If e2o includes an object type column, dynamic objects are rewritten to `dynamic_type_name`.
       - o2o is collapsed to the union of relations (validity removed). If type columns exist,
         dynamic objects are rewritten to `dynamic_type_name`.

    Notes on object_changes
    -----------------------
    object_changes are derived from the objects snapshot history by:
      - emitting all non-null attributes in the first snapshot as initial changes
      - emitting subsequent changes when attribute values differ from the previous snapshot

    Time handling
    -------------
    - All timestamps are parsed into pandas datetime.
    - freeze_time is accepted as any pandas-compatible time string and treated as UTC when
      timezone information is present.
    - Internal computations use tz-naive timestamps (timezone removed after UTC parsing).

    """

    def __init__(
        self,
        events: Optional[pd.DataFrame] = None,
        objects: Optional[pd.DataFrame] = None,
        e2o: Optional[pd.DataFrame] = None,
        o2o: Optional[pd.DataFrame] = None,
    ):
        self.events = events.copy(deep=True) if events is not None else None
        self.objects = objects.copy(deep=True) if objects is not None else None
        self.e2o = e2o.copy(deep=True) if e2o is not None else None
        self.o2o = o2o.copy(deep=True) if o2o is not None else None

    # ---------------------------------------------------------------------
    # Persistence
    # ---------------------------------------------------------------------
    def save_to_tocelfile(self, file_path: str) -> None:
        """
        Persist the TOCEL log as a ZIP archive containing CSV files.

        Parameters
        ----------
        file_path:
            Target path for the ZIP file.
        """
        dataframes = {
            lbl_csv_events: self.events.copy(deep=True) if self.events is not None else None,
            lbl_csv_objects: self.objects.copy(deep=True) if self.objects is not None else None,
            lbl_csv_e2o: self.e2o.copy(deep=True) if self.e2o is not None else None,
            lbl_csv_o2o: self.o2o.copy(deep=True) if self.o2o is not None else None,
        }

        def _format_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
            for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
                df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            return df

        with zipfile.ZipFile(file_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for filename, dataframe in dataframes.items():
                if dataframe is None:
                    continue
                dataframe = _format_datetime_columns(dataframe)
                with io.BytesIO() as buffer:
                    dataframe.to_csv(buffer, index=False, encoding="utf-8")
                    zf.writestr(filename, buffer.getvalue())

    @classmethod
    def load_from_tocelfile(cls, zip_file_path: str) -> "TOCEL":
        """
        Load a TOCEL log from a ZIP archive containing CSV files.

        Parameters
        ----------
        zip_file_path:
            Path to the ZIP file.

        Returns
        -------
        TOCEL
            Loaded TOCEL instance.
        """
        events, objects, e2o, o2o = None, None, None, None

        dataframe_mapping = {
            lbl_csv_events: "events",
            lbl_csv_objects: "objects",
            lbl_csv_e2o: "e2o",
            lbl_csv_o2o: "o2o",
        }

        with zipfile.ZipFile(zip_file_path, "r") as zf:
            for filename in zf.namelist():
                if filename not in dataframe_mapping:
                    continue
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

        # Convert timestamp-like columns where present
        df_dts = [events, objects, e2o, o2o]
        cols_timestamps = [lbl_timestamp, lbl_timestamp_validfrom, lbl_timestamp_validto]
        for df in df_dts:
            if df is None or df.empty:
                continue
            for col in cols_timestamps:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

        return cls(events, objects, e2o, o2o)

    # ---------------------------------------------------------------------
    # Internal utilities
    # ---------------------------------------------------------------------
    @staticmethod
    def _normalize_freeze_time(freeze_time: Optional[str]) -> Optional[pd.Timestamp]:
        """
        Normalize freeze_time to tz-naive UTC timestamp.

        Parameters
        ----------
        freeze_time:
            Timestamp string (pandas compatible). If None, returns None.

        Returns
        -------
        pandas.Timestamp or None
        """
        if freeze_time is None:
            return None
        t = pd.to_datetime(freeze_time, errors="coerce", utc=True)
        if pd.isna(t):
            raise ValueError(f"freeze_time could not be parsed: {freeze_time!r}")
        return t.tz_localize(None)

    @staticmethod
    def _normalize_tocel_object_columns(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """
        Normalize common OCEL-style object snapshot column names to internal constants.

        This method does not modify file formats; it only renames columns in-memory.

        Parameters
        ----------
        df:
            Objects snapshot DataFrame.

        Returns
        -------
        pandas.DataFrame or None
        """
        if df is None or df.empty:
            return df

        colmap = {}
        if "ocel:oid" in df.columns and lbl_object_id not in df.columns:
            colmap["ocel:oid"] = lbl_object_id
        if "ocel:type" in df.columns and lbl_object_type not in df.columns:
            colmap["ocel:type"] = lbl_object_type
        if "ocel:timestamp:valid_from" in df.columns and lbl_timestamp_validfrom not in df.columns:
            colmap["ocel:timestamp:valid_from"] = lbl_timestamp_validfrom
        if "ocel:timestamp:valid_to" in df.columns and lbl_timestamp_validto not in df.columns:
            colmap["ocel:timestamp:valid_to"] = lbl_timestamp_validto
        if "ocel:osid" in df.columns and lbl_object_snapshot_id not in df.columns:
            colmap["ocel:osid"] = lbl_object_snapshot_id

        df = df.rename(columns=colmap)

        for c in [lbl_timestamp_validfrom, lbl_timestamp_validto]:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce", utc=True).dt.tz_localize(None)

        return df

    @staticmethod
    def _detect_dynamic_objects(objects_df: pd.DataFrame) -> Set[str]:
        """
        Identify dynamically typed objects (objects with >1 distinct type across snapshots).

        Parameters
        ----------
        objects_df:
            Normalized objects snapshot DataFrame.

        Returns
        -------
        set[str]
            Set of object IDs that are dynamically typed.
        """
        if objects_df is None or objects_df.empty:
            return set()

        df = objects_df[[lbl_object_id, lbl_object_type]].dropna().copy()
        df[lbl_object_type] = df[lbl_object_type].astype(str).str.strip()

        type_counts = (
            df.drop_duplicates()
            .groupby(lbl_object_id)[lbl_object_type]
            .nunique()
        )
        return set(type_counts[type_counts > 1].index.tolist())

    @staticmethod
    def _asof_pick_snapshots(objects_df: pd.DataFrame, t: pd.Timestamp) -> pd.DataFrame:
        """
        Select per object the snapshot valid at time t using an as-of rule.

        Rule:
          - choose snapshot with maximum valid_from <= t
          - fallback to earliest snapshot if none satisfies valid_from <= t

        Parameters
        ----------
        objects_df:
            Normalized objects snapshot DataFrame.
        t:
            Freeze time (tz-naive).

        Returns
        -------
        pandas.DataFrame
            One row per object ID.
        """
        df = objects_df.copy().sort_values([lbl_object_id, lbl_timestamp_validfrom])

        cand = df[df[lbl_timestamp_validfrom] <= t]
        picked = df.iloc[0:0].copy()

        if not cand.empty:
            idx = cand.groupby(lbl_object_id)[lbl_timestamp_validfrom].idxmax()
            picked = cand.loc[idx].copy()

        missing = set(df[lbl_object_id].unique()) - set(picked[lbl_object_id].unique())
        if missing:
            fallback = (
                df[df[lbl_object_id].isin(missing)]
                .groupby(lbl_object_id, as_index=False)
                .first()
            )
            picked = pd.concat([picked, fallback], ignore_index=True)

        return picked

    @staticmethod
    def _build_object_changes_from_snapshots(snapshots_df: pd.DataFrame) -> pd.DataFrame:
        """
        Derive OCEL-style object_changes from TOCEL object snapshots.

        The emitted changes follow these rules:
          - For the first snapshot of each object: emit changes for each non-null attribute.
          - For subsequent snapshots: emit a change for an attribute if the value differs from the
            previous snapshot and the new value is non-null.

        Parameters
        ----------
        snapshots_df:
            Normalized objects snapshot DataFrame.

        Returns
        -------
        pandas.DataFrame
            Object changes DataFrame.
        """
        if snapshots_df is None or snapshots_df.empty:
            return pd.DataFrame(columns=[lbl_object_id, lbl_object_type, lbl_timestamp, lbl_object_attribute_changed_name])

        df = snapshots_df.copy().sort_values([lbl_object_id, lbl_timestamp_validfrom])
        df[lbl_timestamp] = df[lbl_timestamp_validfrom]

        df = df.drop(
            columns=[c for c in [lbl_timestamp_validfrom, lbl_timestamp_validto, lbl_object_snapshot_id] if c in df.columns],
            errors="ignore",
        )

        base_cols = {lbl_object_id, lbl_object_type, lbl_timestamp}
        rest_columns = [c for c in df.columns if c not in base_cols]

        new_rows = []
        for _, group in df.groupby(lbl_object_id):
            group = group.sort_values(lbl_timestamp)
            prev = None

            for i in range(len(group)):
                row = group.iloc[i]

                if prev is None:
                    for col in rest_columns:
                        if pd.notna(row[col]):
                            r = {k: np.nan for k in df.columns}
                            r[lbl_object_id] = row[lbl_object_id]
                            r[lbl_object_type] = row[lbl_object_type]
                            r[lbl_timestamp] = row[lbl_timestamp]
                            r[lbl_object_attribute_changed_name] = col
                            r[col] = row[col]
                            new_rows.append(r)
                else:
                    for col in rest_columns:
                        cur = row[col]
                        prv = prev[col]
                        if pd.notna(cur) and (pd.isna(prv) or cur != prv):
                            r = {k: np.nan for k in df.columns}
                            r[lbl_object_id] = row[lbl_object_id]
                            r[lbl_object_type] = row[lbl_object_type]
                            r[lbl_timestamp] = row[lbl_timestamp]
                            r[lbl_object_attribute_changed_name] = col
                            r[col] = cur
                            new_rows.append(r)

                prev = row

        return pd.DataFrame(new_rows)

    @staticmethod
    def _overwrite_types_by_mapping(df: Optional[pd.DataFrame], oid_to_type: Dict[str, str], id_col: str, type_col: str) -> Optional[pd.DataFrame]:
        """
        Rewrite type column values in df based on an ID->type mapping.

        Parameters
        ----------
        df:
            Target DataFrame.
        oid_to_type:
            Mapping from object id to resolved type.
        id_col:
            Column holding the object id.
        type_col:
            Column holding the object type.

        Returns
        -------
        pandas.DataFrame or None
        """
        if df is None or df.empty:
            return df
        if id_col not in df.columns or type_col not in df.columns:
            return df

        out = df.copy()
        out[type_col] = out[id_col].map(oid_to_type).fillna(out[type_col])
        return out

    @staticmethod
    def _overwrite_types_for_dynamic_objects(
        df: Optional[pd.DataFrame],
        dynamic_oids: Set[str],
        dynamic_type_name: str,
        mappings,
    ) -> Optional[pd.DataFrame]:
        """
        Rewrite types for dynamic objects to a distinguished dynamic type.

        Parameters
        ----------
        df:
            Target DataFrame.
        dynamic_oids:
            Set of dynamic object IDs.
        dynamic_type_name:
            Distinguished type assigned to dynamic objects in global freezing.
        mappings:
            Iterable of (id_col, type_col) pairs to update.

        Returns
        -------
        pandas.DataFrame or None
        """
        if df is None or df.empty:
            return df
        out = df.copy()
        for id_col, type_col in mappings:
            if id_col in out.columns and type_col in out.columns:
                out.loc[out[id_col].isin(dynamic_oids), type_col] = dynamic_type_name
        return out

    @staticmethod
    def _build_type_history_changes(objects_snap_sorted: pd.DataFrame, dynamic_oids: Set[str], type_attr_name: str) -> pd.DataFrame:
        """
        Build object_changes rows capturing type evolution for dynamic objects.

        A row is emitted when:
          - it is the first snapshot of the object, or
          - the object type differs from the previous snapshot type

        The emitted change rows use:
          - lbl_object_attribute_changed_name = type_attr_name
          - value stored in the column named `type_attr_name`
          - lbl_timestamp = snapshot valid_from

        Parameters
        ----------
        objects_snap_sorted:
            Normalized objects snapshots sorted by (object_id, valid_from).
        dynamic_oids:
            Set of dynamically typed objects.
        type_attr_name:
            Attribute name used in OCEL to store type history.

        Returns
        -------
        pandas.DataFrame
        """
        if not dynamic_oids:
            return pd.DataFrame(columns=[lbl_object_id, lbl_object_type, lbl_timestamp, lbl_object_attribute_changed_name, type_attr_name])

        needed = {lbl_object_id, lbl_object_type, lbl_timestamp_validfrom}
        if not needed.issubset(set(objects_snap_sorted.columns)):
            return pd.DataFrame(columns=[lbl_object_id, lbl_object_type, lbl_timestamp, lbl_object_attribute_changed_name, type_attr_name])

        df = objects_snap_sorted[objects_snap_sorted[lbl_object_id].isin(dynamic_oids)].copy()
        if df.empty:
            return pd.DataFrame(columns=[lbl_object_id, lbl_object_type, lbl_timestamp, lbl_object_attribute_changed_name, type_attr_name])

        df = df.sort_values([lbl_object_id, lbl_timestamp_validfrom])
        df["__prev_type"] = df.groupby(lbl_object_id)[lbl_object_type].shift(1)
        mask = df["__prev_type"].isna() | (df[lbl_object_type] != df["__prev_type"])
        df = df[mask].copy()

        out = pd.DataFrame(
            {
                lbl_object_id: df[lbl_object_id].values,
                lbl_object_type: df[lbl_object_type].values,  # overwritten to dynamic_type_name in global freezing
                lbl_timestamp: df[lbl_timestamp_validfrom].values,
                lbl_object_attribute_changed_name: [type_attr_name] * len(df),
                type_attr_name: df[lbl_object_type].values,
            }
        )
        return out

    # ---------------------------------------------------------------------
    # Transformation: TOCEL -> OCEL
    # ---------------------------------------------------------------------
    def cast_to_ocel(
        self,
        freeze_time: Optional[str] = None,
        dynamic_type_name: str = "dynamic",
        type_attr_name: str = "__tocel_type_history",
    ):
        """
        Freeze this TOCEL into an OCEL according to the specified mode.

        Parameters
        ----------
        freeze_time:
            If provided, snapshot freezing is applied at this time.
            If None, global freezing is applied (t = ⊥).
        dynamic_type_name:
            Distinguished object type assigned to dynamic objects in global freezing.
        type_attr_name:
            Name of the OCEL object attribute used to store dynamic type history
            (objects table stores the last value; object_changes store the evolution).

        Returns
        -------
        OCEL
            Instance of your local OCEL class (imported from .ocel).
        """
        from .ocel import OCEL

        events = self.events.copy(deep=True) if self.events is not None else None
        objects_snap = self.objects.copy(deep=True) if self.objects is not None else None
        e2o = self.e2o.copy(deep=True) if self.e2o is not None else None
        o2o = self.o2o.copy(deep=True) if self.o2o is not None else None

        objects_snap = self._normalize_tocel_object_columns(objects_snap)

        if e2o is not None and not e2o.empty:
            e2o = e2o.drop(columns=[lbl_object_snapshot_id], errors="ignore")

        if o2o is not None and not o2o.empty:
            o2o = o2o.drop(
                columns=[c for c in [lbl_object_snapshot_id, lbl_target_object_snapshot_id, lbl_timestamp_validfrom, lbl_timestamp_validto]
                         if c in o2o.columns],
                errors="ignore",
            )
            o2o = o2o.drop_duplicates(subset=[lbl_object_id, lbl_target_object_id, lbl_qual])

        if objects_snap is None or objects_snap.empty:
            return OCEL(events=events, objects=None, e2o=e2o, o2o=o2o, object_changes=pd.DataFrame())

        for c in [lbl_object_id, lbl_object_type, lbl_timestamp_validfrom]:
            if c not in objects_snap.columns:
                raise ValueError(f"TOCEL.objects missing required column after normalization: {c}")

        objects_snap_sorted = objects_snap.sort_values([lbl_object_id, lbl_timestamp_validfrom])
        dynamic_oids = self._detect_dynamic_objects(objects_snap_sorted)

        # -----------------------------------------------------------------
        # Snapshot freezing (t != ⊥)
        # -----------------------------------------------------------------
        if freeze_time is not None:
            t = self._normalize_freeze_time(freeze_time)
            picked = self._asof_pick_snapshots(objects_snap_sorted, t)

            objects = picked.drop(
                columns=[c for c in [lbl_object_snapshot_id, lbl_timestamp_validfrom, lbl_timestamp_validto] if c in picked.columns],
                errors="ignore",
            ).copy()

            # Full object history (not truncated) and type normalization to type-at-t
            object_changes = self._build_object_changes_from_snapshots(objects_snap_sorted)

            type_at_t = (
                picked[[lbl_object_id, lbl_object_type]]
                .dropna(subset=[lbl_object_id, lbl_object_type])
                .drop_duplicates(subset=[lbl_object_id])
                .set_index(lbl_object_id)[lbl_object_type]
                .to_dict()
            )

            objects = self._overwrite_types_by_mapping(objects, type_at_t, lbl_object_id, lbl_object_type)
            e2o = self._overwrite_types_by_mapping(e2o, type_at_t, lbl_object_id, lbl_object_type)
            object_changes = self._overwrite_types_by_mapping(object_changes, type_at_t, lbl_object_id, lbl_object_type)

            if object_changes is not None and not object_changes.empty and lbl_timestamp in object_changes.columns:
                object_changes = object_changes.sort_values(
                    by=[lbl_object_id, lbl_timestamp, lbl_object_attribute_changed_name],
                    kind="mergesort",
                ).reset_index(drop=True)

            return OCEL(events=events, objects=objects, e2o=e2o, o2o=o2o, object_changes=object_changes)

        # -----------------------------------------------------------------
        # Global freezing (t = ⊥)
        # -----------------------------------------------------------------
        last_idx = objects_snap_sorted.groupby(lbl_object_id)[lbl_timestamp_validfrom].idxmax()
        last_rows = objects_snap_sorted.loc[last_idx].copy()

        last_temporal_type_per_oid = (
            objects_snap_sorted.groupby(lbl_object_id)[lbl_object_type].last()
            .astype(str)
            .str.strip()
        )

        # Objects table: assign distinguished dynamic type
        last_rows.loc[last_rows[lbl_object_id].isin(dynamic_oids), lbl_object_type] = dynamic_type_name

        objects = last_rows.drop(
            columns=[c for c in [lbl_object_snapshot_id, lbl_timestamp_validfrom, lbl_timestamp_validto] if c in last_rows.columns],
            errors="ignore",
        ).copy()

        # Objects table: add and populate type history attribute (last value)
        if type_attr_name not in objects.columns:
            objects[type_attr_name] = np.nan
        dyn_mask = objects[lbl_object_id].isin(dynamic_oids)
        objects.loc[dyn_mask, type_attr_name] = objects.loc[dyn_mask, lbl_object_id].map(last_temporal_type_per_oid)

        # object_changes: full attribute history for all objects
        object_changes = self._build_object_changes_from_snapshots(objects_snap_sorted)

        if object_changes is None or object_changes.empty:
            object_changes = pd.DataFrame(
                columns=[lbl_object_id, lbl_object_type, lbl_timestamp, lbl_object_attribute_changed_name, type_attr_name]
            )
        elif type_attr_name not in object_changes.columns:
            object_changes[type_attr_name] = np.nan

        # object_changes: append type evolution changes for dynamic objects
        type_hist_changes = self._build_type_history_changes(objects_snap_sorted, dynamic_oids, type_attr_name)
        if type_hist_changes is not None and not type_hist_changes.empty:
            for c in type_hist_changes.columns:
                if c not in object_changes.columns:
                    object_changes[c] = np.nan
            for c in object_changes.columns:
                if c not in type_hist_changes.columns:
                    type_hist_changes[c] = np.nan
            object_changes = pd.concat([object_changes, type_hist_changes[object_changes.columns]], ignore_index=True)

        # Enforce distinguished type for dynamic objects throughout OCEL views
        object_changes = self._overwrite_types_for_dynamic_objects(
            object_changes, dynamic_oids, dynamic_type_name, [(lbl_object_id, lbl_object_type)]
        )
        e2o = self._overwrite_types_for_dynamic_objects(
            e2o, dynamic_oids, dynamic_type_name, [(lbl_object_id, lbl_object_type)]
        )
        o2o = self._overwrite_types_for_dynamic_objects(
            o2o,
            dynamic_oids,
            dynamic_type_name,
            [(lbl_object_id, lbl_object_type), (lbl_target_object_id, lbl_object_type)],
        )

        if object_changes is not None and not object_changes.empty and lbl_timestamp in object_changes.columns:
            object_changes = object_changes.sort_values(
                by=[lbl_object_id, lbl_timestamp, lbl_object_attribute_changed_name],
                kind="mergesort",
            ).reset_index(drop=True)

        return OCEL(events=events, objects=objects, e2o=e2o, o2o=o2o, object_changes=object_changes)
