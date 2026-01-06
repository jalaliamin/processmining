# =========================
# olap.py
# =========================
from __future__ import annotations

import copy
import re
from typing import Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import pm4py
from pm4py.objects.ocel.obj import OCEL


# --------------------------------------------------------------------
# Column detection (pm4py-version tolerant)
# --------------------------------------------------------------------
def _detect_object_id_col(ocel: OCEL, df: pd.DataFrame) -> str:
    for cand in [
        getattr(ocel, "object_id_column", None),
        "ocel:oid",
        "object_id",
        "oid",
    ]:
        if cand and cand in df.columns:
            return cand
    raise ValueError("Unable to detect object id column (e.g., 'ocel:oid').")


def _detect_object_type_col(ocel: OCEL, df: pd.DataFrame) -> str:
    for cand in [
        getattr(ocel, "object_type_column", None),
        "ocel:type",
        "object_type",
        "type",
    ]:
        if cand and cand in df.columns:
            return cand
    raise ValueError("Unable to detect object type column (e.g., 'ocel:type').")


def _detect_timestamp_col(df: pd.DataFrame) -> Optional[str]:
    for cand in [
        getattr(pm4py.objects.ocel.constants, "DEFAULT_TIMESTAMP", None),
        "ocel:timestamp",
        "timestamp",
        "time",
    ]:
        if cand and cand in df.columns:
            return cand
    return None


def _detect_object_changes_field_col(oc: pd.DataFrame) -> str:
    """
    Detect column that stores "which attribute changed" in pm4py object_changes.
    Common pm4py name: 'ocel:field'.
    """
    for cand in ["ocel:field", "field", "attribute", "attr", "ocel:attribute", "changed_attribute"]:
        if cand in oc.columns:
            return cand
    raise ValueError(
        "Unable to detect the 'changed attribute name' column in object_changes "
        "(expected e.g. 'ocel:field')."
    )


def _detect_object_changes_value_col(oc: pd.DataFrame) -> Optional[str]:
    """
    Detect a dedicated value column for long-form object_changes.
    Common pm4py name: 'ocel:value' (varies across versions/exports).
    """
    for cand in ["ocel:value", "value", "val", "ocel:val"]:
        if cand in oc.columns:
            return cand
    return None


def _ensure_datetime(df: pd.DataFrame, col: str) -> pd.DataFrame:
    out = df.copy()
    out[col] = pd.to_datetime(out[col], errors="coerce", utc=True).dt.tz_localize(None)
    return out


# --------------------------------------------------------------------
# History lookup (as-of) for drill-down
# --------------------------------------------------------------------
def _build_asof_attribute_values(
    ocel: OCEL,
    object_attribute: str,
) -> Tuple[pd.DataFrame, str]:
    """
    Build an as-of change stream for a single object attribute.

    Returns
    -------
    (changes_df, oid_col)
    where changes_df has columns: [oid_col, 'ts', 'value'].

    Supported object_changes layouts:
    - Wide: attribute values stored in a column named `object_attribute`
    - Long: attribute values stored in a dedicated column (e.g., 'ocel:value'),
            with attribute name stored in 'ocel:field'
    """
    if ocel.object_changes is None or ocel.object_changes.empty:
        if ocel.objects is None or ocel.objects.empty:
            raise ValueError("Cannot build history stream: OCEL has no objects and no object_changes.")
        oid_col = _detect_object_id_col(ocel, ocel.objects)
        return pd.DataFrame(columns=[oid_col, "ts", "value"]), oid_col

    oc = ocel.object_changes.copy()
    ts_col = _detect_timestamp_col(oc)
    if ts_col is None:
        raise ValueError("object_changes lacks a recognized timestamp column (e.g., 'ocel:timestamp').")

    oc = _ensure_datetime(oc, ts_col)

    oid_col = _detect_object_id_col(ocel, oc)
    field_col = _detect_object_changes_field_col(oc)
    value_col = _detect_object_changes_value_col(oc)

    if value_col is not None:
        # Long format: (oid, field, value, ts)
        sub = oc.loc[oc[field_col] == object_attribute, [oid_col, ts_col, value_col]].copy()
        sub = sub.rename(columns={ts_col: "ts", value_col: "value"})
    else:
        # Wide format: (oid, field, <attribute columns...>, ts)
        if object_attribute not in oc.columns:
            return pd.DataFrame(columns=[oid_col, "ts", "value"]), oid_col
        sub = oc.loc[oc[field_col] == object_attribute, [oid_col, ts_col, object_attribute]].copy()
        sub = sub.rename(columns={ts_col: "ts", object_attribute: "value"})

    sub = sub.dropna(subset=[oid_col, "ts"])
    # Global monotonic sort by ts required by merge_asof
    sub = sub.sort_values(by=["ts", oid_col], kind="mergesort")
    return sub, oid_col


def _attach_value_asof(
    df: pd.DataFrame,
    oid_col: str,
    asof_values: pd.DataFrame,
    object_attribute: str,
    time_col: Optional[str],
) -> pd.DataFrame:
    """
    Attach __olap_attr_value to df:
      - If time_col exists: value as-of time_col per object (merge_asof)
      - Else: last known value per object

    merge_asof constraints:
      - left and right must be globally sorted by the 'on' key (ts)
      - rows with NaT timestamps cannot participate (kept as NA)
    """
    if df is None or df.empty or oid_col not in df.columns:
        return df

    out = df.copy()

    if asof_values is None or asof_values.empty:
        out["__olap_attr_value"] = out[object_attribute] if object_attribute in out.columns else pd.NA
        return out

    # No time column: last known value per object
    if time_col is None or time_col not in out.columns:
        last_vals = (
            asof_values.sort_values(["ts", oid_col], kind="mergesort")
            .groupby(oid_col, as_index=False)
            .last()[[oid_col, "value"]]
            .rename(columns={"value": "__olap_attr_value"})
        )
        out = out.merge(last_vals, on=oid_col, how="left")
        if object_attribute in out.columns:
            out["__olap_attr_value"] = out["__olap_attr_value"].fillna(out[object_attribute])
        return out

    out = _ensure_datetime(out, time_col)

    # Only rows with non-null ts can be merged
    valid_mask = out[time_col].notna() & out[oid_col].notna()
    out["__olap_attr_value"] = pd.NA

    if not valid_mask.any():
        if object_attribute in out.columns:
            out["__olap_attr_value"] = out["__olap_attr_value"].fillna(out[object_attribute])
        return out

    left = out.loc[valid_mask, [oid_col, time_col]].copy().rename(columns={time_col: "ts"})
    left["__row_idx"] = left.index.values
    left = left.sort_values(by=["ts", oid_col], kind="mergesort")

    right = asof_values.copy().dropna(subset=[oid_col, "ts"])
    right = right.sort_values(by=["ts", oid_col], kind="mergesort")

    merged = pd.merge_asof(
        left,
        right,
        on="ts",
        by=oid_col,
        direction="backward",
        allow_exact_matches=True,
    )

    out.loc[merged["__row_idx"].values, "__olap_attr_value"] = merged["value"].values

    if object_attribute in out.columns:
        out["__olap_attr_value"] = out["__olap_attr_value"].fillna(out[object_attribute])

    return out


# --------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------
def drill_down(
    ocel: OCEL,
    object_type: str,
    object_attribute: str,
    *,
    consider_history: bool = False,
) -> OCEL:
    """
    Drill down on object types by an object attribute.

    consider_history=False (default):
        Legacy behavior. Uses the current value from ocel.objects across all components.

    consider_history=True:
        History-aware behavior. Uses value-as-of-time (from object_changes) for relations and
        object_changes, and last-known value for objects.

    Returns
    -------
    OCEL
        A deep-copied pm4py OCEL instance with rewritten object types.
    """
    if not isinstance(ocel, OCEL):
        raise TypeError("ocel must be an instance of pm4py.objects.ocel.obj.OCEL")

    ocel = copy.deepcopy(ocel)

    objects = ocel.objects
    relations = ocel.relations
    object_changes = ocel.object_changes

    if objects is None or objects.empty:
        raise ValueError("OCEL.objects is empty; drill_down requires objects.")

    oid_col_obj = _detect_object_id_col(ocel, objects)
    otype_col_obj = _detect_object_type_col(ocel, objects)

    components = [
        ("objects", objects),
        ("relations", relations),
        ("object_changes", object_changes),
    ]
    res = {}

    if not consider_history:
        # Legacy: join current values from objects.
        for name, df in components:
            if df is None or df.empty:
                res[name] = df
                continue

            final_columns = list(df.columns)
            processing_columns = list(set(final_columns + [object_attribute]))

            df1 = df.merge(objects, on=[oid_col_obj], suffixes=("", "_y"))[processing_columns]
            idx = df1[df1[otype_col_obj] == object_type].index

            df1.loc[idx, otype_col_obj] = (
                "("
                + df1.loc[idx, otype_col_obj].astype(str)
                + ","
                + df1.loc[idx, object_attribute].astype(str)
                + ")"
            )
            res[name] = df1[final_columns]

        return pm4py.objects.ocel.obj.OCEL(
            ocel.events,
            res["objects"],
            res["relations"],
            ocel.globals,
            ocel.parameters,
            ocel.o2o,
            ocel.e2e,
            res["object_changes"],
        )

    # History-aware: build as-of stream for the attribute
    asof_values, oid_col_hist = _build_asof_attribute_values(ocel, object_attribute)

    if oid_col_hist != oid_col_obj and not asof_values.empty:
        asof_values = asof_values.rename(columns={oid_col_hist: oid_col_obj})

    for name, df in components:
        if df is None or df.empty:
            res[name] = df
            continue

        if oid_col_obj not in df.columns:
            res[name] = df
            continue

        final_columns = list(df.columns)

        time_col = None if name == "objects" else _detect_timestamp_col(df)
        df1 = _attach_value_asof(df, oid_col_obj, asof_values, object_attribute, time_col=time_col)

        idx = df1[df1[otype_col_obj] == object_type].index
        vals = df1.loc[idx, "__olap_attr_value"]
        new_types = "(" + object_type + "," + vals.astype(str) + ")"

        df1.loc[idx, otype_col_obj] = np.where(vals.notna(), new_types, df1.loc[idx, otype_col_obj])

        df1 = df1.drop(columns=["__olap_attr_value"], errors="ignore")
        res[name] = df1[final_columns]

    return pm4py.objects.ocel.obj.OCEL(
        ocel.events,
        res["objects"],
        res["relations"],
        ocel.globals,
        ocel.parameters,
        ocel.o2o,
        ocel.e2e,
        res["object_changes"],
    )


def roll_up(
    ocel: OCEL,
    object_type: str,
    object_attribute: str,
) -> OCEL:
    """
    Roll up drilled-down object types back to their base type.

    This roll-up is intentionally mode-independent: it collapses any label of the form
        (object_type, ...)
    back to:
        object_type
    across objects, relations, and object_changes.

    Parameters
    ----------
    ocel:
        pm4py OCEL instance.
    object_type:
        Base object type to roll up to.
    object_attribute:
        Kept for API symmetry with drill_down. Not used by this implementation.

    Returns
    -------
    OCEL
        A deep-copied pm4py OCEL instance with rolled-up object types.
    """
    if not isinstance(ocel, OCEL):
        raise TypeError("ocel must be an instance of pm4py.objects.ocel.obj.OCEL")

    ocel = copy.deepcopy(ocel)

    objects = ocel.objects
    relations = ocel.relations
    object_changes = ocel.object_changes

    if objects is None or objects.empty:
        raise ValueError("OCEL.objects is empty; roll_up requires objects.")

    otype_col_obj = _detect_object_type_col(ocel, objects)
    pattern = re.compile(rf"^\({re.escape(object_type)},.*\)$")

    components = [
        ("objects", objects),
        ("relations", relations),
        ("object_changes", object_changes),
    ]
    res = {}

    for name, df in components:
        if df is None or df.empty:
            res[name] = df
            continue

        if otype_col_obj not in df.columns:
            res[name] = df
            continue

        df1 = df.copy()
        mask = df1[otype_col_obj].astype(str).apply(lambda v: bool(pattern.match(v)))
        df1.loc[mask, otype_col_obj] = object_type
        res[name] = df1

    return pm4py.objects.ocel.obj.OCEL(
        ocel.events,
        res["objects"],
        res["relations"],
        ocel.globals,
        ocel.parameters,
        ocel.o2o,
        ocel.e2e,
        res["object_changes"],
    )


# --------------------------------------------------------------------
# unfold/fold (unchanged)
# --------------------------------------------------------------------
def unfold(ocel: OCEL, event_type: str, object_type: str, qualifiers: Optional[Sequence[str]] = None) -> OCEL:
    if not isinstance(ocel, OCEL):
        raise TypeError("ocel must be an instance of pm4py.objects.ocel.obj.OCEL")

    ocel = copy.deepcopy(ocel)

    activity_col = pm4py.objects.ocel.constants.DEFAULT_EVENT_ACTIVITY
    qual_col = ocel.qualifier
    event_id_col = getattr(ocel, "event_id_column", None) or "ocel:eid"
    obj_type_col = _detect_object_type_col(ocel, ocel.relations)

    if qualifiers is None:
        Q = set(list(ocel.relations[qual_col].drop_duplicates()))
    else:
        Q = set(qualifiers)

    df = ocel.relations
    rel_affected_rows_index = df[
        (df[activity_col] == event_type) & (df[obj_type_col] == object_type) & (df[qual_col].isin(Q))
    ].index
    affected_event_ids = ocel.relations.iloc[rel_affected_rows_index][event_id_col]

    rel_affected_rows_index = df[df[event_id_col].isin(affected_event_ids)].index
    df.loc[rel_affected_rows_index, activity_col] = "(" + event_type + "," + object_type + ")"

    df = ocel.events
    rel_affected_rows_index = df[df[event_id_col].isin(affected_event_ids)].index
    df.loc[rel_affected_rows_index, activity_col] = "(" + event_type + "," + object_type + ")"

    return ocel


def fold(ocel: OCEL, event_type: str, object_type: str) -> OCEL:
    if not isinstance(ocel, OCEL):
        raise TypeError("ocel must be an instance of pm4py.objects.ocel.obj.OCEL")

    ocel = copy.deepcopy(ocel)

    activity_col = pm4py.objects.ocel.constants.DEFAULT_EVENT_ACTIVITY

    ocel.events[activity_col] = ocel.events[activity_col].apply(
        lambda val: event_type if val == f"({event_type},{object_type})" else val
    )
    ocel.relations[activity_col] = ocel.relations[activity_col].apply(
        lambda val: event_type if val == f"({event_type},{object_type})" else val
    )

    return ocel