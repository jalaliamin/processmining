def filter_ocel(ocel, object_types=None, event_threshold=0):
  from pm4py.objects.ocel.obj import OCEL

  assert type(ocel) is OCEL
  
  if object_types==None:
    object_types = list(ocel.objects[ocel.object_type_column].unique())

  df_ev  = ocel.events
  df_rel = ocel.relations
  df_obj = ocel.objects
  
  df_obj = df_obj[df_obj[ocel.object_type_column ].isin(object_types)]
  df_rel = df_rel[df_rel[ocel.object_type_column ].isin(object_types)]
  
  df_ev = df_ev[df_ev[ocel.event_id_column ].isin(df_rel[ocel.event_id_column ])]

  df_ev  = df_ev.groupby([ocel.event_activity]).filter(lambda x: len(x) >= event_threshold)

  df_rel = df_rel[df_rel[ocel.event_id_column].isin(df_ev[ocel.event_id_column])]

  df_obj = df_obj[df_obj[ocel.object_id_column].isin(df_rel[[ocel.object_id_column]][ocel.object_id_column].unique())]

  return OCEL(df_ev, df_obj, df_rel, ocel.globals, ocel.parameters)