import pm4py

def drill_down_objecttype(ocel, ot, oa):
  from pm4py.objects.ocel.obj import OCEL
  assert type(ocel) is OCEL
  
  components = [('objects', ocel.objects), ('relations',ocel.relations), ('object_changes',ocel.object_changes)]
  res = {}
  
  for (n,c) in components:
    df1 = c
    
    final_columns = list(df1.columns)
    processing_columns = final_columns.copy()
    processing_columns.append(oa)
    processing_columns = list(set(processing_columns))
    
    df1 = df1.merge(ocel.objects, on=[ocel.object_id_column], suffixes=('', '_y'))[processing_columns]
    idx = df1[df1[ocel.object_type_column]==ot].index
    df1.loc[idx, ocel.object_type_column] = '(' + df1.loc[idx, ocel.object_type_column].astype(str) + ',' + df1.loc[idx, oa].astype(str) + ')'
    
    res[n] =  df1[final_columns]
    
  return pm4py.objects.ocel.obj.OCEL(
        ocel.events
        ,res['objects']
        ,res['relations']
        ,ocel.globals
        ,ocel.parameters
        ,ocel.o2o
        ,ocel.e2e
        ,res['object_changes']
  )
  
def roll_up_objecttype(ocel, ot, oa):
  from pm4py.objects.ocel.obj import OCEL
  assert type(ocel) is OCEL
  
  components = [('objects', ocel.objects), ('relations',ocel.relations), ('object_changes',ocel.object_changes)]
  res = {}
  
  for (n,c) in components:
    df1 = c
    
    final_columns = list(df1.columns)
    processing_columns = final_columns.copy()
    processing_columns.append(oa)
    processing_columns = list(set(processing_columns))
    
    df1 = df1.merge(ocel.objects, on=[ocel.object_id_column], suffixes=('', '_y'))[processing_columns]
    idx = df1[df1[ocel.object_type_column]=='(' + ot + ',' + df1[oa] + ')'].index
    df1.loc[idx, ocel.object_type_column] = ot
    
    res[n] =  df1[final_columns]
    
  return pm4py.objects.ocel.obj.OCEL(
        ocel.events
        ,res['objects']
        ,res['relations']
        ,ocel.globals
        ,ocel.parameters
        ,ocel.o2o
        ,ocel.e2e
        ,res['object_changes']
    )