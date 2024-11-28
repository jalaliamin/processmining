import pm4py
from pm4py.objects.ocel.obj import OCEL
import copy

def drill_down(ocel, object_type, object_attribute):
    assert type(ocel) is OCEL
    ocel = copy.deepcopy(ocel)
    
    components = [('objects', ocel.objects), ('relations',ocel.relations), ('object_changes',ocel.object_changes)]
    res = {}
  
    for (n,c) in components:
        df1 = c
        
        final_columns = list(df1.columns)
        processing_columns = final_columns.copy()
        processing_columns.append(object_attribute)
        processing_columns = list(set(processing_columns))
        
        df1 = df1.merge(ocel.objects, on=[ocel.object_id_column], suffixes=('', '_y'))[processing_columns]
        idx = df1[df1[ocel.object_type_column]==object_type].index
        df1.loc[idx, ocel.object_type_column] = '(' + df1.loc[idx, ocel.object_type_column].astype(str) + ',' + df1.loc[idx, object_attribute].astype(str) + ')'
        
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
  
def roll_up(ocel, object_type, object_attribute):
    assert type(ocel) is OCEL
    ocel = copy.deepcopy(ocel)
    
    components = [('objects', ocel.objects), ('relations',ocel.relations), ('object_changes',ocel.object_changes)]
    res = {}
  
    for (n,c) in components:
        df1 = c
        
        final_columns = list(df1.columns)
        processing_columns = final_columns.copy()
        processing_columns.append(object_attribute)
        processing_columns = list(set(processing_columns))
        
        df1 = df1.merge(ocel.objects, on=[ocel.object_id_column], suffixes=('', '_y'))[processing_columns]
        idx = df1[df1[ocel.object_type_column]=='(' + object_type + ',' + df1[object_attribute] + ')'].index
        df1.loc[idx, ocel.object_type_column] = object_type
        
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
    
def unfold(ocel, event_type, object_type, qualifiers=None):
    assert type(ocel) is OCEL
    ocel = copy.deepcopy(ocel)
    
    activity_col = pm4py.objects.ocel.constants.DEFAULT_EVENT_ACTIVITY
    qual_col = ocel.qualifier
    event_id_col = ocel.event_id_column
    obj_type_col = ocel.object_type_column
    
    if qualifiers==None:
        Q=set(list(ocel.relations[qual_col].drop_duplicates()))
    else:
        Q=qualifiers    

    df = ocel.relations
    rel_affected_rows_index = df[(df[activity_col]==event_type)&(df[obj_type_col]==object_type)&(df[qual_col].isin(Q))].index
    affected_event_ids = ocel.relations.iloc[rel_affected_rows_index][event_id_col]
    
    rel_affected_rows_index = df[df[event_id_col].isin(affected_event_ids)].index
    df.loc[rel_affected_rows_index,activity_col]= '(' + event_type + ',' + object_type + ')'
    ocel.relations[activity_col].drop_duplicates()
    
    df = ocel.events
    rel_affected_rows_index = df[df[event_id_col].isin(affected_event_ids)].index
    df.loc[rel_affected_rows_index,activity_col]= '(' + event_type + ',' + object_type + ')'

    return ocel
    
def fold(ocel, event_type, object_type):
    assert type(ocel) is OCEL
    ocel = copy.deepcopy(ocel)
    
    ocel.events[pm4py.objects.ocel.constants.DEFAULT_EVENT_ACTIVITY] = ocel.events[pm4py.objects.ocel.constants.DEFAULT_EVENT_ACTIVITY].apply(
        lambda val: event_type if val=='('+event_type+','+object_type+')' else val)
    ocel.relations[pm4py.objects.ocel.constants.DEFAULT_EVENT_ACTIVITY] = ocel.relations[pm4py.objects.ocel.constants.DEFAULT_EVENT_ACTIVITY].apply(
        lambda val: event_type if val=='('+event_type+','+object_type+')' else val)
    
    return ocel