def unfold(
    df, rel, new_activity_name, 
    rename_source_node=False, event_classifier = ['case:concept:name'], order_columns = ['time:timestamp'], activity_column = 'concept:name'):
    import pandas as pd
    import itertools

    # for clas in event_classifier:
    df = df.copy()
    grouped = df.groupby(event_classifier)
    filtered_rows_idx = []

    # iterate through each group
    for name, group in grouped:
        # sort the group by timestamp
        group = group.sort_values(order_columns)

        # iterate through each row in the group
        for i in range(1, len(group)):
            # check if the previous task is "add item" and the current task is "Pack item"
            if group.iloc[i-1][activity_column] == rel[0] and group.iloc[i][activity_column] == rel[1]:
                # add the index values of the two rows to the filtered list
                if rename_source_node:
                    filtered_rows_idx.append(group.iloc[i-1:i].index.tolist())
                else:
                    filtered_rows_idx.append(group.iloc[i:i+1].index.tolist())

    filtered_rows_idx = list(itertools.chain(*filtered_rows_idx))

    df.loc[filtered_rows_idx,activity_column] = new_activity_name

    return df