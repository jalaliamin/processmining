def set_ocdfg_custome_color(ot_color_dict):
  import pm4py.visualization.ocel.ocdfg.variants.classic as ocdfgcolor

  def _assign_color(ot: str) -> str:
    return ot_color_dict[ot]

  ocdfgcolor.ot_to_color = _assign_color
