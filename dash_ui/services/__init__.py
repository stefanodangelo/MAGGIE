

def build_string_input(maintenance_type, qr_code_part, partlist_part):
    part_list_bullet_points = "\n\t".join([f"- {part}" for part in partlist_part])

    query = f"""
    Give me assistance to {maintenance_type} part {qr_code_part} from BPW on a trailer.
    When returning the final answer with tools, parts list and steps, combine it with all the information you have on the following items:
    { part_list_bullet_points}
    """
    return query

