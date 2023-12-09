def check_keys_not_in_dict(keys_set, input_dict):
    return not all(key in input_dict for key in keys_set)
