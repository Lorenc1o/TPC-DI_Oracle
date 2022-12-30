def char_insert(value):
    if value is None:
        return ''
    value = value.replace("'", "''")
    return value
  