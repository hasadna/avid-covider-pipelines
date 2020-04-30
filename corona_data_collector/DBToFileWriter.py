from corona_data_collector.config import answer_titles, values_to_convert, keys_to_convert, insulation_status_keys_to_convert
from corona_data_collector.questionare_versions import get_version_columns


inverse_converted_keys = {}
for orig_key, conv_key in keys_to_convert.items():
    if conv_key not in inverse_converted_keys:
        inverse_converted_keys[conv_key] = []
    inverse_converted_keys[conv_key].append(orig_key)


def get_default_value(column_name, version):
    if column_name in get_version_columns(version):
        return 0
    if column_name in inverse_converted_keys:
        alternative_keys = inverse_converted_keys[column_name]
        for alt_key in alternative_keys:
            if alt_key in get_version_columns(version):
                return 0
    return ''


def collect_row(row, return_array=False, force_version=None):
    returned_array = []
    for key, _ in sorted(list(answer_titles.items())):
        val = row.get(key, get_default_value(key, force_version if force_version else row['version']))
        if val is None:
            val = 0
        if isinstance(val, str):
            val = val.replace(',', ' - ')
        returned_array.append(val)
    returned_array = [str(x) for x in returned_array]
    if return_array:
        return returned_array
    else:
        return ','.join(returned_array)


def convert_values(db_row, stats=None):
    for convert_key in keys_to_convert:
        if convert_key in db_row:
            db_row[keys_to_convert[convert_key]] = db_row[convert_key]
            db_row.pop(convert_key)
    for key in insulation_status_keys_to_convert:
        if db_row.get(key):
            db_row["insulation_status"] = db_row.pop(key)
            break
    for key, value in db_row.items():
        if key in values_to_convert:
            str_value = str(value).lower()
            if str_value in values_to_convert[key]:
                db_row[key] = values_to_convert[key][str_value]
            elif value is None:
                continue
            elif stats:
                statkey = 'invalid_values_to_convert_%s__%s' % (key, str_value)
                stats[statkey] += 1
                return None
            else:
                print('convert_values: missing values_to_convert key="%s" value="%s"' % (key, str_value))
                return None
        if type(db_row[key]) == bool:
            db_row[key] = int(db_row[key])
    return db_row
