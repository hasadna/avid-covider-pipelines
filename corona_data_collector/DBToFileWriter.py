import os
import shutil
import telegram
from corona_data_collector.config import answer_titles, values_to_convert, keys_to_convert, destination_output, \
    telegram_token, \
    telegram_chat_id
from corona_data_collector.gps_generator import GPSGenerator
from corona_data_collector.questionare_versions import questionare_versions

inverse_converted_keys = {}
for orig_key, conv_key in keys_to_convert.items():
    if conv_key not in inverse_converted_keys:
        inverse_converted_keys[conv_key] = []
    inverse_converted_keys[conv_key].append(orig_key)


def get_default_value(column_name, version):
    if column_name in questionare_versions[version]:
        return 0
    if column_name in inverse_converted_keys:
        alternative_keys = inverse_converted_keys[column_name]
        for alt_key in alternative_keys:
            if alt_key in questionare_versions[version]:
                return 0
    return ''


def collect_row(row):
    returned_array = []
    for key, _ in sorted(list(answer_titles.items())):
        val = row.get(key, get_default_value(key, row['version']))
        if val is None:
            val = 0
        if isinstance(val, str):
            val = val.replace(',', ' - ')
        returned_array.append(val)
    return ','.join([str(x) for x in returned_array])


def write_answer_keys(target_filename, prefix='', suffix='', ):
    answer_keys_line = ''
    for key, value in sorted(list(answer_titles.items())):
        if len(answer_keys_line) == 0:
            answer_keys_line = value
        else:
            answer_keys_line = f'{answer_keys_line},{value}'
    if len(prefix) > 0:
        prefix = f'{prefix},'
    if len(suffix) > 0:
        suffix = f',{suffix}'
    with open(target_filename, 'w') as target_file:
        target_file.write(f'{prefix}{answer_keys_line}{suffix}\n')


def convert_values(db_row):
    try:
        for convert_key in keys_to_convert:
            if convert_key in db_row:
                db_row[keys_to_convert[convert_key]] = db_row[convert_key]
                db_row.pop(convert_key)
        for key, value in db_row.items():
            if key in values_to_convert:
                db_row[key] = values_to_convert[key][value]
            if type(db_row[key]) == bool:
                db_row[key] = int(db_row[key])
        return db_row
    except Exception as err:
        print(f'error in converting the following row: ', db_row, err)
        return None


class DBToFileWriter:
    resultSet = []
    target_filename = ''
    filename_with_coords = ''
    last_created_record = None
    broken_records = 0

    def __init__(self, target_filename):
        self.target_filename = target_filename
        write_answer_keys(self.target_filename)
        self.bot = telegram.Bot(token=telegram_token)

    def log_database_data(self):
            fixed_row = ''
            try:
                lines = []
                for row in self.resultSet:
                    fixed_row = convert_values(row)
                    if fixed_row is None:
                        self.broken_records += 1
                        continue
                    collected_row = collect_row(fixed_row)
                    if len(collected_row.split(',')) == len(answer_titles):
                        lines.append(collected_row + "\n")
                    else:
                        print('skipped row: ', collected_row)
                with open(self.target_filename, 'a', encoding="utf-8") as file:
                    file.writelines(lines)
            except Exception as err:
                print('failed to write data to file - ', err)
                print('failing row: ', fixed_row)
                exit(1)
            finally:
                message = f'data written to file {self.target_filename}. Number of rows: {len(self.resultSet)}. '
                f'last id: {self.resultSet[- 1]["id"]}. Number of records retrieved but could not be evaluated: '
                f'{self.broken_records}'
                print(message)
                self.bot.send_message(chat_id=telegram_chat_id, text=message,
                                      parse_mode=telegram.ParseMode.HTML)

    def add_gps_coordinates(self, use_web_finder=False):
        try:
            gps_generator = GPSGenerator(use_web_finder)
            data_with_coords = gps_generator.load_gps_coordinates(self.target_filename)
            dot_index = self.target_filename.find('.')
            self.filename_with_coords = self.target_filename[:dot_index] + '_with_coords.csv'
            write_answer_keys(target_filename=self.filename_with_coords, suffix='lat,lng,address_street_accurate')
            with open(self.filename_with_coords, 'a') as file_with_coords:
                file_with_coords.writelines(data_with_coords)
            message = f'Data with GPS coordinates was written to {self.filename_with_coords}'
            print(message)
            self.bot.send_message(chat_id=telegram_chat_id, text=message,
                                  parse_mode=telegram.ParseMode.HTML)
        except Exception as err:
            print('failed to load coordinates', err)

    def get_last_created(self):
        value_to_return = ''
        with open(self.target_filename, 'r') as f:
            file_lines = f.readlines()
            last_line = file_lines[-1]
        if last_line:
            created_index = list(answer_titles).index('created')
            last_line_array = last_line.split(',')
            value_to_return = last_line_array[created_index]
        return value_to_return

    def clear_output_files(self):
        os.remove(self.target_filename)
        if not os.path.exists(destination_output):
            os.makedirs(destination_output)
        shutil.move(
            self.filename_with_coords,
            os.path.join(destination_output, os.path.basename(self.filename_with_coords))
        )
