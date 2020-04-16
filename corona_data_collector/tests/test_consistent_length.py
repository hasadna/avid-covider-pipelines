import unittest
from corona_data_collector.DBToFileWriter import DBToFileWriter, collect_row, write_answer_keys
from corona_data_collector.config import answer_titles, values_to_convert

class TestConsistentLength(unittest.TestCase):

    def test_length_consistent(self):
        data = [{
            "age": "66",
            "sex": "female",
            "locale": "he",
            "street": "קהילת ונציה",
            "smoking": "long_past_smokre",
            "version": "1.0.1",
            "city_town": "תל אביב,נאות אפקה",
            "temperature": "36.5",
            "met_above_18": "1",
            "met_under_18": "0",
            "chronic_cancer": True,
            "exposure_status": "none",
            "general_feeling": "feel_good",
            "numPreviousReports": 0,
            "chronic_hypertension": True,
            "id": 172495,
            "created": "2020-03-25T15:04:39.768496"},
            {"id": 175130, "created": "2020-03-25 16:32:38.997021", "age": "72", "sex": "male", "locale": "he", "street": "יונה סאלק", "smoking": "long_past_smokre", "version": "1.0.1", "city_town": "אשדוד", "temperature": "36.3", "met_above_18": "0", "met_under_18": "0", "exposure_status": "none", "general_feeling": "feel_good", "numPreviousReports": 0, "chronic_hypertension": True},
            {"id": 175131, "created": "2020-03-25 16:32:39.75928", "age": "26", "sex": "female", "locale": "he", "street": "מבצע דקל", "smoking": "אף פעם", "version": "0.2.1", "continue": "בטח!", "city_town": "פתח תקווה", "flatmates": "3", "flatmates_over_70": "0", "insulation_status": "not-insulated", "flatmates_under_18": "1", "work_serve_over_10": False, "exposure_met_people": "0", "symptoms_sore_throat": True, "symptoms_clogged_nose": True, "toplevel_symptoms_cough": True},
            {"id": "281791","created": "2020-03-25 16:32:39.75928", "age": "43", "sex": "female", "locale": "ru", "street": "תל לכיש 20", "version": "2.0.2", "city_town": "דימונה", "met_above_18": "2", "met_under_18": "0", "dateFirstReport": "2020-03-27T21:28:03.189Z", "exposure_status": "none", "general_feeling": "feel_good", "engagementSource": "direct", "numPreviousReports": 3, "symptoms_dry_cough": True, "notificationsEnabled": False, "precondition_smoking": "long_past_smokre", "preconditions_received": True, "toplevel_symptoms_cough": True},
            {"id": "281792", "created": "2020-03-25 16:32:39.75928","age": "69", "sex": "female", "alias": "בת 69 מסולד רמת השרון", "locale": "he", "street": "סולד", "version": "2.0.2", "city_town": "רמת השרון", "met_above_18": "0", "met_under_18": "yes", "dateFirstReport": "2020-03-25T13:13:38.750Z", "exposure_status": "none", "general_feeling": "לא", "engagementSource": "direct", "numPreviousReports": 5, "notificationsEnabled": False, "precondition_smoking": "daily_smoker", "preconditions_received": True}
        ]
        db_to_file_writer = DBToFileWriter()
        db_to_file_writer.target_filename = 'test_file.csv'
        db_to_file_writer.resultSet = data
        write_answer_keys('test_file.csv')
        db_to_file_writer.log_database_data()
        with open("./test_file.csv", "r") as test_file:
            first_line = test_file.readline()
            first_line_items = first_line.split(',')
            line = test_file.readline()
            while line:
                line_items = line.split(',')
                self.assertEqual(len(first_line_items), len(line_items), f'line is not same lengths as title {line}')
                line = test_file.readline()

    def test_exposure_status_failure(self):
        record = {
            'age': '72', 'sex': 0, 'locale': 'he', 'street': 'יונה סאלק', 'smoking': 1, 'version': '1.0.1',
            'city_town': 'אשדוד', 'temperature': '36.3', 'met_above_18': '0', 'met_under_18': '0',
            'general_feeling': 0, 'numPreviousReports': 0, 'chronic_hypertension': 1, 'id': 175130,
            'created': '2020-03-25T16:32:38.997021', 'insulation_status': 0
         }
        record_to_store = collect_row(record)
        print(record_to_store)
        self.assertGreater(len(record_to_store), 0, 'failed to create a record that can be stored in file')

    def test_file_length_consistent(self):
        first = True
        row_length = 0
        with open('../corona_bot_answers_2020-04-03_0928_with_coords.csv', 'r') as generated_file:
            content = generated_file.readlines()
        for line in content:
            length = len(line.split(','))
            if first:
                row_length = length
                first = False
            self.assertEqual(length, row_length, line)

    def test_expected_contact_with_patient(self):
        first = True
        back_from_abroad_db = [169603,169632,169813,169882,169924,170067,170099,170127,170326,170436,170479,170621,170685,170735,170777,170903,170962,170970,170989,171078,171142,171201,171230,171288,171290,171374,171440,171499,171506,171599,171718,172048,172076,172218,172231,172316,172367,172373,172406,172505,172537,172594,172596,172810,172894,173077,173186,173199,173211,173377,173436,173579,173816,173979,174099,174295,174359,174369,174372,174470,174840,174859,174910,175235,175290,175332,175373,175455,175537,175773,175887,176364,176453,176478,176501,176926,177207,177381,177426,177575,177870,178217,178328,178420,178511,178551,178603,178604,178681,178939,179016,179338,179480,179492,179549,179621,179664,179702,179758,180074,180125,180145,180271,180795,180943,180944,181023,181049,181162,181192,181252,181339,181445,181520,181630]
        with open('../bot_data/corona_bot_answers_2020-04-12_1226_with_coords.csv', 'r') as generated_file:
            content = generated_file.readlines()
            contact_with_patient_key = values_to_convert['insulation_status']['contact-with-patient']
            back_from_abroad_key = values_to_convert['insulation_status']['back-from-abroad']
            contact_with_patient_count = 0
            contact_with_patient_array = []
            back_from_abroad_array = []
            back_from_abroad_count = 0
            isolation_reason_key = None
        for line in content:
            line_array = line.split(',')
            if first:
                isolation_reason_key = line_array.index('isolation')
                first = False
            else:
                if int(line_array[isolation_reason_key]) == contact_with_patient_key:
                    contact_with_patient_count += 1
                    contact_with_patient_array.append(int(line_array[0]))
                if int(line_array[isolation_reason_key]) == back_from_abroad_key:
                    self.assertIn(int(line_array[0]), back_from_abroad_db)
                    back_from_abroad_count += 1
                    back_from_abroad_array.append(int(line_array[0]))
        print(back_from_abroad_array)
        print(contact_with_patient_array)
        self.assertEqual(82, contact_with_patient_count)
        self.assertEqual(115, back_from_abroad_count)

    def test_isolated_total_count(self):
        first = True
        db_isolated_id = [169603,169630,169632,169637,169690,169728,169753,169813,169829,169837,169882,169924,169930,170014,170042,170064,170067,170097,170099,170127,170184,170223,170234,170244,170263,170272,170289,170322,170326,170328,170350,170370,170390,170414,170428,170432,170436,170438,170442,170448,170453,170478,170479,170621,170629,170685,170735,170744,170777,170811,170878,170886,170903,170929,170936,170962,170970,170989,171009,171018,171078,171097,171123,171127,171132,171133,171142,171158,171162,171200,171201,171230,171256,171268,171283,171288,171290,171302,171323,171337,171342,171374,171399,171440,171472,171499,171506,171541,171571,171590,171599,171615,171686,171718,171720,171753,171823,171865,171900,171904,171907,171991,172048,172076,172153,172155,172163,172165,172218,172225,172231,172233,172236,172263,172276,172277,172316,172367,172373,172406,172419,172458,172483,172491,172492,172505,172511,172537,172542,172594,172596,172629,172637,172638,172644,172716,172727,172733,172749,172750,172789,172797,172808,172810,172894,172923,172925,172952,172956,172972,172995,173006,173077,173087,173112,173177,173178,173186,173199,173211,173222,173272,173275,173335,173336,173377,173436,173466,173507,173524,173579,173671,173768,173816,173965,173973,173979,173980,174018,174040,174049,174055,174063,174082,174084,174095,174099,174144,174146,174167,174202,174206,174232,174236,174239,174242,174258,174259,174263,174267,174271,174295,174313,174332,174350,174359,174369,174372,174374,174394,174405,174411,174443,174456,174470,174496,174506,174511,174541,174617,174652,174744,174768,174779,174813,174830,174840,174850,174859,174865,174890,174910,174997,175018,175025,175027,175056,175128,175154,175159,175167,175179,175235,175280,175290,175332,175339,175373,175424,175443,175455,175465,175470,175492,175503,175519,175537,175542,175628,175644,175684,175691,175730,175765,175773,175790,175831,175849,175857,175863,175880,175883,175887,175894,175908,175976,176035,176040,176046,176076,176124,176132,176198,176202,176211,176241,176288,176300,176340,176364,176386,176408,176435,176453,176466,176478,176490,176501,176534,176574,176613,176617,176674,176681,176804,176825,176827,176860,176889,176926,176930,177008,177045,177107,177113,177118,177122,177136,177207,177211,177238,177296,177363,177381,177409,177418,177426,177512,177559,177575,177608,177627,177721,177732,177780,177798,177810,177865,177870,177905,177945,177947,177953,178091,178118,178138,178186,178217,178252,178289,178304,178328,178420,178508,178511,178517,178525,178551,178603,178604,178681,178700,178713,178742,178750,178756,178781,178792,178836,178848,178867,178881,178910,178939,178955,179016,179033,179065,179066,179074,179160,179185,179212,179225,179250,179270,179281,179294,179338,179376,179418,179480,179492,179549,179594,179621,179661,179664,179669,179683,179702,179714,179758,179768,179769,179888,179982,180002,180010,180021,180027,180044,180074,180123,180125,180131,180136,180145,180169,180198,180271,180284,180383,180394,180438,180448,180478,180505,180511,180553,180575,180579,180587,180629,180725,180747,180795,180798,180840,180888,180941,180943,180944,180964,180991,181023,181037,181049,181120,181162,181164,181192,181218,181220,181230,181252,181304,181326,181339,181410,181445,181483,181520,181555,181562,181599,181630,181665]
        with open('../bot_data/corona_bot_answers_2020-04-12_1226_with_coords.csv', 'r') as generated_file:
            content = generated_file.readlines()
            isolation_key = 0
            isolated = 0
        for line in content:
            line_array = line.split(',')
            if first:
                isolation_key = line_array.index('isolation')
                first = False
            else:
                if int(line_array[isolation_key]) > 0:
                    self.assertIn(line_array[0], db_isolated_id)
                    isolated += 1
        self.assertEqual(648, isolated)

    def test_consistent_number_of_isolated(self):
        with open('../bot_data/corona_bot_answers_25_3_2020_with_coords.csv', 'r') as data_file:
            content = data_file.readlines()
        first = True
        isolated_counter = 0
        for line in content:
            line_array = line.split(',')
            if first:
                first_line = line_array
                first = False
            else:
                if int(line_array[first_line.index('isolation')]) > 0:
                    isolated_counter += 1
        self.assertEqual(1890+6+138+40+99, isolated_counter)
